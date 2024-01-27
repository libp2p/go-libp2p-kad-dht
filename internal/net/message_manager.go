package net

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"

	logging "github.com/ipfs/go-log"
	"github.com/libp2p/go-msgio"

	//lint:ignore SA1019 TODO migrate away from gogo pb
	"github.com/libp2p/go-msgio/protoio"

	"go.opencensus.io/stats"
	"go.opencensus.io/tag"

	"github.com/libp2p/go-libp2p-kad-dht/metrics"
	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
)

const dhtMessageTimeout = 10 * time.Second

// ErrReadTimeout is an error that occurs when no message is read within the timeout period.
var ErrReadTimeout = fmt.Errorf("timed out reading response")

var logger = logging.Logger("dht")

// messageSenderImpl is responsible for sending requests and messages to peers efficiently, including reuse of streams.
// It also tracks metrics for sent requests and messages.
type messageSenderImpl struct {
	host      host.Host // the network services we need
	smlk      sync.Mutex
	strmap    map[peer.ID]*peerMessageSender
	protocols []protocol.ID
}

func NewMessageSenderImpl(h host.Host, protos []protocol.ID) pb.MessageSenderWithDisconnect {
	return &messageSenderImpl{
		host:      h,
		strmap:    make(map[peer.ID]*peerMessageSender),
		protocols: protos,
	}
}

func (m *messageSenderImpl) OnDisconnect(ctx context.Context, p peer.ID) {
	m.smlk.Lock()
	defer m.smlk.Unlock()
	ms, ok := m.strmap[p]
	if !ok {
		return
	}
	delete(m.strmap, p)

	// Do this asynchronously as ms.writeLock can block for a while.
	go func() {
		ms.writeLock <- struct{}{}
		if p := ms.pipeline; p != nil {
			p.kill(false)
		}
		<-ms.writeLock
	}()
}

// SendRequest sends out a request, but also makes sure to
// measure the RTT for latency measurements.
func (m *messageSenderImpl) SendRequest(ctx context.Context, p peer.ID, pmes *pb.Message) (*pb.Message, error) {
	ctx, _ = tag.New(ctx, metrics.UpsertMessageType(pmes))

	ms, err := m.messageSenderForPeer(ctx, p)
	if err != nil {
		stats.Record(ctx,
			metrics.SentRequests.M(1),
			metrics.SentRequestErrors.M(1),
		)
		logger.Debugw("request failed to open message sender", "error", err, "to", p)
		return nil, err
	}

	start := time.Now()

	rpmes, err := ms.SendRequest(ctx, pmes)
	if err != nil {
		stats.Record(ctx,
			metrics.SentRequests.M(1),
			metrics.SentRequestErrors.M(1),
		)
		logger.Debugw("request failed", "error", err, "to", p)
		return nil, err
	}

	stats.Record(ctx,
		metrics.SentRequests.M(1),
		metrics.SentBytes.M(int64(pmes.Size())),
		metrics.OutboundRequestLatency.M(float64(time.Since(start))/float64(time.Millisecond)),
	)
	m.host.Peerstore().RecordLatency(p, time.Since(start))
	return rpmes, nil
}

// SendMessage sends out a message
func (m *messageSenderImpl) SendMessage(ctx context.Context, p peer.ID, pmes *pb.Message) error {
	ctx, _ = tag.New(ctx, metrics.UpsertMessageType(pmes))

	ms, err := m.messageSenderForPeer(ctx, p)
	if err != nil {
		stats.Record(ctx,
			metrics.SentMessages.M(1),
			metrics.SentMessageErrors.M(1),
		)
		logger.Debugw("message failed to open message sender", "error", err, "to", p)
		return err
	}

	if err := ms.SendMessage(ctx, pmes); err != nil {
		stats.Record(ctx,
			metrics.SentMessages.M(1),
			metrics.SentMessageErrors.M(1),
		)
		logger.Debugw("message failed", "error", err, "to", p)
		return err
	}

	stats.Record(ctx,
		metrics.SentMessages.M(1),
		metrics.SentBytes.M(int64(pmes.Size())),
	)
	return nil
}

func (m *messageSenderImpl) messageSenderForPeer(ctx context.Context, p peer.ID) (*peerMessageSender, error) {
	m.smlk.Lock()
	ms, ok := m.strmap[p]
	if ok {
		m.smlk.Unlock()
		return ms, nil
	}
	ms = newPeerMessageSender(p, m)
	m.strmap[p] = ms
	m.smlk.Unlock()

	return ms, nil
}

// peerMessageSender is responsible for sending requests and messages to a particular peer
// it apply backpressure by pipelining messages.
type peerMessageSender struct {
	pipeline  *pipeline
	writeLock chan struct{} // use a chan so we can select againt ctx.Done

	p peer.ID
	m *messageSenderImpl

	waiting uint
	invalid bool
}

func newPeerMessageSender(p peer.ID, m *messageSenderImpl) *peerMessageSender {
	return &peerMessageSender{
		writeLock: make(chan struct{}, 1),
		p:         p,
		m:         m,
	}
}

type pipeline struct {
	hasError      atomic.Bool
	hasBeenClosed bool
	nextReader    chan struct{}
	s             network.Stream
	r             msgio.ReadCloser
	bufW          bufio.Writer
	w             protoio.Writer
	readError     error
	closeLk       sync.Mutex
	waiting       uint
}

func (p *pipeline) kill(wasWaiting bool) {
	p.closeLk.Lock()
	defer p.closeLk.Unlock()

	p.hasError.Store(true)
	w := p.waiting
	if wasWaiting {
		w--
	}
	if w == 0 && !p.hasBeenClosed {
		p.hasBeenClosed = true
		p.s.Reset()
		p.r.Close()
	} else {
		p.waiting = w
	}
}

func (ms *peerMessageSender) getStream(ctx context.Context) error {
	if ms.invalid {
		return fmt.Errorf("message sender has been invalidated")
	}
	if ms.pipeline != nil {
		if !ms.pipeline.hasError.Load() {
			return nil
		}
		ms.pipeline = nil
	}

	// We only want to speak to peers using our primary protocols. We do not want to query any peer that only speaks
	// one of the secondary "server" protocols that we happen to support (e.g. older nodes that we can respond to for
	// backwards compatibility reasons).
	nstr, err := ms.m.host.NewStream(ctx, ms.p, ms.m.protocols...)
	if err != nil {
		return err
	}

	nr := make(chan struct{})
	close(nr)
	ms.pipeline = &pipeline{
		nextReader: nr,
		s:          nstr,
		r:          msgio.NewVarintReaderSize(nstr, network.MessageSizeMax),
	}
	ms.pipeline.bufW.Reset(nstr)
	ms.pipeline.w = protoio.NewDelimitedWriter(&ms.pipeline.bufW)

	return nil
}

func (ms *peerMessageSender) SendMessage(ctx context.Context, pmes *pb.Message) error {
	select {
	case ms.writeLock <- struct{}{}:
		defer func() { <-ms.writeLock }()
	case <-ctx.Done():
		return ctx.Err()
	}

	_, _, cancel, err := ms.sendMessage(ctx, pmes)
	cancel()
	return err
}

// sendMessage let the caller handle [ms.writeLock].
func (ms *peerMessageSender) sendMessage(ictx context.Context, pmes *pb.Message) (deadline time.Time, ctx context.Context, cancel context.CancelFunc, err error) {
	ctx, cancel = context.WithTimeout(ictx, dhtMessageTimeout)

	err = ms.getStream(ctx)
	if err != nil {
		return
	}
	p := ms.pipeline
	var good bool
	defer func() {
		if !good {
			p.kill(false)
			ms.pipeline = nil
		}
	}()

	var ok bool
	deadline, ok = ctx.Deadline()
	if !ok {
		panic("wtf we just added a timeout, can't not have a deadline.")
	}
	err = p.s.SetWriteDeadline(deadline)
	if err != nil {
		return
	}

	err = p.w.WriteMsg(pmes)
	if err != nil {
		return
	}
	err = p.bufW.Flush()
	if err != nil {
		return
	}

	good = true
	return
}

func (p *pipeline) decrement() {
	p.closeLk.Lock()
	defer p.closeLk.Unlock()

	w := p.waiting - 1
	if w == 0 && p.hasError.Load() && !p.hasBeenClosed {
		p.hasBeenClosed = true
		p.s.Reset()
		p.r.Close()
	} else {
		p.waiting = w
	}
}

func (p *pipeline) increment() {
	p.closeLk.Lock()
	defer p.closeLk.Unlock()
	p.waiting++
}

func (ms *peerMessageSender) SendRequest(ctx context.Context, pmes *pb.Message) (*pb.Message, error) {
	select {
	case ms.writeLock <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	deadline, ctx, cancel, err := ms.sendMessage(ctx, pmes)
	defer cancel()
	if err != nil {
		<-ms.writeLock
		return nil, err
	}

	p := ms.pipeline
	nextNext := make(chan struct{})
	next := p.nextReader
	p.nextReader = nextNext
	p.increment()
	<-ms.writeLock
	var good bool
	defer func() {
		if !good {
			p.kill(true)
		}
	}()

	// FIXME: handle theses contex failures nicely,
	// we could start a background goroutine to clean up the stream, or let the next goroutine do it.

	select {
	case <-next:
	case <-ctx.Done():
		go func() {
			// pass the chain
			<-next
			close(nextNext)
		}()
		return nil, ctx.Err()
	}

	defer close(nextNext)

	if rerr := p.readError; rerr != nil {
		return nil, rerr
	}

	err = p.s.SetReadDeadline(deadline)
	if err != nil {
		return nil, err
	}

	bytes, err := p.r.ReadMsg()
	if err != nil {
		p.readError = err
		return nil, err
	}
	mes := new(pb.Message)
	err = mes.Unmarshal(bytes)
	p.r.ReleaseMsg(bytes)
	if err != nil {
		p.readError = err
		return nil, err
	}

	p.decrement()
	good = true

	return mes, nil
}

// The Protobuf writer performs multiple small writes when writing a message.
// We need to buffer those writes, to make sure that we're not sending a new
// packet for every single write.
type bufferedDelimitedWriter struct {
	*bufio.Writer
	protoio.WriteCloser
}

var writerPool = sync.Pool{
	New: func() interface{} {
		w := bufio.NewWriter(nil)
		return &bufferedDelimitedWriter{
			Writer:      w,
			WriteCloser: protoio.NewDelimitedWriter(w),
		}
	},
}

func WriteMsg(w io.Writer, mes *pb.Message) error {
	bw := writerPool.Get().(*bufferedDelimitedWriter)
	bw.Reset(w)
	err := bw.WriteMsg(mes)
	if err == nil {
		err = bw.Flush()
	}
	bw.Reset(nil)
	writerPool.Put(bw)
	return err
}

func (w *bufferedDelimitedWriter) Flush() error {
	return w.Writer.Flush()
}
