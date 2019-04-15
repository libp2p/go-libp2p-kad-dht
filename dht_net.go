package dht

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"time"

	ggio "github.com/gogo/protobuf/io"
	ctxio "github.com/jbenet/go-context/io"
	"github.com/libp2p/go-libp2p-kad-dht/metrics"
	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
	inet "github.com/libp2p/go-libp2p-net"
	peer "github.com/libp2p/go-libp2p-peer"
	"go.opencensus.io/stats"
	"go.opencensus.io/tag"
)

var ErrReadTimeout = fmt.Errorf("timed out reading response")

type bufferedWriteCloser interface {
	ggio.WriteCloser
	Flush() error
}

// The Protobuf writer performs multiple small writes when writing a message.
// We need to buffer those writes, to make sure that we're not sending a new
// packet for every single write.
type bufferedDelimitedWriter struct {
	*bufio.Writer
	ggio.WriteCloser
}

func newBufferedDelimitedWriter(str io.Writer) bufferedWriteCloser {
	w := bufio.NewWriter(str)
	return &bufferedDelimitedWriter{
		Writer:      w,
		WriteCloser: ggio.NewDelimitedWriter(w),
	}
}

func (w *bufferedDelimitedWriter) Flush() error {
	return w.Writer.Flush()
}

// handleNewStream implements the inet.StreamHandler
func (dht *IpfsDHT) handleNewStream(s inet.Stream) {
	defer s.Reset()
	if dht.handleNewMessage(s) {
		// Gracefully close the stream for writes.
		s.Close()
	}
}

// Returns true on orderly completion of writes (so we can Close the stream).
func (dht *IpfsDHT) handleNewMessage(s inet.Stream) bool {
	ctx := dht.ctx
	cr := ctxio.NewReader(ctx, s) // ok to use. we defer close stream in this func
	cw := ctxio.NewWriter(ctx, s) // ok to use. we defer close stream in this func
	r := ggio.NewDelimitedReader(cr, inet.MessageSizeMax)
	w := newBufferedDelimitedWriter(cw)
	mPeer := s.Conn().RemotePeer()

	for {
		var req pb.Message
		switch err := r.ReadMsg(&req); err {
		case io.EOF:
			return true
		default:
			// This string test is necessary because there isn't a single stream reset error
			// instance	in use.
			if err.Error() != "stream reset" {
				logger.Debugf("error reading message: %#v", err)
			}
			stats.RecordWithTags(
				ctx,
				[]tag.Mutator{tag.Upsert(metrics.KeyMessageType, "UNKNOWN")},
				metrics.ReceivedMessageErrors.M(1),
			)
			return false
		case nil:
		}

		startTime := time.Now()
		ctx, _ = tag.New(
			ctx,
			tag.Upsert(metrics.KeyMessageType, req.GetType().String()),
		)

		stats.Record(
			ctx,
			metrics.ReceivedMessages.M(1),
			metrics.ReceivedBytes.M(int64(req.Size())),
		)

		handler := dht.handlerForMsgType(req.GetType())
		if handler == nil {
			stats.Record(ctx, metrics.ReceivedMessageErrors.M(1))
			logger.Warningf("can't handle received message of type %v", req.GetType())
			return false
		}

		resp, err := handler(ctx, mPeer, &req)
		if err != nil {
			stats.Record(ctx, metrics.ReceivedMessageErrors.M(1))
			logger.Debugf("error handling message: %v", err)
			return false
		}

		dht.updateFromMessage(ctx, mPeer, &req)

		if resp == nil {
			continue
		}

		// send out response msg
		err = w.WriteMsg(resp)
		if err == nil {
			err = w.Flush()
		}
		if err != nil {
			stats.Record(ctx, metrics.ReceivedMessageErrors.M(1))
			logger.Debugf("error writing response: %v", err)
			return false
		}
		elapsedTime := time.Since(startTime)
		latencyMillis := float64(elapsedTime) / float64(time.Millisecond)
		stats.Record(ctx, metrics.InboundRequestLatency.M(latencyMillis))
	}
}

// sendRequest sends out a request, but also makes sure to measure the RTT for latency measurements.
func (dht *IpfsDHT) sendRequest(ctx context.Context, p peer.ID, req *pb.Message) (_ *pb.Message, err error) {
	ctx, _ = tag.New(ctx, metrics.UpsertMessageType(req))
	dht.recordOutboundMessage(ctx, req)
	started := time.Now()
	defer dht.observeRpcLatency(req, "request", &err, started)
	reply, err := dht.streamPool.getPeer(p).doRequest(ctx, req)
	if err == nil {
		dht.updateFromMessage(ctx, p, reply)
		dht.peerstore.RecordLatency(p, time.Since(started))
		stats.Record(
			ctx,
			metrics.SentRequests.M(1),
			metrics.SentBytes.M(int64(req.Size())),
			metrics.OutboundRequestLatency.M(
				float64(time.Since(started))/float64(time.Millisecond),
			),
		)
	} else {
		stats.Record(ctx, metrics.SentRequestErrors.M(1))
	}
	return reply, err
}

// sendMessage sends out a message
func (dht *IpfsDHT) sendMessage(ctx context.Context, p peer.ID, pmes *pb.Message) error {
	ctx, _ = tag.New(ctx, metrics.UpsertMessageType(pmes))
	dht.recordOutboundMessage(ctx, pmes)
	started := time.Now()
	err := dht.streamPool.getPeer(p).send(ctx, pmes)
	if err == nil {
		stats.Record(
			ctx,
			metrics.SentMessages.M(1),
			metrics.SentBytes.M(int64(pmes.Size())),
		)
	} else {
		stats.Record(ctx, metrics.SentMessageErrors.M(1))
	}
	dht.observeRpcLatency(pmes, "send", &err, started)
	return err
}

func (dht *IpfsDHT) updateFromMessage(ctx context.Context, p peer.ID, mes *pb.Message) error {
	// Make sure that this node is actually a DHT server, not just a client.
	protos, err := dht.peerstore.SupportsProtocols(p, dht.protocolStrs()...)
	if err == nil && len(protos) > 0 {
		dht.Update(ctx, p)
	}
	return nil
}

func (dht *IpfsDHT) newNetStream(ctx context.Context, p peer.ID) (inet.Stream, error) {
	s, err := dht.host.NewStream(ctx, p, dht.protocols...)
	return s, err
}

func (dht *IpfsDHT) recordOutboundMessage(ctx context.Context, m *pb.Message) {
}

func (dht *IpfsDHT) observeRpcLatency(sent *pb.Message, rpcType string, err *error, started time.Time) {
}
