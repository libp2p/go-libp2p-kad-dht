package dht

import (
	"context"
	"log"
	"sync"

	pbio "github.com/gogo/protobuf/io"
	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
	inet "github.com/libp2p/go-libp2p-net"
	peer "github.com/libp2p/go-libp2p-peer"
	"golang.org/x/xerrors"
)

func (dht *IpfsDHT) getPoolStream(ctx context.Context, p peer.ID) (*poolStream, error) {
	dht.streamPoolMu.Lock()
	for ps := range dht.streamPool[p] {
		dht.deletePoolStreamLocked(ps, p)
		if ps.bad() {
			log.Printf("got bad pool stream for %v", p)
			continue
		}
		dht.streamPoolMu.Unlock()
		log.Printf("reusing pool stream for %v", p)
		return ps, nil
	}
	dht.streamPoolMu.Unlock()
	log.Printf("creating new pool stream for %v", p)
	return dht.newPoolStream(ctx, p)
}

func (dht *IpfsDHT) putPoolStream(ps *poolStream, p peer.ID) {
	dht.streamPoolMu.Lock()
	defer dht.streamPoolMu.Unlock()
	if ps.bad() {
		log.Printf("putting pool stream for %v but it went bad", p)
		return
	}
	log.Printf("putting pool stream for %v", p)
	if dht.streamPool[p] == nil {
		dht.streamPool[p] = make(map[*poolStream]struct{})
	}
	dht.streamPool[p][ps] = struct{}{}
}

func (dht *IpfsDHT) deletePoolStream(ps *poolStream, p peer.ID) {
	dht.streamPoolMu.Lock()
	dht.deletePoolStreamLocked(ps, p)
	dht.streamPoolMu.Unlock()
}

func (dht *IpfsDHT) deletePoolStreamLocked(ps *poolStream, p peer.ID) {
	log.Printf("deleting pool stream for %v", p)
	delete(dht.streamPool[p], ps)
	if len(dht.streamPool) == 0 {
		delete(dht.streamPool, p)
	}
}

func (dht *IpfsDHT) newPoolStream(ctx context.Context, p peer.ID) (*poolStream, error) {
	s, err := dht.newStream(ctx, p)
	if err != nil {
		return nil, xerrors.Errorf("opening stream: %w", err)
	}
	ps := &poolStream{
		stream: s,
		w:      newBufferedDelimitedWriter(s),
		r:      pbio.NewDelimitedReader(s, inet.MessageSizeMax),
		m:      make(chan chan *pb.Message, 1),
	}
	ps.onReaderErr = func() {
		ps.reset()
		dht.deletePoolStream(ps, p)
	}
	go ps.reader()
	return ps, nil
}

type poolStream struct {
	stream interface {
		Reset() error
	}
	w           bufferedWriteCloser
	r           pbio.ReadCloser
	onReaderErr func()

	mu        sync.Mutex
	m         chan chan *pb.Message
	readerErr error
}

func (me *poolStream) reset() {
	me.stream.Reset()
}

func (me *poolStream) send(m *pb.Message) (err error) {
	defer func() {
		if err != nil {
			log.Printf("error sending message: %v", err)
		}
	}()
	if err := me.w.WriteMsg(m); err != nil {
		return xerrors.Errorf("writing message: %w", err)
	}
	if err := me.w.Flush(); err != nil {
		return xerrors.Errorf("flushing: %w", err)
	}
	return nil
}

func (me *poolStream) request(ctx context.Context, req *pb.Message) (*pb.Message, error) {
	replyChan := make(chan *pb.Message, 1)
	me.mu.Lock()
	if me.readerErr != nil {
		me.mu.Unlock()
		return nil, xerrors.Errorf("reader: %w", me.readerErr)
	}
	select {
	case me.m <- replyChan:
	default:
		me.mu.Unlock()
		return nil, xerrors.New("message pipeline full")
	}
	me.mu.Unlock()
	err := me.send(req)
	if err != nil {
		return nil, err
	}
	select {
	case reply, ok := <-replyChan:
		if !ok {
			return nil, xerrors.Errorf("reader: %w", err)
		}
		return reply, nil
	case <-ctx.Done():
		return nil, xerrors.Errorf("while waiting for reply: %w", ctx.Err())
	}
}

func (me *poolStream) reader() {
	err := me.readLoop()
	me.mu.Lock()
	me.readerErr = err
	close(me.m)
	me.mu.Unlock()
	me.onReaderErr()
	for mc := range me.m {
		close(mc)
	}
}

func (me *poolStream) readLoop() error {
	for {
		var m pb.Message
		err := me.r.ReadMsg(&m)
		if err != nil {
			return err
		}
		select {
		case mc := <-me.m:
			mc <- &m
		default:
			return xerrors.New("read superfluous message")
		}
	}
}

func (me *poolStream) bad() bool {
	me.mu.Lock()
	defer me.mu.Unlock()
	return me.readerErr != nil
}
