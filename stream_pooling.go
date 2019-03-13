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

func (dht *IpfsDHT) getPoolStream(ctx context.Context, p peer.ID) (_ *poolStream, reused bool, _ error) {
	dht.streamPoolMu.Lock()
	for ps := range dht.streamPool[p] {
		dht.deletePoolStreamLocked(ps, p)
		if ps.err() != nil {
			// Stream went bad and hasn't deleted itself yet.
			continue
		}
		dht.streamPoolMu.Unlock()
		return ps, true, nil
	}
	dht.streamPoolMu.Unlock()
	ps, err := dht.newPoolStream(ctx, p)
	return ps, false, err
}

func (dht *IpfsDHT) putPoolStream(ps *poolStream, p peer.ID) {
	dht.streamPoolMu.Lock()
	defer dht.streamPoolMu.Unlock()
	if ps.err() != nil {
		return
	}
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
	go func() {
		ps.reader()
		dht.deletePoolStream(ps, p)
		ps.reset()
	}()
	return ps, nil
}

type poolStream struct {
	stream interface {
		Reset() error
	}
	w bufferedWriteCloser
	r pbio.ReadCloser

	// Synchronizes m and readerErr.
	mu sync.Mutex
	// Receives channels to send responses on.
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

func (me *poolStream) request(ctx context.Context, req *pb.Message) (<-chan *pb.Message, error) {
	replyChan := make(chan *pb.Message, 1)
	me.mu.Lock()
	if err := me.errLocked(); err != nil {
		me.mu.Unlock()
		return nil, err
	}
	select {
	case me.m <- replyChan:
	default:
		me.mu.Unlock()
		panic("message pipeline full")
	}
	me.mu.Unlock()
	err := me.send(req)
	return replyChan, err
}

// Handles the error returned from the read loop.
func (me *poolStream) reader() {
	err := me.readLoop()
	me.mu.Lock()
	me.readerErr = err
	close(me.m)
	me.mu.Unlock()
	for mc := range me.m {
		close(mc)
	}
}

// Reads from the stream until something is wrong.
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

func (me *poolStream) err() error {
	me.mu.Lock()
	defer me.mu.Unlock()
	return me.errLocked()
}

// A stream has gone bad when the reader has given up.
func (me *poolStream) errLocked() error {
	if me.readerErr != nil {
		return xerrors.Errorf("reader: %w", me.readerErr)
	}
	return nil
}
