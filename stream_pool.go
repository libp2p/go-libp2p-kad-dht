package dht

import (
	"context"
	"log"
	"sync"

	"golang.org/x/xerrors"

	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
	peer "github.com/libp2p/go-libp2p-peer"
)

type streamPool struct {
	newStream func(context.Context, peer.ID) (*stream, error)
	mu        sync.Mutex
	m         map[peer.ID]*peerStreamPool
}

func (me *streamPool) deleteStream(s *stream, p peer.ID) {
	me.getPeer(p).delete(s)
}

func (me *streamPool) getPeer(p peer.ID) *peerStreamPool {
	me.mu.Lock()
	defer me.mu.Unlock()
	me.initPeer(p)
	return me.m[p]
}

func (sp *streamPool) initPeer(p peer.ID) {
	if sp.m == nil {
		sp.m = make(map[peer.ID]*peerStreamPool)
	}
	if _, ok := sp.m[p]; ok {
		return
	}
	psp := &peerStreamPool{
		newStream: func(ctx context.Context) (*stream, error) {
			return sp.newStream(ctx, p)
		},
		waiters: make(map[*streamWaiter]struct{}),
		streams: make(map[*stream]struct{}),
		pending: 1,
	}
	sp.m[p] = psp
	go psp.addStream()
}

func (sp *streamPool) deletePeer(p peer.ID) {
	psp := sp.m[p]
	psp.mu.Lock()
	defer psp.mu.Unlock()
	if psp.empty() {
		delete(sp.m, p)
	}
}

type peerStreamPool struct {
	newStream func(context.Context) (*stream, error)

	mu      sync.Mutex
	streams map[*stream]struct{}
	waiters map[*streamWaiter]struct{}
	pending int
	//sendMu    sync.Mutex
}

func (me *peerStreamPool) getStream(ctx context.Context) (*stream, error) {
	me.mu.Lock()
	for s := range me.streams {
		delete(me.streams, s)
		me.mu.Unlock()
		return s, nil
	}
	w := &streamWaiter{}
	w.ret.Lock()
	me.waiters[w] = struct{}{}
	if me.pending < len(me.waiters) {
		me.pending++
		go me.addStream()
	}
	if me.pending < len(me.waiters) {
		panic("not enough pending streams")
	}
	me.mu.Unlock()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		<-ctx.Done()
		w.set(nil, ctx.Err())
	}()
	w.ret.Lock()
	return w.s, w.err
}

func (me *peerStreamPool) addStream() {
	s, err := me.newStream(context.Background())
	me.mu.Lock()
	defer me.mu.Unlock()
	me.pending--
	if me.pending < 0 {
		panic("negative pending")
	}
	me.putStreamLocked(s, err)
}

func (me *peerStreamPool) putStream(s *stream, err error) {
	me.mu.Lock()
	defer me.mu.Unlock()
	me.putStreamLocked(s, err)
}

func (me *peerStreamPool) putStreamLocked(s *stream, err error) {
	for w := range me.waiters {
		w.set(s, err)
		delete(me.waiters, w)
		return
	}
	if err != nil {
		log.Printf("dropped new stream error: %v", err)
		return
	}
	me.streams[s] = struct{}{}
}

func (me *peerStreamPool) delete(s *stream) {
	me.mu.Lock()
	defer me.mu.Unlock()
	delete(me.streams, s)
}

func (me *peerStreamPool) empty() bool {
	return len(me.streams) == 0 && len(me.waiters) == 0
}

func (me *peerStreamPool) send(ctx context.Context, m *pb.Message, beforeWrite func()) error {
	s, err := me.getStream(ctx)
	if err != nil {
		return xerrors.Errorf("getting stream: %w", err)
	}
	err = s.send(m)
	me.putStream(s, err)
	return err
}

func (me *peerStreamPool) doRequest(ctx context.Context, req *pb.Message, beforeWrite func()) (*pb.Message, error) {
	s, err := me.getStream(ctx)
	if err != nil {
		return nil, xerrors.Errorf("getting stream: %w", err)
	}
	type requestResult struct {
		*pb.Message
		error
	}
	rrCh := make(chan requestResult, 1)
	go func() {
		resp, err := s.request(ctx, req)
		rrCh <- requestResult{resp, err}
		me.putStream(s, err)
	}()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case rr := <-rrCh:
		return rr.Message, rr.error
	}
}

type streamWaiter struct {
	mu   sync.Mutex
	s    *stream
	err  error
	done bool
	ret  sync.Mutex
}

func (me *streamWaiter) set(s *stream, err error) {
	me.mu.Lock()
	if me.done {
		me.mu.Unlock()
		return
	}
	me.s = s
	me.err = err
	me.done = true
	me.mu.Unlock()
	me.ret.Unlock()
}
