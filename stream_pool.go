package dht

import (
	"context"
	"sync"

	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
	peer "github.com/libp2p/go-libp2p-peer"
)

type streamPool struct {
	newStream func(context.Context, peer.ID) (*stream, error)
	mu        sync.Mutex
	m         map[peer.ID]*peerStreamPool
}

type peerStreamPool struct {
	newStream func(context.Context) (*stream, error)
	mu        sync.Mutex
	streams   map[*stream]struct{}
	waiters   map[chan *stream]struct{}
	sendMu    sync.Mutex
}

func (sp *streamPool) get(p peer.ID) (*stream, bool) {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	if _, ok := sp.m[p]; !ok {
		return nil, false
	}
	return sp.m[p].get()
}

func (me *peerStreamPool) get() (*stream, bool) {
	me.mu.Lock()
	defer me.mu.Unlock()
	for s := range me.streams {
		delete(me.streams, s)
		if s.err() != nil {
			// Stream went bad and hasn't deleted itself yet.
			continue
		}
		return s, true
	}
	return nil, false
}

func (me *streamPool) getPeer(p peer.ID) *peerStreamPool {
	me.mu.Lock()
	defer me.mu.Unlock()
	me.initPeer(p)
	return me.m[p]
}

func (me *peerStreamPool) put(s *stream) {
	me.mu.Lock()
	defer me.mu.Unlock()
	for ch := range me.waiters {
		select {
		case ch <- s:
			return
		default:
		}
	}
	me.streams[s] = struct{}{}
}

// Mutex already held, and is released in here.
func (me *peerStreamPool) wait(ctx context.Context) (*stream, bool) {
	for s := range me.streams {
		return s, true
	}
	ch := make(chan *stream, 1)
	me.waiters[ch] = struct{}{}
	me.mu.Unlock()
	defer me.mu.Lock()
	select {
	case <-ctx.Done():
		me.mu.Lock()
		delete(me.waiters, ch)
		me.mu.Unlock()
		close(ch)
		select {
		case s, ok := <-ch:
			if ok {
				return s, true
			}
		default:
		}
		return nil, false
	case s := <-ch:
		close(ch)
		me.mu.Lock()
		delete(me.waiters, ch)
		me.mu.Unlock()
		return s, true
	}
}

func (sp *streamPool) wait(ctx context.Context, p peer.ID) (*stream, bool) {
	sp.mu.Lock()
	sp.initPeer(p)
	psp := sp.m[p]
	psp.mu.Lock()
	sp.mu.Unlock()
	defer psp.mu.Unlock()
	return psp.wait(ctx)
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
		waiters: make(map[chan *stream]struct{}),
		streams: make(map[*stream]struct{}),
	}
	sp.m[p] = psp
	go func() {
		s, err := psp.newStream(context.Background())
		if err == nil {
			psp.put(s)
		}
	}()
}

func (sp *streamPool) put(s *stream, p peer.ID) {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	if s.err() != nil {
		return
	}
	sp.initPeer(p)
	sp.m[p].put(s)
}

func (sp *streamPool) delete(ps *stream, p peer.ID) {
	sp.mu.Lock()
	sp.deleteLocked(ps, p)
	sp.mu.Unlock()
}

func (sp *streamPool) deleteLocked(ps *stream, p peer.ID) {
	if _, ok := sp.m[p]; !ok {
		return
	}
	sp.m[p].delete(ps)
	sp.deletePeer(p)
}

func (sp *streamPool) deletePeer(p peer.ID) {
	psp := sp.m[p]
	psp.mu.Lock()
	defer psp.mu.Unlock()
	if psp.empty() {
		delete(sp.m, p)
	}
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
	me.sendMu.Lock()
	defer me.sendMu.Unlock()
	s, ok := me.get()
	if !ok {
		var err error
		s, err = me.newStream(ctx)
		if err != nil {
			return err
		}
	}
	defer me.put(s)
	beforeWrite()
	return s.send(m)
}
