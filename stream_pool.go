package dht

import (
	"context"
	"sync"

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
