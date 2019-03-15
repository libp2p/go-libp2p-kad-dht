package dht

import (
	"context"
	"sync"

	peer "github.com/libp2p/go-libp2p-peer"
)

type streamPool struct {
	mu sync.Mutex
	m  map[peer.ID]map[*stream]struct{}
}

func (sp *streamPool) get(ctx context.Context, p peer.ID) (*stream, bool) {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	for ps := range sp.m[p] {
		sp.deleteLocked(ps, p)
		if ps.err() != nil {
			// Stream went bad and hasn't deleted itself yet.
			continue
		}
		return ps, true
	}
	return nil, false
}

func (sp *streamPool) put(ps *stream, p peer.ID) {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	if ps.err() != nil {
		return
	}
	if sp.m == nil {
		sp.m = make(map[peer.ID]map[*stream]struct{})
	}
	if sp.m[p] == nil {
		sp.m[p] = make(map[*stream]struct{})
	}
	sp.m[p][ps] = struct{}{}
}

func (sp *streamPool) delete(ps *stream, p peer.ID) {
	sp.mu.Lock()
	sp.deleteLocked(ps, p)
	sp.mu.Unlock()
}

func (sp *streamPool) deleteLocked(ps *stream, p peer.ID) {
	delete(sp.m[p], ps)
	if len(sp.m) == 0 {
		delete(sp.m, p)
	}
}
