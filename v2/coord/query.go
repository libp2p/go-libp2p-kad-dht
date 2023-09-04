package coord

import (
	"context"
	"fmt"
	"sync"

	"github.com/libp2p/go-libp2p-kad-dht/v2/kadt"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/plprobelab/go-kademlia/key"
	"github.com/plprobelab/go-kademlia/query"
	"github.com/plprobelab/go-kademlia/util"
	"golang.org/x/exp/slog"
)

type PooledQueryBehaviour struct {
	pool    *query.Pool[key.Key256, ma.Multiaddr]
	waiters map[query.QueryID]NotifyCloser[DhtEvent]

	pendingMu sync.Mutex
	pending   []DhtEvent
	ready     chan struct{}

	logger *slog.Logger
}

func NewPooledQueryBehaviour(pool *query.Pool[key.Key256, ma.Multiaddr], logger *slog.Logger) *PooledQueryBehaviour {
	h := &PooledQueryBehaviour{
		pool:    pool,
		waiters: make(map[query.QueryID]NotifyCloser[DhtEvent]),
		ready:   make(chan struct{}, 1),
		logger:  logger,
	}
	return h
}

func (r *PooledQueryBehaviour) Notify(ctx context.Context, ev DhtEvent) {
	ctx, span := util.StartSpan(ctx, "PooledQueryBehaviour.Notify")
	defer span.End()

	r.pendingMu.Lock()
	defer r.pendingMu.Unlock()

	var cmd query.PoolEvent
	switch ev := ev.(type) {
	case *EventStartQuery:
		cmd = &query.EventPoolAddQuery[key.Key256, ma.Multiaddr]{
			QueryID:           ev.QueryID,
			Target:            ev.Target,
			ProtocolID:        ev.ProtocolID,
			Message:           ev.Message,
			KnownClosestNodes: SliceOfPeerIDToSliceOfNodeID(ev.KnownClosestNodes),
		}
		if ev.Notify != nil {
			r.waiters[ev.QueryID] = ev.Notify
		}

	case *EventStopQuery:
		cmd = &query.EventPoolStopQuery{
			QueryID: ev.QueryID,
		}

	case *EventGetCloserNodesSuccess:
		for _, info := range ev.CloserNodes {
			// TODO: do this after advancing pool
			r.pending = append(r.pending, &EventDhtAddNodeInfo{
				NodeInfo: info,
			})
		}
		waiter, ok := r.waiters[ev.QueryID]
		if ok {
			waiter.Notify(ctx, &EventQueryProgressed{
				NodeID:   ev.To.ID,
				QueryID:  ev.QueryID,
				Response: CloserNodesResponse(ev.Target, ev.CloserNodes),
				// Stats:    stats,
			})
		}
		cmd = &query.EventPoolMessageResponse[key.Key256, ma.Multiaddr]{
			NodeID:   kadt.PeerID(ev.To.ID),
			QueryID:  ev.QueryID,
			Response: CloserNodesResponse(ev.Target, ev.CloserNodes),
		}
	case *EventGetCloserNodesFailure:
		cmd = &query.EventPoolMessageFailure[key.Key256]{
			NodeID:  kadt.PeerID(ev.To.ID),
			QueryID: ev.QueryID,
			Error:   ev.Err,
		}
	default:
		panic(fmt.Sprintf("unexpected dht event: %T", ev))
	}

	// attempt to advance the query pool
	ev, ok := r.advancePool(ctx, cmd)
	if ok {
		r.pending = append(r.pending, ev)
	}
	if len(r.pending) > 0 {
		select {
		case r.ready <- struct{}{}:
		default:
		}
	}
}

func (r *PooledQueryBehaviour) Ready() <-chan struct{} {
	return r.ready
}

func (r *PooledQueryBehaviour) Perform(ctx context.Context) (DhtEvent, bool) {
	ctx, span := util.StartSpan(ctx, "PooledQueryBehaviour.Perform")
	defer span.End()

	// No inbound work can be done until Perform is complete
	r.pendingMu.Lock()
	defer r.pendingMu.Unlock()

	for {
		// drain queued events first.
		if len(r.pending) > 0 {
			var ev DhtEvent
			ev, r.pending = r.pending[0], r.pending[1:]

			if len(r.pending) > 0 {
				select {
				case r.ready <- struct{}{}:
				default:
				}
			}
			return ev, true
		}

		// attempt to advance the query pool
		ev, ok := r.advancePool(ctx, &query.EventPoolPoll{})
		if ok {
			return ev, true
		}

		if len(r.pending) == 0 {
			return nil, false
		}
	}
}

func (r *PooledQueryBehaviour) advancePool(ctx context.Context, ev query.PoolEvent) (DhtEvent, bool) {
	ctx, span := util.StartSpan(ctx, "PooledQueryBehaviour.advancePool")
	defer span.End()

	pstate := r.pool.Advance(ctx, ev)
	switch st := pstate.(type) {
	case *query.StatePoolQueryMessage[key.Key256, ma.Multiaddr]:
		return &EventOutboundGetClosestNodes{
			QueryID: st.QueryID,
			To:      NodeIDToAddrInfo(st.NodeID),
			Target:  st.Message.Target(),
			Notify:  r,
		}, true
	case *query.StatePoolWaitingAtCapacity:
		// nothing to do except wait for message response or timeout
	case *query.StatePoolWaitingWithCapacity:
		// nothing to do except wait for message response or timeout
	case *query.StatePoolQueryFinished:
		waiter, ok := r.waiters[st.QueryID]
		if ok {
			waiter.Notify(ctx, &EventQueryFinished{
				QueryID: st.QueryID,
				Stats:   st.Stats,
			})
			waiter.Close()
		}
	case *query.StatePoolQueryTimeout:
		// TODO
	case *query.StatePoolIdle:
		// nothing to do
	default:
		panic(fmt.Sprintf("unexpected pool state: %T", st))
	}

	return nil, false
}
