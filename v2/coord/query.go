package kademlia

import (
	"context"
	"fmt"
	"sync"

	"github.com/plprobelab/go-kademlia/kad"
	"github.com/plprobelab/go-kademlia/query"
	"github.com/plprobelab/go-kademlia/util"
	"golang.org/x/exp/slog"
)

type PooledQueryBehaviour[K kad.Key[K], A kad.Address[A]] struct {
	pool    *query.Pool[K, A]
	waiters map[query.QueryID]NotifyCloser[DhtEvent]

	pendingMu sync.Mutex
	pending   []DhtEvent
	ready     chan struct{}

	logger *slog.Logger
}

func NewPooledQueryBehaviour[K kad.Key[K], A kad.Address[A]](pool *query.Pool[K, A], logger *slog.Logger) *PooledQueryBehaviour[K, A] {
	h := &PooledQueryBehaviour[K, A]{
		pool:    pool,
		waiters: make(map[query.QueryID]NotifyCloser[DhtEvent]),
		ready:   make(chan struct{}, 1),
		logger:  logger,
	}
	return h
}

func (r *PooledQueryBehaviour[K, A]) Notify(ctx context.Context, ev DhtEvent) {
	ctx, span := util.StartSpan(ctx, "PooledQueryBehaviour.Notify")
	defer span.End()

	r.pendingMu.Lock()
	defer r.pendingMu.Unlock()

	var cmd query.PoolEvent
	switch ev := ev.(type) {
	case *EventStartQuery[K, A]:
		cmd = &query.EventPoolAddQuery[K, A]{
			QueryID:           ev.QueryID,
			Target:            ev.Target,
			ProtocolID:        ev.ProtocolID,
			Message:           ev.Message,
			KnownClosestNodes: ev.KnownClosestNodes,
		}
		if ev.Notify != nil {
			r.waiters[ev.QueryID] = ev.Notify
		}

	case *EventStopQuery:
		cmd = &query.EventPoolStopQuery{
			QueryID: ev.QueryID,
		}

	case *EventGetClosestNodesSuccess[K, A]:
		for _, info := range ev.ClosestNodes {
			// TODO: do this after advancing pool
			r.pending = append(r.pending, &EventDhtAddNodeInfo[K, A]{
				NodeInfo: info,
			})
		}
		waiter, ok := r.waiters[ev.QueryID]
		if ok {
			waiter.Notify(ctx, &EventQueryProgressed[K, A]{
				NodeID:   ev.To.ID(),
				QueryID:  ev.QueryID,
				Response: ClosestNodesFakeResponse(ev.Target, ev.ClosestNodes),
				// Stats:    stats,
			})
		}
		cmd = &query.EventPoolMessageResponse[K, A]{
			NodeID:   ev.To.ID(),
			QueryID:  ev.QueryID,
			Response: ClosestNodesFakeResponse(ev.Target, ev.ClosestNodes),
		}
	case *EventGetClosestNodesFailure[K, A]:
		cmd = &query.EventPoolMessageFailure[K]{
			NodeID:  ev.To.ID(),
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

func (r *PooledQueryBehaviour[K, A]) Ready() <-chan struct{} {
	return r.ready
}

func (r *PooledQueryBehaviour[K, A]) Perform(ctx context.Context) (DhtEvent, bool) {
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

func (r *PooledQueryBehaviour[K, A]) advancePool(ctx context.Context, ev query.PoolEvent) (DhtEvent, bool) {
	ctx, span := util.StartSpan(ctx, "PooledQueryBehaviour.advancePool")
	defer span.End()

	pstate := r.pool.Advance(ctx, ev)
	switch st := pstate.(type) {
	case *query.StatePoolQueryMessage[K, A]:
		return &EventOutboundGetClosestNodes[K, A]{
			QueryID: st.QueryID,
			To:      NewNodeAddr[K, A](st.NodeID, nil),
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
