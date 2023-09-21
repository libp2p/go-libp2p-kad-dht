package brdcst

import (
	"context"
	"fmt"

	"github.com/plprobelab/go-kademlia/kad"
	"go.opentelemetry.io/otel/trace"

	"github.com/libp2p/go-libp2p-kad-dht/v2/internal/coord/coordt"
	"github.com/libp2p/go-libp2p-kad-dht/v2/internal/coord/query"
	"github.com/libp2p/go-libp2p-kad-dht/v2/tele"
)

type FollowUp[K kad.Key[K], N kad.NodeID[K], M coordt.Message] struct {
	queryID coordt.QueryID
	pool    *query.Pool[K, N, M]
	msg     M
	closest []N

	// nodes we still need to store records with
	todo    map[string]N
	waiting map[string]N
	success map[string]N
	failed  map[string]struct {
		Node N
		Err  error
	}
}

func NewFollowUp[K kad.Key[K], N kad.NodeID[K], M coordt.Message](qid coordt.QueryID, pool *query.Pool[K, N, M], msg M) *FollowUp[K, N, M] {
	return &FollowUp[K, N, M]{
		queryID: qid,
		pool:    pool,
		msg:     msg,
		todo:    map[string]N{},
		waiting: map[string]N{},
		success: map[string]N{},
		failed: map[string]struct {
			Node N
			Err  error
		}{},
	}
}

func (f *FollowUp[K, N, M]) Advance(ctx context.Context, ev BroadcastEvent) (out BroadcastState) {
	ctx, span := tele.StartSpan(ctx, "FollowUp.Advance", trace.WithAttributes(tele.AttrInEvent(ev)))
	defer func() {
		span.SetAttributes(tele.AttrOutEvent(out))
		span.End()
	}()

	pev := f.handleEvent(ctx, ev)
	if pev != nil {
		if state, terminal := f.advancePool(ctx, pev); terminal {
			return state
		}
	}

	for k, n := range f.todo {
		delete(f.todo, k)
		f.waiting[k] = n
		return &StateBroadcastStoreRecord[K, N, M]{
			QueryID: f.queryID,
			NodeID:  n,
			Message: f.msg,
		}
	}

	if len(f.waiting) > 0 {
		return &StateBroadcastWaiting{}
	}

	if len(f.todo) == 0 && len(f.closest) != 0 {
		return &StateBroadcastFinished[K, N]{
			QueryID:   f.queryID,
			Contacted: f.closest,
			Errors:    f.failed,
		}
	}

	return &StateBroadcastIdle{}
}

func (f *FollowUp[K, N, M]) handleEvent(ctx context.Context, ev BroadcastEvent) query.PoolEvent {
	switch ev := ev.(type) {
	case *EventBroadcastStart[K, N]:
		return &query.EventPoolAddFindCloserQuery[K, N]{
			QueryID: ev.QueryID,
			Target:  ev.Target,
			Seed:    ev.Seed,
		}
	case *EventBroadcastStop:
		// TODO: stop outstanding storage requests
		return &query.EventPoolStopQuery{
			QueryID: ev.QueryID,
		}
	case *EventBroadcastNodeResponse[K, N]:
		return &query.EventPoolNodeResponse[K, N]{
			QueryID:     ev.QueryID,
			NodeID:      ev.NodeID,
			CloserNodes: ev.CloserNodes,
		}
	case *EventBroadcastNodeFailure[K, N]:
		return &query.EventPoolNodeFailure[K, N]{
			QueryID: ev.QueryID,
			NodeID:  ev.NodeID,
			Error:   ev.Error,
		}
	case *EventBroadcastStoreRecordSuccess[K, N, M]:
		delete(f.waiting, ev.NodeID.String())
		f.success[ev.NodeID.String()] = ev.NodeID
	case *EventBroadcastStoreRecordFailure[K, N, M]:
		delete(f.waiting, ev.NodeID.String())
		f.failed[ev.NodeID.String()] = struct {
			Node N
			Err  error
		}{Node: ev.NodeID, Err: ev.Error}
	case *EventBroadcastPoll:
		// ignore, nothing to do
		return &query.EventPoolPoll{}
	default:
		panic(fmt.Sprintf("unexpected event: %T", ev))
	}

	return nil
}

func (f *FollowUp[K, N, M]) advancePool(ctx context.Context, ev query.PoolEvent) (out BroadcastState, term bool) {
	ctx, span := tele.StartSpan(ctx, "FollowUp.advanceQuery", trace.WithAttributes(tele.AttrInEvent(ev)))
	defer func() {
		span.SetAttributes(tele.AttrOutEvent(out))
		span.End()
	}()

	state := f.pool.Advance(ctx, ev)
	switch st := state.(type) {
	case *query.StatePoolFindCloser[K, N]:
		return &StateBroadcastFindCloser[K, N]{
			QueryID: st.QueryID,
			NodeID:  st.NodeID,
			Target:  st.Target,
			Stats:   st.Stats,
		}, true
	case *query.StatePoolWaitingAtCapacity:
		return &StateBroadcastWaiting{
			QueryID: f.queryID,
		}, true
	case *query.StatePoolWaitingWithCapacity:
		return &StateBroadcastWaiting{
			QueryID: f.queryID,
		}, true
	case *query.StatePoolQueryFinished[K, N]:
		f.closest = st.ClosestNodes

		for _, n := range st.ClosestNodes {
			f.todo[n.String()] = n
		}

	case *query.StatePoolQueryTimeout:
		return &StateBroadcastFinished[K, N]{
			QueryID:   st.QueryID,
			Contacted: make([]N, 0),
			Errors: map[string]struct {
				Node N
				Err  error
			}{},
		}, true
	case *query.StatePoolIdle:
		// nothing to do
	default:
		panic(fmt.Sprintf("unexpected pool state: %T", st))
	}

	return nil, false
}
