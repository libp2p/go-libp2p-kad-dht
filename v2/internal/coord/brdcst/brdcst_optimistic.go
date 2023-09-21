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

type Optimistic[K kad.Key[K], N kad.NodeID[K], M coordt.Message] struct {
	queryID query.QueryID
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

func NewOptimistic[K kad.Key[K], N kad.NodeID[K], M coordt.Message](qid query.QueryID, pool *query.Pool[K, N, M], msg M) *Optimistic[K, N, M] {
	return &Optimistic[K, N, M]{
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

func (f *Optimistic[K, N, M]) Advance(ctx context.Context, ev BroadcastEvent) (out BroadcastState) {
	ctx, span := tele.StartSpan(ctx, "Optimistic.Advance", trace.WithAttributes(tele.AttrInEvent(ev)))
	defer func() {
		span.SetAttributes(tele.AttrOutEvent(out))
		span.End()
	}()

	switch ev := ev.(type) {
	case *EventBroadcastStart[K, N]:
		cmd := &query.EventPoolAddFindCloserQuery[K, N]{
			QueryID:           ev.QueryID,
			Target:            ev.Target,
			KnownClosestNodes: ev.KnownClosestNodes,
		}
		return f.advancePool(ctx, cmd)
	case *EventBroadcastStop:
		// TODO: ...
	case *EventBroadcastNodeResponse[K, N]:
		cmd := &query.EventPoolNodeResponse[K, N]{
			QueryID:     ev.QueryID,
			NodeID:      ev.NodeID,
			CloserNodes: ev.CloserNodes,
		}
		return f.advancePool(ctx, cmd)
	case *EventBroadcastNodeFailure[K, N]:
		cmd := &query.EventPoolNodeFailure[K, N]{
			QueryID: ev.QueryID,
			NodeID:  ev.NodeID,
			Error:   ev.Error,
		}
		return f.advancePool(ctx, cmd)
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
		return f.advancePool(ctx, &query.EventPoolPoll{})
	default:
		panic(fmt.Sprintf("unexpected event: %T", ev))
	}

	// TODO: remove duplication with below
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

func (f *Optimistic[K, N, M]) advancePool(ctx context.Context, qev query.PoolEvent) BroadcastState {
	ctx, span := tele.StartSpan(ctx, "Optimistic.advanceQuery")
	defer span.End()

	state := f.pool.Advance(ctx, qev)
	switch st := state.(type) {
	case *query.StatePoolFindCloser[K, N]:
		return &StateBroadcastFindCloser[K, N]{
			QueryID: st.QueryID,
			NodeID:  st.NodeID,
			Target:  st.Target,
			Stats:   st.Stats,
		}
	case *query.StatePoolWaitingAtCapacity:
		return &StateBroadcastWaiting{
			QueryID: f.queryID,
		}
	case *query.StatePoolWaitingWithCapacity:
		return &StateBroadcastWaiting{
			QueryID: f.queryID,
		}
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
		}
	case *query.StatePoolIdle:
		// nothing to do
	default:
		panic(fmt.Sprintf("unexpected pool state: %T", st))
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
