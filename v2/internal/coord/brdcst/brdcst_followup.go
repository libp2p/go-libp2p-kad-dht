package brdcst

import (
	"context"
	"fmt"

	"github.com/libp2p/go-libp2p-kad-dht/v2/pb"

	"github.com/libp2p/go-libp2p-kad-dht/v2/internal/coord/query"
	"github.com/libp2p/go-libp2p-kad-dht/v2/tele"
	"github.com/plprobelab/go-kademlia/kad"
	"go.opentelemetry.io/otel/trace"
)

type FollowUp[K kad.Key[K], N kad.NodeID[K]] struct {
	queryID query.QueryID
	pool    *query.Pool[K, N, *pb.Message]
	qDone   bool // query done?

	// nodes we still need to store records with
	todo    map[string]N
	waiting map[string]N
	success map[string]N
	failed  map[string]N
}

var _ stateMachine

func NewFollowUp[K kad.Key[K], N kad.NodeID[K]](qid query.QueryID, pool *query.Pool[K, N, *pb.Message]) *FollowUp[K, N] {
	return &FollowUp[K, N]{
		queryID: qid,
		pool:    pool,
		qDone:   false,
		todo:    map[string]N{},
		waiting: map[string]N{},
		success: map[string]N{},
		failed:  map[string]N{},
	}
}

func (f *FollowUp[K, N]) Advance(ctx context.Context, ev BroadcastEvent) BroadcastState {
	ctx, span := tele.StartSpan(ctx, "FollowUp.Advance", trace.WithAttributes(tele.AttrInEvent(ev)))
	defer span.End()

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
		if f.qDone {
			delete(f.waiting, ev.NodeID.String())
			f.success[ev.NodeID.String()] = ev.NodeID
		} else {
			cmd := &query.EventPoolNodeResponse[K, N]{
				QueryID:     ev.QueryID,
				NodeID:      ev.NodeID,
				CloserNodes: ev.CloserNodes,
			}
			return f.advancePool(ctx, cmd)
		}
	case *EventBroadcastNodeFailure[K, N]:
		if f.qDone {
			delete(f.waiting, ev.NodeID.String())
			f.failed[ev.NodeID.String()] = ev.NodeID
		} else {
			cmd := &query.EventPoolNodeFailure[K, N]{
				QueryID: ev.QueryID,
				NodeID:  ev.NodeID,
				Error:   ev.Error,
			}
			return f.advancePool(ctx, cmd)
		}
	case *EventBroadcastPoll:
		// ignore, nothing to do
	default:
		panic(fmt.Sprintf("unexpected event: %T", ev))
	}

	// TODO: remove duplication with below
	for k, n := range f.todo {
		delete(f.todo, k)
		f.waiting[k] = n
		return &StateBroadcastStoreRecord[K, N]{
			QueryID: f.queryID,
			NodeID:  n,
		}
	}

	if len(f.waiting) > 0 {
		return &StateBroadcastWaiting{}
	}

	// TODO: capacity handling
	return &StateBroadcastIdle{}
}

func (f *FollowUp[K, N]) advancePool(ctx context.Context, qev query.PoolEvent) BroadcastState {
	ctx, span := tele.StartSpan(ctx, "Broadcast.advanceQuery")
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
		// nothing to do except wait for message response or timeout
	case *query.StatePoolWaitingWithCapacity:
		// nothing to do except wait for message response or timeout
	case *query.StatePoolQueryFinished[K, N]:
		f.qDone = true

		for _, n := range st.ClosestNodes {
			f.todo[n.String()] = n
		}

	case *query.StatePoolQueryTimeout:
		return &StateBroadcastFinished{
			QueryID: st.QueryID,
			Stats:   st.Stats,
		}
	case *query.StatePoolIdle:
		// nothing to do
	default:
		panic(fmt.Sprintf("unexpected pool state: %T", st))
	}

	for k, n := range f.todo {
		delete(f.todo, k)
		f.waiting[k] = n
		return &StateBroadcastStoreRecord[K, N]{
			QueryID: f.queryID,
			NodeID:  n,
		}
	}

	if len(f.waiting) > 0 {
		return &StateBroadcastWaiting{}
	}

	// TODO: capacity handling
	return &StateBroadcastIdle{}
}
