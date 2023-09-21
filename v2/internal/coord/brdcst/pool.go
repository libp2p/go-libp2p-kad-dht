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

type Strategy string

const (
	StrategyFollowUp   Strategy = "follow-up"
	StrategyOptimistic Strategy = "optimistic"
)

type Pool[K kad.Key[K], N kad.NodeID[K], M coordt.Message] struct {
	queryPool *query.Pool[K, N, M]

	broadcasts map[query.QueryID]coordt.StateMachine[BroadcastEvent, BroadcastState]

	// cfg is a copy of the optional configuration supplied to the Pool
	cfg BroadcastConfig
}

// NewPool manages all running broadcast operations.
func NewPool[K kad.Key[K], N kad.NodeID[K], M coordt.Message](queryPool *query.Pool[K, N, M], cfg *BroadcastConfig) (*Pool[K, N, M], error) {
	if cfg == nil {
		cfg = DefaultPoolConfig()
	} else if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return &Pool[K, N, M]{
		queryPool:  queryPool,
		broadcasts: map[query.QueryID]coordt.StateMachine[BroadcastEvent, BroadcastState]{},
		cfg:        *cfg,
	}, nil
}

// Advance advances the state of the bootstrap by attempting to advance its query if running.
func (p *Pool[K, N, M]) Advance(ctx context.Context, ev PoolEvent) PoolState {
	ctx, span := tele.StartSpan(ctx, "Pool.Advance", trace.WithAttributes(tele.AttrInEvent(ev)))
	defer span.End()

	eventQueryID := query.InvalidQueryID
	switch ev := ev.(type) {
	case *EventPoolAddBroadcast[K, N, M]:
		switch ev.Strategy {
		case StrategyFollowUp:
			p.broadcasts[ev.QueryID] = NewFollowUp(ev.QueryID, p.queryPool, ev.Message)
		case StrategyOptimistic:
			panic("implement me")
		}

		bev := &EventBroadcastStart[K, N]{
			QueryID:           ev.QueryID,
			Target:            ev.Target,
			KnownClosestNodes: ev.KnownClosestNodes,
		}

		eventQueryID = ev.QueryID
		state := p.advanceBroadcast(ctx, p.broadcasts[ev.QueryID], bev)
		if state != nil {
			return state
		}

	case *EventPoolStopBroadcast:
		b, ok := p.broadcasts[ev.QueryID]
		if !ok {
			break
		}

		eventQueryID = ev.QueryID
		bev := &EventBroadcastStop{
			QueryID: ev.QueryID,
		}

		state := p.advanceBroadcast(ctx, b, bev)
		if state != nil {
			return state
		}

	case *EventPoolGetCloserNodesSuccess[K, N]:
		b, ok := p.broadcasts[ev.QueryID]
		if !ok {
			break
		}

		eventQueryID = ev.QueryID
		bev := &EventBroadcastNodeResponse[K, N]{
			QueryID:     ev.QueryID,
			NodeID:      ev.NodeID,
			CloserNodes: ev.CloserNodes,
		}

		state := p.advanceBroadcast(ctx, b, bev)
		if state != nil {
			return state
		}

	case *EventPoolGetCloserNodesFailure[K, N]:
		b, ok := p.broadcasts[ev.QueryID]
		if !ok {
			break
		}

		eventQueryID = ev.QueryID
		bev := &EventBroadcastNodeFailure[K, N]{
			QueryID: ev.QueryID,
			NodeID:  ev.NodeID,
			Error:   ev.Error,
		}

		state := p.advanceBroadcast(ctx, b, bev)
		if state != nil {
			return state
		}

	case *EventPoolStoreRecordSuccess[K, N, M]:
		b, ok := p.broadcasts[ev.QueryID]
		if !ok {
			break
		}

		eventQueryID = ev.QueryID
		bev := &EventBroadcastStoreRecordSuccess[K, N, M]{
			QueryID:  ev.QueryID,
			NodeID:   ev.NodeID,
			Request:  ev.Request,
			Response: ev.Response,
		}

		state := p.advanceBroadcast(ctx, b, bev)
		if state != nil {
			return state
		}

	case *EventPoolStoreRecordFailure[K, N, M]:
		b, ok := p.broadcasts[ev.QueryID]
		if !ok {
			break
		}

		eventQueryID = ev.QueryID
		bev := &EventBroadcastStoreRecordFailure[K, N, M]{
			QueryID: ev.QueryID,
			NodeID:  ev.NodeID,
			Request: ev.Request,
			Error:   ev.Error,
		}

		state := p.advanceBroadcast(ctx, b, bev)
		if state != nil {
			return state
		}

	case *EventPoolPoll:
		// no event to process

	default:
		panic(fmt.Sprintf("unexpected event: %T", ev))
	}

	if len(p.broadcasts) == 0 {
		return &StatePoolIdle{}
	}

	for queryID, broadcast := range p.broadcasts {
		if eventQueryID == queryID {
			continue
		}

		state := p.advanceBroadcast(ctx, broadcast, &EventBroadcastPoll{})
		if state != nil {
			return state
		}
	}

	return &StatePoolIdle{}
}

func (p *Pool[K, N, M]) advanceBroadcast(ctx context.Context, sm coordt.StateMachine[BroadcastEvent, BroadcastState], bev BroadcastEvent) PoolState {
	ctx, span := tele.StartSpan(ctx, "Pool.advanceBroadcast", trace.WithAttributes(tele.AttrInEvent(bev)))
	defer span.End()

	state := sm.Advance(ctx, bev)
	switch st := state.(type) {
	case *StateBroadcastFindCloser[K, N]:
		return &StatePoolFindCloser[K, N]{
			QueryID: st.QueryID,
			Stats:   st.Stats,
			NodeID:  st.NodeID,
			Target:  st.Target,
		}
	case *StateBroadcastWaiting:
		return &StatePoolWaiting{}
	case *StateBroadcastStoreRecord[K, N, M]:
		return &StatePoolStoreRecord[K, N, M]{
			QueryID: st.QueryID,
			NodeID:  st.NodeID,
			Message: st.Message,
		}
	case *StateBroadcastFinished:
		delete(p.broadcasts, st.QueryID)
		return &StatePoolBroadcastFinished{
			QueryID: st.QueryID,
		}

	}

	return nil
}
