package brdcst

import (
	"context"
	"fmt"

	"github.com/plprobelab/go-kademlia/kad"
	"go.opentelemetry.io/otel/trace"

	"github.com/libp2p/go-libp2p-kad-dht/v2/internal/coord/query"
	"github.com/libp2p/go-libp2p-kad-dht/v2/pb"
	"github.com/libp2p/go-libp2p-kad-dht/v2/tele"
)

type Strategy string

const (
	StrategyFollowUp   Strategy = "follow-up"
	StrategyOptimistic Strategy = "optimistic"
)

// TODO: consolidate with coord.SM.
type stateMachine interface {
	Advance(context.Context, BroadcastEvent) BroadcastState
}

type Pool[K kad.Key[K], N kad.NodeID[K]] struct {
	queryPool *query.Pool[K, N, *pb.Message] // TODO: remove *pb.Message

	broadcasts map[query.QueryID]stateMachine

	// cfg is a copy of the optional configuration supplied to the Pool
	cfg BroadcastConfig[K]
}

// NewPool // TODO: replace *pb.Message
func NewPool[K kad.Key[K], N kad.NodeID[K]](queryPool *query.Pool[K, N, *pb.Message], cfg *BroadcastConfig[K]) (*Pool[K, N], error) {
	if cfg == nil {
		cfg = DefaultPoolConfig[K]()
	} else if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return &Pool[K, N]{
		queryPool:  queryPool,
		broadcasts: map[query.QueryID]stateMachine{},
		cfg:        *cfg,
	}, nil
}

// Advance advances the state of the bootstrap by attempting to advance its query if running.
func (p *Pool[K, N]) Advance(ctx context.Context, ev PoolEvent) PoolState {
	ctx, span := tele.StartSpan(ctx, "Pool.Advance", trace.WithAttributes(tele.AttrInEvent(ev)))
	defer span.End()

	eventQueryID := query.InvalidQueryID
	switch ev := ev.(type) {
	case *EventPoolAddBroadcast[K, N]:
		switch ev.Strategy {
		case StrategyFollowUp:
			p.broadcasts[ev.QueryID] = NewFollowUp(ev.QueryID, p.queryPool)
		case StrategyOptimistic:
			panic("implement me")
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
		if state.IsTerminal() {
			return state
		}

	case *EventPoolNodeResponse[K, N]:
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
		if state.IsTerminal() {
			return state
		}
	case *EventPoolNodeFailure[K, N]:
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
		if state.IsTerminal() {
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
		if state.IsTerminal() {
			return state
		}
	}

	return &StatePoolIdle{}
}

func (p *Pool[K, N]) advanceBroadcast(ctx context.Context, sm stateMachine, bev BroadcastEvent) PoolState {
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
	case *StateBroadcastStoreRecord[K, N]:
		return &StatePoolStoreRecord[K, N]{
			QueryID: st.QueryID,
			NodeID:  st.NodeID,
		}
	case *StateBroadcastFinished:
		delete(p.broadcasts, st.QueryID)
		return &StatePoolFinished{
			QueryID: st.QueryID,
			Stats:   st.Stats,
		}

	}

	return nil
}
