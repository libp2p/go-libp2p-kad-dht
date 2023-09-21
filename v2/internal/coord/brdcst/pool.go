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

type stateMachine = coordt.StateMachine[BroadcastEvent, BroadcastState]

// Pool is a [coordt.StateMachine] that manages all running broadcast
// operations. In the future it could limit the number of concurrent operations,
// but right now it is just a mediator between a query pool and the broadcast
// operations.
//
// Conceptually, a broadcast consists of finding the closest nodes to a certain
// key and then storing the record with them. There are a few different
// strategies that can be applied. For now these are the [FollowUp] and the [Optimistic]
// strategies. In the future, we also want to support [Reprovide Sweep].
// However, this requires a different type of query as we are not looking for
// the closest peers but rather enumerating the keyspace. In any case, this
// broadcast [Pool] would mediate between both components.
//
// [Reprovide Sweep]: https://www.notion.so/pl-strflt/DHT-Reprovide-Sweep-3108adf04e9d4086bafb727b17ae033d?pvs=4
type Pool[K kad.Key[K], N kad.NodeID[K], M coordt.Message] struct {
	qp  *query.Pool[K, N, M]           // the query pool of "get closer peers" queries
	bcs map[query.QueryID]stateMachine // all currently running broadcast operations
	cfg ConfigPool                     // cfg is a copy of the optional configuration supplied to the Pool
}

// NewPool manages all running broadcast operations.
func NewPool[K kad.Key[K], N kad.NodeID[K], M coordt.Message](self N, cfg *ConfigPool) (*Pool[K, N, M], error) {
	if cfg == nil {
		cfg = DefaultPoolConfig()
	} else if err := cfg.Validate(); err != nil {
		return nil, err
	}

	qp, err := query.NewPool[K, N, M](self, cfg.pCfg)
	if err != nil {
		return nil, fmt.Errorf("new query pool: %w", err)
	}

	return &Pool[K, N, M]{
		qp:  qp,
		bcs: map[query.QueryID]stateMachine{},
		cfg: *cfg,
	}, nil
}

// Advance advances the state of the broadcast [Pool].
func (p *Pool[K, N, M]) Advance(ctx context.Context, ev PoolEvent) (out PoolState) {
	ctx, span := tele.StartSpan(ctx, "Pool.Advance", trace.WithAttributes(tele.AttrInEvent(ev)))
	defer func() {
		span.SetAttributes(tele.AttrOutEvent(out))
		span.End()
	}()

	eventQueryID := query.InvalidQueryID
	switch ev := ev.(type) {
	case *EventPoolStartBroadcast[K, N, M]:
		eventQueryID = ev.QueryID

		// first initialize the state machine for the broadcast desired strategy
		switch ev.Config.(type) {
		case *ConfigFollowUp:
			p.bcs[ev.QueryID] = NewFollowUp(ev.QueryID, p.qp, ev.Message)
		case *ConfigOptimistic:
			panic("implement me")
		}

		// start the new state machine
		bev := &EventBroadcastStart[K, N]{
			QueryID: ev.QueryID,
			Target:  ev.Target,
			Seed:    ev.Seed,
		}

		state := p.advanceBroadcast(ctx, p.bcs[ev.QueryID], bev)
		if state != nil {
			return state
		}

	case *EventPoolStopBroadcast:
		b, ok := p.bcs[ev.QueryID]
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
		b, ok := p.bcs[ev.QueryID]
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
		b, ok := p.bcs[ev.QueryID]
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
		b, ok := p.bcs[ev.QueryID]
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
		b, ok := p.bcs[ev.QueryID]
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

	if len(p.bcs) == 0 {
		return &StatePoolIdle{}
	}

	for queryID, broadcast := range p.bcs {
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
	case *StateBroadcastFinished[K, N]:
		delete(p.bcs, st.QueryID)
		return &StatePoolBroadcastFinished[K, N]{
			QueryID:   st.QueryID,
			Contacted: st.Contacted,
			Errors:    st.Errors,
		}

	}

	return nil
}

// PoolState must be implemented by all states that a [Pool] can reach. States
// are basically the events that the [Pool] emits that other state machines or
// behaviours could react upon.
type PoolState interface {
	poolState()
}

type StatePoolFindCloser[K kad.Key[K], N kad.NodeID[K]] struct {
	QueryID query.QueryID
	Target  K // the key that the query wants to find closer nodes for
	NodeID  N // the node to send the message to
	Stats   query.QueryStats
}

type StatePoolWaiting struct {
	QueryID query.QueryID
}

type StatePoolStoreRecord[K kad.Key[K], N kad.NodeID[K], M coordt.Message] struct {
	QueryID query.QueryID
	NodeID  N
	Message M
}

type StatePoolBroadcastFinished[K kad.Key[K], N kad.NodeID[K]] struct {
	QueryID   query.QueryID
	Contacted []N
	Errors    map[string]struct {
		Node N
		Err  error
	}
}

type StatePoolIdle struct{}

func (*StatePoolFindCloser[K, N]) poolState()        {}
func (*StatePoolWaiting) poolState()                 {}
func (*StatePoolStoreRecord[K, N, M]) poolState()    {}
func (*StatePoolBroadcastFinished[K, N]) poolState() {}
func (*StatePoolIdle) poolState()                    {}

// PoolEvent is an event intended to advance the state of the [Pool] state
// machine. The [Pool] state machine only operates on events that implement
// this interface. An "Event" is the opposite of a "State". An "Event" flows
// into the state machine and a "State" flows out of it.
type PoolEvent interface {
	poolEvent()
}

// EventPoolPoll is an event that signals the [Pool] state machine that
// it can perform housekeeping work such as time out queries.
type EventPoolPoll struct{}

// EventPoolStartBroadcast is an event that attempts to start a new broadcast
// operation. This is the entry point.
type EventPoolStartBroadcast[K kad.Key[K], N kad.NodeID[K], M coordt.Message] struct {
	QueryID query.QueryID // a unique ID for this operation
	Target  K             // the key we want to store the record for
	Message M             // the message that we want to send to the closest peers (this encapsulates the payload we want to store)
	Seed    []N           // the closest nodes we know so far and from where we start the operation
	Config  Config        // the configuration for this operation. Most importantly, this defines the broadcast strategy ([FollowUp] or [Optimistic])
}

// EventPoolStopBroadcast notifies a [Pool] to stop a query.
type EventPoolStopBroadcast struct {
	QueryID query.QueryID // the id of the query that should be stopped
}

type EventPoolGetCloserNodesSuccess[K kad.Key[K], N kad.NodeID[K]] struct {
	QueryID     query.QueryID // the id of the query that sent the message
	NodeID      N             // the node the message was sent to
	Target      K
	CloserNodes []N // the closer nodes sent by the node
}

// EventPoolGetCloserNodesFailure notifies a [Pool] that an attempt to contact a node has failed.
type EventPoolGetCloserNodesFailure[K kad.Key[K], N kad.NodeID[K]] struct {
	QueryID query.QueryID // the id of the query that sent the message
	NodeID  N             // the node the message was sent to
	Target  K
	Error   error // the error that caused the failure, if any
}

type EventPoolStoreRecordSuccess[K kad.Key[K], N kad.NodeID[K], M coordt.Message] struct {
	QueryID  query.QueryID // the id of the query that sent the message
	NodeID   N             // the node the message was sent to
	Request  M
	Response M
}

// EventPoolStoreRecordFailure notifies a [Pool] that an attempt to contact a node has failed.
type EventPoolStoreRecordFailure[K kad.Key[K], N kad.NodeID[K], M coordt.Message] struct {
	QueryID query.QueryID // the id of the query that sent the message
	NodeID  N             // the node the message was sent to
	Request M
	Error   error // the error that caused the failure, if any
}

// poolEvent() ensures that only events accepted by a [Pool] can be assigned to
// the [PoolEvent] interface.
func (*EventPoolStopBroadcast) poolEvent()               {}
func (*EventPoolPoll) poolEvent()                        {}
func (*EventPoolStartBroadcast[K, N, M]) poolEvent()     {}
func (*EventPoolGetCloserNodesSuccess[K, N]) poolEvent() {}
func (*EventPoolGetCloserNodesFailure[K, N]) poolEvent() {}
func (*EventPoolStoreRecordSuccess[K, N, M]) poolEvent() {}
func (*EventPoolStoreRecordFailure[K, N, M]) poolEvent() {}
