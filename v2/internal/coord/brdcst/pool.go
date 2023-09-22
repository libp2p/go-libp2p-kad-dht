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

// Broadcast is a type alias for a specific kind of state machine that any
// kind of broadcast strategy state machine must implement. Currently, there
// are the [FollowUp] and [Optimistic] state machines.
type Broadcast = coordt.StateMachine[BroadcastEvent, BroadcastState]

// Pool is a [coordt.StateMachine] that manages all running broadcast
// operations. In the future it could limit the number of concurrent operations,
// but right now it is just keeping track of all running broadcasts. The
// referenced [query.Pool] is passed down to the respective broadcast state
// machines. This is not nice because it breaks the hierarchy but makes things
// way easier.
//
// Conceptually, a broadcast consists of finding the closest nodes to a certain
// key and then storing the record with them. There are a few different
// strategies that can be applied. For now, these are the [FollowUp] and the [Optimistic]
// strategies. In the future, we also want to support [Reprovide Sweep].
// However, this requires a different type of query as we are not looking for
// the closest nodes but rather enumerating the keyspace. In any case, this
// broadcast [Pool] would keep track of all running broadcasts.
//
// [Reprovide Sweep]: https://www.notion.so/pl-strflt/DHT-Reprovide-Sweep-3108adf04e9d4086bafb727b17ae033d?pvs=4
type Pool[K kad.Key[K], N kad.NodeID[K], M coordt.Message] struct {
	qp  *query.Pool[K, N, M]         // the query pool of "get closer peers" queries
	bcs map[coordt.QueryID]Broadcast // all currently running broadcast operations
	cfg ConfigPool                   // cfg is a copy of the optional configuration supplied to the Pool
}

// NewPool initializes a new broadcast pool. If cfg is nil, the
// [DefaultPoolConfig] will be used. Each broadcast pool creates its own query
// pool ([query.Pool]). A query pool limits the number of concurrent queries
// and already exists "stand-alone" beneath the [coord.PooledQueryBehaviour].
// We are initializing a new one in here because:
//  1. it allows us to apply different limits to either broadcast or ordinary
//     "get closer nodes" queries
//  2. the query pool logic will stay simpler
//  3. we don't need to cross communicated from the broadcast to the query pool
//     4.
func NewPool[K kad.Key[K], N kad.NodeID[K], M coordt.Message](self N, cfg *ConfigPool) (*Pool[K, N, M], error) {
	if cfg == nil {
		cfg = DefaultPoolConfig()
	} else if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("validate pool config: %w", err)
	}

	qp, err := query.NewPool[K, N, M](self, cfg.pCfg)
	if err != nil {
		return nil, fmt.Errorf("new query pool: %w", err)
	}

	return &Pool[K, N, M]{
		qp:  qp,
		bcs: map[coordt.QueryID]Broadcast{},
		cfg: *cfg,
	}, nil
}

// Advance advances the state of the broadcast [Pool]. It first handles the
// event by extracting the broadcast state machine that should handle this event
// from the [Pool.bcs] map and constructing the correct event for that broadcast
// state machine. If either the state machine wasn't found (shouldn't happen) or
// there's no corresponding broadcast event ([EventPoolPoll] for example) don't
// do anything and instead try to advance the other broadcast state machines.
func (p *Pool[K, N, M]) Advance(ctx context.Context, ev PoolEvent) (out PoolState) {
	ctx, span := tele.StartSpan(ctx, "Pool.Advance", trace.WithAttributes(tele.AttrInEvent(ev)))
	defer func() {
		span.SetAttributes(tele.AttrOutEvent(out))
		span.End()
	}()

	sm, bev := p.handleEvent(ctx, ev)
	if sm != nil && bev != nil {
		if state, terminal := p.advanceBroadcast(ctx, sm, bev); terminal {
			return state
		}
	}

	// advance other state machines until we have reached a terminal state in any
	for _, bsm := range p.bcs {
		if sm == bsm {
			continue
		}

		state, terminal := p.advanceBroadcast(ctx, bsm, &EventBroadcastPoll{})
		if terminal {
			return state
		}
	}

	return &StatePoolIdle{}
}

// handleEvent receives a broadcast [PoolEvent] and returns the corresponding
// broadcast state machine [FollowUp] or [Optimistic] plus the event for that
// state machine. If any return parameter is nil, either the pool event was for
// an unknown query or the event doesn't need to be forwarded to the state
// machine.
func (p *Pool[K, N, M]) handleEvent(ctx context.Context, ev PoolEvent) (Broadcast, BroadcastEvent) {
	switch ev := ev.(type) {
	case *EventPoolStartBroadcast[K, N, M]:
		// first initialize the state machine for the broadcast desired strategy
		switch ev.Config.(type) {
		case *ConfigFollowUp:
			p.bcs[ev.QueryID] = NewFollowUp(ev.QueryID, p.qp, ev.Message)
		case *ConfigOptimistic:
			panic("implement me")
		}

		// start the new state machine
		return p.bcs[ev.QueryID], &EventBroadcastStart[K, N]{
			Target: ev.Target,
			Seed:   ev.Seed,
		}

	case *EventPoolStopBroadcast:
		return p.bcs[ev.QueryID], &EventBroadcastStop{}

	case *EventPoolGetCloserNodesSuccess[K, N]:
		return p.bcs[ev.QueryID], &EventBroadcastNodeResponse[K, N]{
			NodeID:      ev.NodeID,
			CloserNodes: ev.CloserNodes,
		}

	case *EventPoolGetCloserNodesFailure[K, N]:
		return p.bcs[ev.QueryID], &EventBroadcastNodeFailure[K, N]{
			NodeID: ev.NodeID,
			Error:  ev.Error,
		}

	case *EventPoolStoreRecordSuccess[K, N, M]:
		return p.bcs[ev.QueryID], &EventBroadcastStoreRecordSuccess[K, N, M]{
			NodeID:   ev.NodeID,
			Request:  ev.Request,
			Response: ev.Response,
		}

	case *EventPoolStoreRecordFailure[K, N, M]:
		return p.bcs[ev.QueryID], &EventBroadcastStoreRecordFailure[K, N, M]{
			NodeID:  ev.NodeID,
			Request: ev.Request,
			Error:   ev.Error,
		}

	case *EventPoolPoll:
		// no event to process

	default:
		panic(fmt.Sprintf("unexpected event: %T", ev))
	}

	return nil, nil
}

// advanceBroadcast advances the given broadcast state machine ([FollowUp] or
// [Optimistic]) and returns the new [Pool] state ([PoolState]). The additional
// boolean value indicates whether the returned [PoolState] should be ignored.
func (p *Pool[K, N, M]) advanceBroadcast(ctx context.Context, sm Broadcast, bev BroadcastEvent) (PoolState, bool) {
	ctx, span := tele.StartSpan(ctx, "Pool.advanceBroadcast", trace.WithAttributes(tele.AttrInEvent(bev)))
	defer span.End()

	state := sm.Advance(ctx, bev)
	switch st := state.(type) {
	case *StateBroadcastFindCloser[K, N]:
		return &StatePoolFindCloser[K, N]{
			QueryID: st.QueryID,
			NodeID:  st.NodeID,
			Target:  st.Target,
		}, true
	case *StateBroadcastWaiting:
		return &StatePoolWaiting{}, true
	case *StateBroadcastStoreRecord[K, N, M]:
		return &StatePoolStoreRecord[K, N, M]{
			QueryID: st.QueryID,
			NodeID:  st.NodeID,
			Message: st.Message,
		}, true
	case *StateBroadcastFinished[K, N]:
		delete(p.bcs, st.QueryID)
		return &StatePoolBroadcastFinished[K, N]{
			QueryID:   st.QueryID,
			Contacted: st.Contacted,
			Errors:    st.Errors,
		}, true
	}

	return nil, false
}

// PoolState must be implemented by all states that a [Pool] can reach. States
// are basically the events that the [Pool] emits that other state machines or
// behaviours could react upon.
type PoolState interface {
	poolState()
}

// StatePoolFindCloser indicates to the broadcast behaviour that a broadcast
// state machine and indirectly the broadcast pool wants to query the given node
// (NodeID) for closer nodes to the target key (Target).
type StatePoolFindCloser[K kad.Key[K], N kad.NodeID[K]] struct {
	QueryID coordt.QueryID // the id of the broadcast operation that wants to send the message
	Target  K              // the key that the query wants to find closer nodes for
	NodeID  N              // the node to send the message to
}

// StatePoolWaiting indicates that the broadcast [Pool] is waiting for network
// I/O to finish. It means the [Pool] isn't idle, but there are operations
// in-flight that it is waiting on to finish.
type StatePoolWaiting struct{}

// StatePoolStoreRecord indicates to the upper layer that the broadcast [Pool]
// wants to store a record using the given Message with the given NodeID. The
// network behaviour should take over and notify the [coord.PooledBroadcastBehaviour]
// about updates.
type StatePoolStoreRecord[K kad.Key[K], N kad.NodeID[K], M coordt.Message] struct {
	QueryID coordt.QueryID // the id of the broadcast operation that wants to send the message
	NodeID  N              // the node to send the message to
	Message M              // the message that should be sent to the remote node
}

// StatePoolBroadcastFinished indicates that the broadcast operation with the
// id QueryID has finished. During that operation, all nodes in Contacted have
// been contacted to store the record. The Contacted slice does not contain
// the nodes we have queried to find the closest nodes to the target key - only
// the ones that we eventually contacted to store the record. The Errors map
// maps the string representation of any node N in the Contacted slice to a
// potential error struct that contains the original Node and error. In the best
// case, this Errors map is empty.
type StatePoolBroadcastFinished[K kad.Key[K], N kad.NodeID[K]] struct {
	QueryID   coordt.QueryID      // the id of the broadcast operation that has finished
	Contacted []N                 // all nodes we contacted to store the record (successful or not)
	Errors    map[string]struct { // any error that occurred for any node that we contacted
		Node N     // a node from the Contacted slice
		Err  error // the error that happened when contacting that Node
	}
}

// StatePoolIdle means that the broadcast [Pool] is not managing any broadcast
// operations at this time.
type StatePoolIdle struct{}

// poolState() ensures that only [PoolState]s can be returned by advancing the
// [Pool] state machine.
func (*StatePoolFindCloser[K, N]) poolState()        {}
func (*StatePoolWaiting) poolState()                 {}
func (*StatePoolStoreRecord[K, N, M]) poolState()    {}
func (*StatePoolBroadcastFinished[K, N]) poolState() {}
func (*StatePoolIdle) poolState()                    {}

// PoolEvent is an event intended to advance the state of the broadcast [Pool]
// state machine. The [Pool] state machine only operates on events that
// implement this interface. An "Event" is the opposite of a "State." An "Event"
// flows into the state machine and a "State" flows out of it.
type PoolEvent interface {
	poolEvent()
}

// EventPoolPoll is an event that signals the broadcast [Pool] state machine
// that it can perform housekeeping work such as time out queries.
type EventPoolPoll struct{}

// EventPoolStartBroadcast is an event that attempts to start a new broadcast
// operation. This is the entry point.
type EventPoolStartBroadcast[K kad.Key[K], N kad.NodeID[K], M coordt.Message] struct {
	QueryID coordt.QueryID // the unique ID for this operation
	Target  K              // the key we want to store the record for
	Message M              // the message that we want to send to the closest peers (this encapsulates the payload we want to store)
	Seed    []N            // the closest nodes we know so far and from where we start the operation
	Config  Config         // the configuration for this operation. Most importantly, this defines the broadcast strategy ([FollowUp] or [Optimistic])
}

// EventPoolStopBroadcast notifies broadcast [Pool] to stop a broadcast
// operation.
type EventPoolStopBroadcast struct {
	QueryID coordt.QueryID // the id of the broadcast operation that should be stopped
}

// EventPoolGetCloserNodesSuccess notifies a [Pool] that a remote node (NodeID)
// has successfully responded with closer nodes (CloserNodes) to the Target key
// for the broadcast operation with the given id (QueryID).
type EventPoolGetCloserNodesSuccess[K kad.Key[K], N kad.NodeID[K]] struct {
	QueryID     coordt.QueryID // the id of the broadcast operation that this response belongs to
	NodeID      N              // the node the message was sent to and that replied
	Target      K              // the key we want are searching closer nodes for
	CloserNodes []N            // the closer nodes sent by the node NodeID
}

// EventPoolGetCloserNodesFailure notifies a [Pool] that a remote node (NodeID)
// has failed responding with closer nodes to the Target key for the broadcast
// operation with the given id (QueryID).
type EventPoolGetCloserNodesFailure[K kad.Key[K], N kad.NodeID[K]] struct {
	QueryID coordt.QueryID // the id of the query that sent the message
	NodeID  N              // the node the message was sent to and that has replied
	Target  K              // the key we want are searching closer nodes for
	Error   error          // the error that caused the failure, if any
}

// EventPoolStoreRecordSuccess noties the broadcast [Pool] that storing a record
// with a remote node (NodeID) was successful. The message that was sent is held
// in Request, and the returned value is contained in Response. However, in the
// case of the Amino DHT, nodes do not respond with a confirmation, so Response
// will always be nil. Check out [pb.Message.ExpectResponse] for information
// about which requests should receive a response.
type EventPoolStoreRecordSuccess[K kad.Key[K], N kad.NodeID[K], M coordt.Message] struct {
	QueryID  coordt.QueryID // the id of the query that sent the message
	NodeID   N              // the node the message was sent to
	Request  M              // the message that was sent to the remote node
	Response M              // the reply we got from the remote node (nil in many cases of the Amino DHT)
}

// EventPoolStoreRecordFailure noties the broadcast [Pool] that storing a record
// with a remote node (NodeID) has failed. The message that was sent is hold
// in Request, and the error will be in Error.
type EventPoolStoreRecordFailure[K kad.Key[K], N kad.NodeID[K], M coordt.Message] struct {
	QueryID coordt.QueryID // the id of the query that sent the message
	NodeID  N              // the node the message was sent to
	Request M              // the message that was sent to the remote node
	Error   error          // the error that caused the failure
}

// poolEvent() ensures that only events accepted by a broadcast [Pool] can be
// assigned to the [PoolEvent] interface.
func (*EventPoolStopBroadcast) poolEvent()               {}
func (*EventPoolPoll) poolEvent()                        {}
func (*EventPoolStartBroadcast[K, N, M]) poolEvent()     {}
func (*EventPoolGetCloserNodesSuccess[K, N]) poolEvent() {}
func (*EventPoolGetCloserNodesFailure[K, N]) poolEvent() {}
func (*EventPoolStoreRecordSuccess[K, N, M]) poolEvent() {}
func (*EventPoolStoreRecordFailure[K, N, M]) poolEvent() {}
