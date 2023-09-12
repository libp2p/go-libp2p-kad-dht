package routing

import (
	"context"
	"fmt"
	"time"

	"github.com/benbjohnson/clock"

	"github.com/plprobelab/go-kademlia/kad"
	"github.com/plprobelab/go-kademlia/kaderr"
	"github.com/plprobelab/go-kademlia/key"

	"github.com/libp2p/go-libp2p-kad-dht/v2/tele"
)

type check[K kad.Key[K]] struct {
	NodeID  kad.NodeID[K]
	Started time.Time
}

type Include[K kad.Key[K]] struct {
	rt kad.RoutingTable[K, kad.NodeID[K]]

	// checks is an index of checks in progress
	checks map[string]check[K]

	candidates *nodeQueue[K]

	// cfg is a copy of the optional configuration supplied to the Include
	cfg IncludeConfig
}

// IncludeConfig specifies optional configuration for an Include
type IncludeConfig struct {
	QueueCapacity int           // the maximum number of nodes that can be in the candidate queue
	Concurrency   int           // the maximum number of include checks that may be in progress at any one time
	Timeout       time.Duration // the time to wait before terminating a check that is not making progress
	Clock         clock.Clock   // a clock that may replaced by a mock when testing
}

// Validate checks the configuration options and returns an error if any have invalid values.
func (cfg *IncludeConfig) Validate() error {
	if cfg.Clock == nil {
		return &kaderr.ConfigurationError{
			Component: "IncludeConfig",
			Err:       fmt.Errorf("clock must not be nil"),
		}
	}

	if cfg.Concurrency < 1 {
		return &kaderr.ConfigurationError{
			Component: "IncludeConfig",
			Err:       fmt.Errorf("concurrency must be greater than zero"),
		}
	}

	if cfg.Timeout < 1 {
		return &kaderr.ConfigurationError{
			Component: "IncludeConfig",
			Err:       fmt.Errorf("timeout must be greater than zero"),
		}
	}

	if cfg.QueueCapacity < 1 {
		return &kaderr.ConfigurationError{
			Component: "IncludeConfig",
			Err:       fmt.Errorf("queue size must be greater than zero"),
		}
	}

	return nil
}

// DefaultIncludeConfig returns the default configuration options for an Include.
// Options may be overridden before passing to NewInclude
func DefaultIncludeConfig() *IncludeConfig {
	return &IncludeConfig{
		Clock:         clock.New(), // use standard time
		Concurrency:   3,
		Timeout:       time.Minute,
		QueueCapacity: 128,
	}
}

func NewInclude[K kad.Key[K]](rt kad.RoutingTable[K, kad.NodeID[K]], cfg *IncludeConfig) (*Include[K], error) {
	if cfg == nil {
		cfg = DefaultIncludeConfig()
	} else if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return &Include[K]{
		candidates: newNodeQueue[K](cfg.QueueCapacity),
		cfg:        *cfg,
		rt:         rt,
		checks:     make(map[string]check[K], cfg.Concurrency),
	}, nil
}

// Advance advances the state of the include state machine by attempting to advance its query if running.
func (b *Include[K]) Advance(ctx context.Context, ev IncludeEvent) IncludeState {
	ctx, span := tele.StartSpan(ctx, "Include.Advance")
	defer span.End()

	switch tev := ev.(type) {

	case *EventIncludeAddCandidate[K]:
		// Ignore if already running a check
		_, checking := b.checks[key.HexString(tev.NodeID.Key())]
		if checking {
			break
		}

		// Ignore if node already in routing table
		if _, exists := b.rt.GetNode(tev.NodeID.Key()); exists {
			break
		}

		// TODO: potentially time out a check and make room in the queue
		if !b.candidates.HasCapacity() {
			return &StateIncludeWaitingFull{}
		}
		b.candidates.Enqueue(ctx, tev.NodeID)

	case *EventIncludeConnectivityCheckSuccess[K]:
		ch, ok := b.checks[key.HexString(tev.NodeID.Key())]
		if ok {
			delete(b.checks, key.HexString(tev.NodeID.Key()))
			if b.rt.AddNode(tev.NodeID) {
				return &StateIncludeRoutingUpdated[K]{
					NodeID: ch.NodeID,
				}
			}
		}
	case *EventIncludeConnectivityCheckFailure[K]:
		delete(b.checks, key.HexString(tev.NodeID.Key()))

	case *EventIncludePoll:
	// ignore, nothing to do
	default:
		panic(fmt.Sprintf("unexpected event: %T", tev))
	}

	if len(b.checks) == b.cfg.Concurrency {
		if !b.candidates.HasCapacity() {
			return &StateIncludeWaitingFull{}
		}
		return &StateIncludeWaitingAtCapacity{}
	}

	candidate, ok := b.candidates.Dequeue(ctx)
	if !ok {
		// No candidate in queue
		if len(b.checks) > 0 {
			return &StateIncludeWaitingWithCapacity{}
		}
		return &StateIncludeIdle{}
	}

	b.checks[key.HexString(candidate.Key())] = check[K]{
		NodeID:  candidate,
		Started: b.cfg.Clock.Now(),
	}

	// Ask the node to find itself
	return &StateIncludeConnectivityCheck[K]{
		NodeID: candidate,
	}
}

// nodeQueue is a bounded queue of unique NodeIDs
type nodeQueue[K kad.Key[K]] struct {
	capacity int
	nodes    []kad.NodeID[K]
	keys     map[string]struct{}
}

func newNodeQueue[K kad.Key[K]](capacity int) *nodeQueue[K] {
	return &nodeQueue[K]{
		capacity: capacity,
		nodes:    make([]kad.NodeID[K], 0, capacity),
		keys:     make(map[string]struct{}, capacity),
	}
}

// Enqueue adds a node to the queue. It returns true if the node was
// added and false otherwise.
func (q *nodeQueue[K]) Enqueue(ctx context.Context, id kad.NodeID[K]) bool {
	if len(q.nodes) == q.capacity {
		return false
	}

	if _, exists := q.keys[key.HexString(id.Key())]; exists {
		return false
	}

	q.nodes = append(q.nodes, id)
	q.keys[key.HexString(id.Key())] = struct{}{}
	return true
}

// Dequeue reads an node from the queue. It returns the node and a true value
// if a node was read or nil and false if no node was read.
func (q *nodeQueue[K]) Dequeue(ctx context.Context) (kad.NodeID[K], bool) {
	if len(q.nodes) == 0 {
		var v kad.NodeID[K]
		return v, false
	}

	var id kad.NodeID[K]
	id, q.nodes = q.nodes[0], q.nodes[1:]
	delete(q.keys, key.HexString(id.Key()))

	return id, true
}

func (q *nodeQueue[K]) HasCapacity() bool {
	return len(q.nodes) < q.capacity
}

// IncludeState is the state of a include.
type IncludeState interface {
	includeState()
}

// StateIncludeConnectivityCheck indicates that an [Include] is waiting to send a connectivity check to a node.
// A find node message should be sent to the node, with the target being the node's key.
type StateIncludeConnectivityCheck[K kad.Key[K]] struct {
	NodeID kad.NodeID[K] // the node to send the message to
}

// StateIncludeIdle indicates that an [Include] is not peforming any work or waiting for any responses..
type StateIncludeIdle struct{}

// StateIncludeWaitingAtCapacity indicates that an [Include] is waiting for responses for checks and
// that the maximum number of concurrent checks has been reached.
type StateIncludeWaitingAtCapacity struct{}

// StateIncludeWaitingWithCapacity indicates that an [Include] is waiting for responses for checks
// but has capacity to perform more.
type StateIncludeWaitingWithCapacity struct{}

// StateIncludeWaitingFull indicates that the include subsystem is waiting for responses for checks and
// that the maximum number of queued candidates has been reached.
type StateIncludeWaitingFull struct{}

// StateIncludeRoutingUpdated indicates the routing table has been updated with a new node.
type StateIncludeRoutingUpdated[K kad.Key[K]] struct {
	NodeID kad.NodeID[K]
}

// includeState() ensures that only Include states can be assigned to an IncludeState.
func (*StateIncludeConnectivityCheck[K]) includeState() {}
func (*StateIncludeIdle) includeState()                 {}
func (*StateIncludeWaitingAtCapacity) includeState()    {}
func (*StateIncludeWaitingWithCapacity) includeState()  {}
func (*StateIncludeWaitingFull) includeState()          {}
func (*StateIncludeRoutingUpdated[K]) includeState()    {}

// IncludeEvent is an event intended to advance the state of an [Include].
type IncludeEvent interface {
	includeEvent()
}

// EventIncludePoll is an event that signals an [Include] to perform housekeeping work such as time out queries.
type EventIncludePoll struct{}

// EventIncludeAddCandidate notifies an [Include] that a node should be added to the candidate list.
type EventIncludeAddCandidate[K kad.Key[K]] struct {
	NodeID kad.NodeID[K] // the candidate node
}

// EventIncludeConnectivityCheckSuccess notifies an [Include] that a requested connectivity check has received a successful response.
type EventIncludeConnectivityCheckSuccess[K kad.Key[K]] struct {
	NodeID kad.NodeID[K] // the node the message was sent to
}

// EventIncludeConnectivityCheckFailure notifies an [Include] that a requested connectivity check has failed.
type EventIncludeConnectivityCheckFailure[K kad.Key[K]] struct {
	NodeID kad.NodeID[K] // the node the message was sent to
	Error  error         // the error that caused the failure, if any
}

// includeEvent() ensures that only events accepted by an [Include] can be assigned to the [IncludeEvent] interface.
func (*EventIncludePoll) includeEvent()                        {}
func (*EventIncludeAddCandidate[K]) includeEvent()             {}
func (*EventIncludeConnectivityCheckSuccess[K]) includeEvent() {}
func (*EventIncludeConnectivityCheckFailure[K]) includeEvent() {}
