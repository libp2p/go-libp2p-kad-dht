package routing

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/plprobelab/go-libdht/kad"
	"github.com/plprobelab/go-libdht/kad/key"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/libp2p/go-libp2p-kad-dht/v2/errs"
	"github.com/libp2p/go-libp2p-kad-dht/v2/tele"
)

type check[K kad.Key[K], N kad.NodeID[K]] struct {
	NodeID  N
	Started time.Time
}

type Include[K kad.Key[K], N kad.NodeID[K]] struct {
	rt kad.RoutingTable[K, N]

	// checks is an index of checks in progress
	checks map[string]check[K, N]

	// candidates is a list of nodes that are candidates for adding to the routing table
	candidates *nodeQueue[K, N]

	// cfg is a copy of the optional configuration supplied to the Include
	cfg IncludeConfig

	// counterChecksSent is a counter that tracks the number of connectivity checks sent.
	counterChecksSent metric.Int64Counter

	// counterChecksPassed is a counter that tracks the number of connectivity checks that have passed.
	counterChecksPassed metric.Int64Counter

	// counterChecksFailed is a counter that tracks the number of connectivity checks that have failed.
	counterChecksFailed metric.Int64Counter

	// counterCandidatesDroppedCapacity is a counter that tracks the number of nodes that were not added to the candidate
	// queue because it was already at maximum capacity. If this rises or remains high then it could indicate that
	// the include state machine cannot keep up with the rate of new nodes being added. This could be affected by
	// the configured maximum number of concurrent checks and the timeout used for terminating slow checks.
	counterCandidatesDroppedCapacity metric.Int64Counter

	// gaugeCandidateCount is a gauge that tracks the number of nodes in the probe's pending queue of scheduled checks.
	gaugeCandidateCount metric.Int64ObservableGauge

	// candidateCount holds the number of candidate nodes after the last state change so that it can be read asynchronously by gaugeCandidateCount
	candidateCount atomic.Int64
}

// IncludeConfig specifies optional configuration for an Include
type IncludeConfig struct {
	QueueCapacity int           // the maximum number of nodes that can be in the candidate queue
	Concurrency   int           // the maximum number of include checks that may be in progress at any one time
	Timeout       time.Duration // the time to wait before terminating a check that is not making progress
	Clock         clock.Clock   // a clock that may replaced by a mock when testing

	// Tracer is the tracer that should be used to trace execution.
	Tracer trace.Tracer

	// Meter is the meter that should be used to record metrics.
	Meter metric.Meter
}

// Validate checks the configuration options and returns an error if any have invalid values.
func (cfg *IncludeConfig) Validate() error {
	if cfg.Clock == nil {
		return &errs.ConfigurationError{
			Component: "IncludeConfig",
			Err:       fmt.Errorf("clock must not be nil"),
		}
	}

	if cfg.Concurrency < 1 {
		return &errs.ConfigurationError{
			Component: "IncludeConfig",
			Err:       fmt.Errorf("concurrency must be greater than zero"),
		}
	}

	if cfg.Timeout < 1 {
		return &errs.ConfigurationError{
			Component: "IncludeConfig",
			Err:       fmt.Errorf("timeout must be greater than zero"),
		}
	}

	if cfg.QueueCapacity < 1 {
		return &errs.ConfigurationError{
			Component: "IncludeConfig",
			Err:       fmt.Errorf("queue size must be greater than zero"),
		}
	}

	if cfg.Tracer == nil {
		return &errs.ConfigurationError{
			Component: "IncludeConfig",
			Err:       fmt.Errorf("tracer must not be nil"),
		}
	}

	if cfg.Meter == nil {
		return &errs.ConfigurationError{
			Component: "IncludeConfig",
			Err:       fmt.Errorf("meter must not be nil"),
		}
	}

	return nil
}

// DefaultIncludeConfig returns the default configuration options for an Include.
// Options may be overridden before passing to NewInclude
func DefaultIncludeConfig() *IncludeConfig {
	return &IncludeConfig{
		Clock:  clock.New(), // use standard time
		Tracer: tele.NoopTracer(),
		Meter:  tele.NoopMeter(),

		Concurrency:   3,
		Timeout:       time.Minute,
		QueueCapacity: 128,
	}
}

func NewInclude[K kad.Key[K], N kad.NodeID[K]](rt kad.RoutingTable[K, N], cfg *IncludeConfig) (*Include[K, N], error) {
	if cfg == nil {
		cfg = DefaultIncludeConfig()
	} else if err := cfg.Validate(); err != nil {
		return nil, err
	}

	in := &Include[K, N]{
		candidates: newNodeQueue[K, N](cfg.QueueCapacity),
		cfg:        *cfg,
		rt:         rt,
		checks:     make(map[string]check[K, N], cfg.Concurrency),
	}

	// initialise metrics
	var err error
	in.counterChecksSent, err = cfg.Meter.Int64Counter(
		"include_checks_sent",
		metric.WithDescription("Total number of connectivity checks sent by the include state machine"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return nil, fmt.Errorf("create include_checks_sent counter: %w", err)
	}

	in.counterChecksPassed, err = cfg.Meter.Int64Counter(
		"include_checks_passed",
		metric.WithDescription("Total number of connectivity checks sent by the include state machine that were successful"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return nil, fmt.Errorf("create include_checks_passed counter: %w", err)
	}

	in.counterChecksFailed, err = cfg.Meter.Int64Counter(
		"include_checks_failed",
		metric.WithDescription("Total number of connectivity checks sent by the include state machine that failed"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return nil, fmt.Errorf("create include_checks_failed counter: %w", err)
	}

	in.counterCandidatesDroppedCapacity, err = cfg.Meter.Int64Counter(
		"include_candidates_dropped_capacity",
		metric.WithDescription("Total number of nodes that were not added to the candidate queue because it was already at maximum capacity"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return nil, fmt.Errorf("create include_candidates_dropped_capacity counter: %w", err)
	}

	in.gaugeCandidateCount, err = cfg.Meter.Int64ObservableGauge(
		"include_candidate_count",
		metric.WithDescription("Total number of nodes in the include state machine's candidate queue"),
		metric.WithUnit("1"),
		metric.WithInt64Callback(func(ctx context.Context, o metric.Int64Observer) error {
			o.Observe(in.candidateCount.Load())
			return nil
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("create include_candidate_count counter: %w", err)
	}

	return in, nil
}

// Advance advances the state of the include state machine by attempting to advance its query if running.
func (in *Include[K, N]) Advance(ctx context.Context, ev IncludeEvent) (out IncludeState) {
	ctx, span := in.cfg.Tracer.Start(ctx, "Include.Advance", trace.WithAttributes(tele.AttrInEvent(ev)))
	defer func() {
		in.candidateCount.Store(int64(in.candidates.Len()))
		span.SetAttributes(tele.AttrOutEvent(out))
		span.End()
	}()

	switch tev := ev.(type) {
	case *EventIncludeAddCandidate[K, N]:
		// Ignore if already running a check
		_, checking := in.checks[key.HexString(tev.NodeID.Key())]
		if checking {
			break
		}

		// Ignore if node already in routing table
		if _, exists := in.rt.GetNode(tev.NodeID.Key()); exists {
			break
		}

		// TODO: potentially time out a check and make room in the queue
		if !in.candidates.HasCapacity() {
			in.counterCandidatesDroppedCapacity.Add(ctx, 1)
			return &StateIncludeWaitingFull{}
		}
		in.candidates.Enqueue(ctx, tev.NodeID)

	case *EventIncludeConnectivityCheckSuccess[K, N]:
		in.counterChecksPassed.Add(ctx, 1)
		ch, ok := in.checks[key.HexString(tev.NodeID.Key())]
		if ok {
			delete(in.checks, key.HexString(tev.NodeID.Key()))
			if in.rt.AddNode(tev.NodeID) {
				return &StateIncludeRoutingUpdated[K, N]{
					NodeID: ch.NodeID,
				}
			}
		}
	case *EventIncludeConnectivityCheckFailure[K, N]:
		in.counterChecksFailed.Add(ctx, 1)
		span.RecordError(tev.Error)
		delete(in.checks, key.HexString(tev.NodeID.Key()))

	case *EventIncludePoll:
		// ignore, nothing to do
	default:
		panic(fmt.Sprintf("unexpected event: %T", tev))
	}

	if len(in.checks) == in.cfg.Concurrency {
		if !in.candidates.HasCapacity() {
			in.counterCandidatesDroppedCapacity.Add(ctx, 1)
			return &StateIncludeWaitingFull{}
		}
		return &StateIncludeWaitingAtCapacity{}
	}

	candidate, ok := in.candidates.Dequeue(ctx)
	if !ok {
		// No candidate in queue
		if len(in.checks) > 0 {
			return &StateIncludeWaitingWithCapacity{}
		}
		return &StateIncludeIdle{}
	}

	in.checks[key.HexString(candidate.Key())] = check[K, N]{
		NodeID:  candidate,
		Started: in.cfg.Clock.Now(),
	}

	// Ask the node to find itself
	in.counterChecksSent.Add(ctx, 1)
	return &StateIncludeConnectivityCheck[K, N]{
		NodeID: candidate,
	}
}

// nodeQueue is a bounded queue of unique NodeIDs
type nodeQueue[K kad.Key[K], N kad.NodeID[K]] struct {
	capacity int
	nodes    []N
	keys     map[string]struct{}
}

func newNodeQueue[K kad.Key[K], N kad.NodeID[K]](capacity int) *nodeQueue[K, N] {
	return &nodeQueue[K, N]{
		capacity: capacity,
		nodes:    make([]N, 0, capacity),
		keys:     make(map[string]struct{}, capacity),
	}
}

// Enqueue adds a node to the queue. It returns true if the node was
// added and false otherwise.
func (q *nodeQueue[K, N]) Enqueue(ctx context.Context, id N) bool {
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
func (q *nodeQueue[K, N]) Dequeue(ctx context.Context) (N, bool) {
	if len(q.nodes) == 0 {
		var v N
		return v, false
	}

	var id N
	id, q.nodes = q.nodes[0], q.nodes[1:]
	delete(q.keys, key.HexString(id.Key()))

	return id, true
}

func (q *nodeQueue[K, N]) HasCapacity() bool {
	return len(q.nodes) < q.capacity
}

func (q *nodeQueue[K, N]) Len() int {
	return len(q.nodes)
}

// IncludeState is the state of a include.
type IncludeState interface {
	includeState()
}

// StateIncludeConnectivityCheck indicates that an [Include] is waiting to send a connectivity check to a node.
// A find node message should be sent to the node, with the target being the node's key.
type StateIncludeConnectivityCheck[K kad.Key[K], N kad.NodeID[K]] struct {
	NodeID N // the node to send the message to
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
type StateIncludeRoutingUpdated[K kad.Key[K], N kad.NodeID[K]] struct {
	NodeID N
}

// includeState() ensures that only Include states can be assigned to an IncludeState.
func (*StateIncludeConnectivityCheck[K, N]) includeState() {}
func (*StateIncludeIdle) includeState()                    {}
func (*StateIncludeWaitingAtCapacity) includeState()       {}
func (*StateIncludeWaitingWithCapacity) includeState()     {}
func (*StateIncludeWaitingFull) includeState()             {}
func (*StateIncludeRoutingUpdated[K, N]) includeState()    {}

// IncludeEvent is an event intended to advance the state of an [Include].
type IncludeEvent interface {
	includeEvent()
}

// EventIncludePoll is an event that signals an [Include] to perform housekeeping work such as time out queries.
type EventIncludePoll struct{}

// EventIncludeAddCandidate notifies an [Include] that a node should be added to the candidate list.
type EventIncludeAddCandidate[K kad.Key[K], N kad.NodeID[K]] struct {
	NodeID N // the candidate node
}

// EventIncludeConnectivityCheckSuccess notifies an [Include] that a requested connectivity check has received a successful response.
type EventIncludeConnectivityCheckSuccess[K kad.Key[K], N kad.NodeID[K]] struct {
	NodeID N // the node the message was sent to
}

// EventIncludeConnectivityCheckFailure notifies an [Include] that a requested connectivity check has failed.
type EventIncludeConnectivityCheckFailure[K kad.Key[K], N kad.NodeID[K]] struct {
	NodeID N     // the node the message was sent to
	Error  error // the error that caused the failure, if any
}

// includeEvent() ensures that only events accepted by an [Include] can be assigned to the [IncludeEvent] interface.
func (*EventIncludePoll) includeEvent()                           {}
func (*EventIncludeAddCandidate[K, N]) includeEvent()             {}
func (*EventIncludeConnectivityCheckSuccess[K, N]) includeEvent() {}
func (*EventIncludeConnectivityCheckFailure[K, N]) includeEvent() {}
