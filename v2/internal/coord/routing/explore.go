package routing

import (
	"container/heap"
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/plprobelab/go-kademlia/kad"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/libp2p/go-libp2p-kad-dht/v2/internal/coord/query"
	"github.com/libp2p/go-libp2p-kad-dht/v2/tele"
)

const ExploreQueryID = query.QueryID("explore")

// The Explore state machine is used to discover new nodes at various distances from the local node in order to improve
// the occupancy of routing table buckets.
//
// For each bucket a random key is generated that would occupy the bucket and a query initiated to find the nodes close
// to it. Dicovered nodes are added to the candidate queue for inclusion in the routing table. During the course of the
// query discovered nodes may result in being included in buckets other than the one being processed.
//
// The explore operation processes buckets in order of distance from the local node and waits for query completion
// before proceeding to the next bucket.
//
// The frequency of running an explore varies by bucket distance, such that closer buckets are processed more frequently.
type Explore[K kad.Key[K], N kad.NodeID[K]] struct {
	// self is the node id of the system the explore is running on
	self N

	// rt is the local routing table
	rt RoutingTableCpl[K, N]

	cplFn NodeIDForCplFunc[K, N]

	// qry is the query used by the explore process
	qry *query.Query[K, N, any]

	// qryCpl is the cpl the current query is exploring for
	qryCpl int

	// cfg is a copy of the optional configuration supplied to the Explore
	cfg ExploreConfig

	// schedule is a list of cpls ordered by the time the next explore is due
	schedule *exploreList
}

// NodeIDForCplFunc is a function that given a cpl generates a [kad.NodeID] with a key that has
// a common prefix length with k of length cpl.
// Invariant: CommonPrefixLength(k, node.Key()) = cpl
type NodeIDForCplFunc[K kad.Key[K], N kad.NodeID[K]] func(k K, cpl int) (N, error)

// ExploreConfig specifies optional configuration for an [Explore]
type ExploreConfig struct {
	// Clock is  a clock that may replaced by a mock when testing
	Clock clock.Clock

	// MaximumCpl is the maximum CPL (common prefix length) that will be explored. This is roughly
	// equivalent to a Kademlia bucket.
	MaximumCpl int

	// Interval is the minimum time interval between exploring each CPL.
	Interval time.Duration

	// Timeout is maximum time to allow for performing an explore for a CPL.
	Timeout time.Duration

	// IntervalMultiplier is a factor that is applied to Interval for CPLs lower than the maximum
	// to determine the time of the next explore operation. The interval to the next explore is calculated
	// using the following formula: Interval x (MaximumCpl - CPL) x IntervalMultiplier x (1 + rand(IntervalJitter))
	// IntervalMultiplier must be 1 or greater.
	IntervalMultiplier float64

	// IntervalJitter is a factor that is used to increase the calculated interval for the next explore
	// operation by a small random amount. It must be between 0 and 0.05. When zero, no jitter is applied.
	IntervalJitter float64
}

// Validate checks the configuration options and returns an error if any have invalid values.
func (cfg *ExploreConfig) Validate() error {
	if cfg.Clock == nil {
		return fmt.Errorf("clock must not be nil")
	}

	if cfg.MaximumCpl < 1 {
		return fmt.Errorf("maximum cpl must be greater than zero")
	}

	if cfg.Interval < 1 {
		return fmt.Errorf("interval must be greater than zero")
	}

	if cfg.Timeout < 1 {
		return fmt.Errorf("timeout must be greater than zero")
	}

	if cfg.IntervalMultiplier < 1 {
		return fmt.Errorf("interval multiplier must be greater than or equal to one")
	}

	if cfg.IntervalJitter < 0 {
		return fmt.Errorf("interval jitter must not be negative")
	}

	if cfg.IntervalJitter > 0.05 {
		return fmt.Errorf("interval jitter must not be greater than 0.05")
	}

	return nil
}

// DefaultExploreConfig returns the default configuration options for an [Explore].
// Options may be overridden before passing to [NewExplore].
func DefaultExploreConfig() *ExploreConfig {
	return &ExploreConfig{
		Clock:              clock.New(), // use standard time
		MaximumCpl:         14,
		Interval:           time.Hour,        // TODO: review default
		Timeout:            10 * time.Minute, // MAGIC
		IntervalMultiplier: 1,
		IntervalJitter:     0, // no jitter by default
	}
}

func NewExplore[K kad.Key[K], N kad.NodeID[K]](self N, rt RoutingTableCpl[K, N], cplFn NodeIDForCplFunc[K, N], cfg *ExploreConfig) (*Explore[K, N], error) {
	if cfg == nil {
		cfg = DefaultExploreConfig()
	} else if err := cfg.Validate(); err != nil {
		return nil, err
	}

	e := &Explore[K, N]{
		self:     self,
		cplFn:    cplFn,
		rt:       rt,
		cfg:      *cfg,
		qryCpl:   -1,
		schedule: new(exploreList),
	}

	// build the initial schedule
	for cpl := cfg.MaximumCpl; cpl >= 0; cpl-- {
		*e.schedule = append(*e.schedule, &exploreEntry{
			Cpl: cpl,
			Due: e.cfg.Clock.Now().Add(e.interval(cpl)),
		})
	}
	heap.Init(e.schedule)

	return e, nil
}

// interval calculates the explore interval for a given cpl
func (e *Explore[K, N]) interval(cpl int) time.Duration {
	interval := float64(e.cfg.Interval) + float64(e.cfg.Interval)*float64(e.cfg.MaximumCpl-cpl)*e.cfg.IntervalMultiplier
	interval *= 1 + rand.Float64()*e.cfg.IntervalJitter
	return time.Duration(interval)
}

// Advance advances the state of the explore by attempting to advance its query if running.
func (e *Explore[K, N]) Advance(ctx context.Context, ev ExploreEvent) ExploreState {
	ctx, span := tele.StartSpan(ctx, "Explore.Advance", trace.WithAttributes(tele.AttrInEvent(ev)))
	defer span.End()

	switch tev := ev.(type) {
	case *EventExplorePoll:
		// ignore, nothing to do
	case *EventExploreFindCloserResponse[K, N]:
		return e.advanceQuery(ctx, &query.EventQueryNodeResponse[K, N]{
			NodeID:      tev.NodeID,
			CloserNodes: tev.CloserNodes,
		})
	case *EventExploreFindCloserFailure[K, N]:
		span.RecordError(tev.Error)
		return e.advanceQuery(ctx, &query.EventQueryNodeFailure[K, N]{
			NodeID: tev.NodeID,
			Error:  tev.Error,
		})
	default:
		panic(fmt.Sprintf("unexpected event: %T", tev))
	}

	if len(*e.schedule) == 0 {
		return &StateExploreIdle{}
	}

	// TODO: check if a query is running

	if e.qry != nil {
		return e.advanceQuery(ctx, &query.EventQueryPoll{})
	}

	// is an explore due yet?
	next := (*e.schedule)[0]
	if e.cfg.Clock.Now().After(next.Due) {
		// start an explore query
		node, err := e.cplFn(e.self.Key(), next.Cpl)
		if err != nil {
			// TODO: don't panic
			panic(err)
		}
		seeds := e.rt.NearestNodes(node.Key(), 20)

		iter := query.NewClosestNodesIter[K, N](e.self.Key())

		qryCfg := query.DefaultQueryConfig()
		qryCfg.Clock = e.cfg.Clock
		// qryCfg.Concurrency = b.cfg.RequestConcurrency
		// qryCfg.RequestTimeout = b.cfg.RequestTimeout

		qry, err := query.NewFindCloserQuery[K, N, any](e.self, ExploreQueryID, e.self.Key(), iter, seeds, qryCfg)
		if err != nil {
			// TODO: don't panic
			panic(err)
		}
		e.qry = qry
		e.qryCpl = next.Cpl

		// reschedule the explore for next time
		next.Due = e.cfg.Clock.Now().Add(e.interval(next.Cpl))
		heap.Fix(e.schedule, 0) // update the heap

		return e.advanceQuery(ctx, &query.EventQueryPoll{})
	}

	return &StateExploreIdle{}
}

func (e *Explore[K, N]) advanceQuery(ctx context.Context, qev query.QueryEvent) ExploreState {
	ctx, span := tele.StartSpan(ctx, "Explore.advanceQuery")
	defer span.End()
	state := e.qry.Advance(ctx, qev)
	switch st := state.(type) {
	case *query.StateQueryFindCloser[K, N]:
		return &StateExploreFindCloser[K, N]{
			Cpl:     e.qryCpl,
			QueryID: st.QueryID,
			Stats:   st.Stats,
			NodeID:  st.NodeID,
			Target:  st.Target,
		}
	case *query.StateQueryFinished[K, N]:
		span.SetAttributes(attribute.String("out_state", "StateExploreFinished"))
		e.qry = nil
		e.qryCpl = -1
		return &StateExploreQueryFinished{
			Cpl:   e.qryCpl,
			Stats: st.Stats,
		}
	case *query.StateQueryWaitingAtCapacity:
		elapsed := e.cfg.Clock.Since(st.Stats.Start)
		if elapsed > e.cfg.Timeout {
			span.SetAttributes(attribute.String("out_state", "StateExploreTimeout"))
			e.qry = nil
			e.qryCpl = -1
			return &StateExploreQueryTimeout{
				Cpl:   e.qryCpl,
				Stats: st.Stats,
			}
		}
		span.SetAttributes(attribute.String("out_state", "StateExploreWaiting"))
		return &StateExploreWaiting{
			Cpl:   e.qryCpl,
			Stats: st.Stats,
		}
	case *query.StateQueryWaitingWithCapacity:
		elapsed := e.cfg.Clock.Since(st.Stats.Start)
		if elapsed > e.cfg.Timeout {
			span.SetAttributes(attribute.String("out_state", "StateExploreTimeout"))
			e.qry = nil
			e.qryCpl = -1
			return &StateExploreQueryTimeout{
				Cpl:   e.qryCpl,
				Stats: st.Stats,
			}
		}
		span.SetAttributes(attribute.String("out_state", "StateExploreWaiting"))
		return &StateExploreWaiting{
			Cpl:   e.qryCpl,
			Stats: st.Stats,
		}
	default:
		panic(fmt.Sprintf("unexpected state: %T", st))
	}
}

// ExploreState is the state of an [Explore].
type ExploreState interface {
	exploreState()
}

// StateExploreIdle indicates that the explore is not running its query.
type StateExploreIdle struct{}

// StateExploreFindCloser indicates that the explore query wants to send a find closer nodes message to a node.
type StateExploreFindCloser[K kad.Key[K], N kad.NodeID[K]] struct {
	Cpl     int // the cpl being explored
	QueryID query.QueryID
	Target  K // the key that the query wants to find closer nodes for
	NodeID  N // the node to send the message to
	Stats   query.QueryStats
}

// StateExploreWaiting indicates that the explore query is waiting for a response.
type StateExploreWaiting struct {
	Cpl   int // the cpl being explored
	Stats query.QueryStats
}

// StateExploreQueryFinished indicates that an explore query has finished.
type StateExploreQueryFinished struct {
	Cpl   int // the cpl being explored
	Stats query.QueryStats
}

// StateExploreQueryTimeout indicates that an explore query has timed out.
type StateExploreQueryTimeout struct {
	Cpl   int // the cpl being explored
	Stats query.QueryStats
}

// exploreState() ensures that only [Explore] states can be assigned to an [ExploreState].
func (*StateExploreIdle) exploreState()             {}
func (*StateExploreFindCloser[K, N]) exploreState() {}
func (*StateExploreWaiting) exploreState()          {}
func (*StateExploreQueryFinished) exploreState()    {}
func (*StateExploreQueryTimeout) exploreState()     {}

// ExploreEvent is an event intended to advance the state of an [Explore].
type ExploreEvent interface {
	exploreEvent()
}

// EventExplorePoll is an event that signals the explore that it can perform housekeeping work such as time out queries.
type EventExplorePoll struct{}

// EventExploreFindCloserResponse notifies a explore that an attempt to find closer nodes has received a successful response.
type EventExploreFindCloserResponse[K kad.Key[K], N kad.NodeID[K]] struct {
	NodeID      N   // the node the message was sent to
	CloserNodes []N // the closer nodes sent by the node
}

// EventExploreFindCloserFailure notifies a explore that an attempt to find closer nodes has failed.
type EventExploreFindCloserFailure[K kad.Key[K], N kad.NodeID[K]] struct {
	NodeID N     // the node the message was sent to
	Error  error // the error that caused the failure, if any
}

// exploreState() ensures that only [Explore] events can be assigned to an [ExploreEvent].
func (*EventExplorePoll) exploreEvent()                     {}
func (*EventExploreFindCloserResponse[K, N]) exploreEvent() {}
func (*EventExploreFindCloserFailure[K, N]) exploreEvent()  {}

type exploreEntry struct {
	Cpl int       // the longest common prefix length shared with the routing table's key
	Due time.Time // the time at which the next explore operation for this cpl is due
}

// exploreList is a min-heap of exploreEntry ordered by Due
type exploreList []*exploreEntry

func (l exploreList) Len() int { return len(l) }

func (l exploreList) Less(i, j int) bool {
	// if due times are equal, then sort lower cpls first
	if l[i].Due.Equal(l[j].Due) {
		return l[i].Cpl < l[j].Cpl
	}

	return l[i].Due.Before(l[j].Due)
}

func (l exploreList) Swap(i, j int) {
	l[i], l[j] = l[j], l[i]
}

func (l *exploreList) Push(x any) {
	v := x.(*exploreEntry)
	*l = append(*l, v)
}

func (l *exploreList) Pop() any {
	if len(*l) == 0 {
		return nil
	}
	old := *l
	n := len(old)
	v := old[n-1]
	old[n-1] = nil
	*l = old[0 : n-1]
	return v
}
