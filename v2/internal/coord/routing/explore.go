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

// ExploreQueryID is the id for the query operated by the explore process
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

	schedule ExploreSchedule
}

// NodeIDForCplFunc is a function that given a cpl generates a [kad.NodeID] with a key that has
// a common prefix length with k of length cpl.
// Invariant: CommonPrefixLength(k, node.Key()) = cpl
type NodeIDForCplFunc[K kad.Key[K], N kad.NodeID[K]] func(k K, cpl int) (N, error)

// An ExploreSchedule provides an ordering for explorations of each cpl in a routing table.
type ExploreSchedule interface {
	// NextCpl returns the first cpl to be explored whose due time is before or equal to the given time.
	// The due time of the cpl should be updated by its designated interval so that its next due time is increased.
	// If no cpl is due at the given time NextCpl should return -1, false
	NextCpl(ts time.Time) (int, bool)
}

// ExploreConfig specifies optional configuration for an [Explore]
type ExploreConfig struct {
	// Clock is  a clock that may replaced by a mock when testing
	Clock clock.Clock

	// Timeout is maximum time to allow for performing an explore for a CPL.
	Timeout time.Duration
}

// Validate checks the configuration options and returns an error if any have invalid values.
func (cfg *ExploreConfig) Validate() error {
	if cfg.Clock == nil {
		return fmt.Errorf("clock must not be nil")
	}

	if cfg.Timeout < 1 {
		return fmt.Errorf("timeout must be greater than zero")
	}

	return nil
}

// DefaultExploreConfig returns the default configuration options for an [Explore].
// Options may be overridden before passing to [NewExplore].
func DefaultExploreConfig() *ExploreConfig {
	return &ExploreConfig{
		Clock:   clock.New(),      // use standard time
		Timeout: 10 * time.Minute, // MAGIC
	}
}

func NewExplore[K kad.Key[K], N kad.NodeID[K]](self N, rt RoutingTableCpl[K, N], cplFn NodeIDForCplFunc[K, N], schedule ExploreSchedule, cfg *ExploreConfig) (*Explore[K, N], error) {
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
		schedule: schedule,
	}

	return e, nil
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

	// if query is running, give it a chance to advance
	if e.qry != nil {
		return e.advanceQuery(ctx, &query.EventQueryPoll{})
	}

	// is an explore due yet?
	next, ok := e.schedule.NextCpl(e.cfg.Clock.Now())
	if !ok {
		return &StateExploreIdle{}
	}

	// start an explore query
	node, err := e.cplFn(e.self.Key(), next)
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
	e.qryCpl = next

	return e.advanceQuery(ctx, &query.EventQueryPoll{})
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

// A DynamicExploreSchedule calculates an explore schedule dynamically
type DynamicExploreSchedule struct {
	// maxCpl is the maximum CPL (common prefix length) that will be scheduled.
	maxCpl int

	// interval is the minimum time interval to leave between explorations of the same CPL.
	interval time.Duration

	// multiplier is a factor that is applied to interval for CPLs lower than the maximum
	multiplier float64

	// jitter is a factor that is used to increase the calculated interval for the next explore
	// operation by a small random amount.
	jitter float64

	// cpls is a list of cpls ordered by the time the next explore is due
	cpls *exploreList
}

// NewDynamicExploreSchedule creates a new dynamic explore schedule.
//
// maxCpl is the maximum CPL (common prefix length) that will be scheduled.
// interval is the base time interval to leave between explorations of the same CPL.
// multiplier is a factor that is applied to interval for CPLs lower than the maximum to increase the interval between
// explorations for lower CPLs (which contain nodes that are more distant).
// jitter is a factor that is used to increase the calculated interval for the next explore
// operation by a small random amount. It must be between 0 and 0.05. When zero, no jitter is applied.
//
// The interval to the next explore is calculated using the following formula:
//
//	interval + (maxCpl - CPL) x interval x multiplier + interval * rand(jitter)
//
// For example, given an a max CPL of 14, an interval of 1 hour, a multiplier of 1 and 0 jitter the following
// schedule will be created:
//
//	CPL 14 explored every hour
//	CPL 13 explored every two hours
//	CPL 12 explored every three hours
//	...
//	CPL 0 explored every 14 hours
//
// For example, given an a max CPL of 14, an interval of 1 hour, a multiplier of 1.5 and 0.01 jitter the following
// schedule will be created:
//
//	CPL 14 explored every 1 hour + up to 6 minutes random jitter
//	CPL 13 explored every 2.5 hours + up to 6 minutes random jitter
//	CPL 12 explored every 4 hours + up to 6 minutes random jitter
//	...
//	CPL 0 explored every 22 hours + up to 6 minutes random jitter
func NewDynamicExploreSchedule(maxCpl int, start time.Time, interval time.Duration, multiplier float64, jitter float64) (*DynamicExploreSchedule, error) {
	if maxCpl < 1 {
		return nil, fmt.Errorf("maximum cpl must be greater than zero")
	}

	if interval < 1 {
		return nil, fmt.Errorf("interval must be greater than zero")
	}

	if multiplier < 1 {
		return nil, fmt.Errorf("interval multiplier must be greater than or equal to one")
	}

	if jitter < 0 {
		return nil, fmt.Errorf("interval jitter must not be negative")
	}

	if jitter > 0.05 {
		return nil, fmt.Errorf("interval jitter must not be greater than 0.05")
	}

	s := &DynamicExploreSchedule{
		maxCpl:     maxCpl,
		interval:   interval,
		multiplier: multiplier,
		jitter:     jitter,
		cpls:       new(exploreList),
	}

	// build the initial schedule
	for cpl := maxCpl; cpl >= 0; cpl-- {
		*s.cpls = append(*s.cpls, &exploreEntry{
			Cpl: cpl,
			Due: start.Add(s.cplInterval(cpl)),
		})
	}
	heap.Init(s.cpls)

	return s, nil
}

func (s *DynamicExploreSchedule) NextCpl(ts time.Time) (int, bool) {
	// is an explore due yet?
	next := (*s.cpls)[0]
	if !next.Due.After(ts) {
		// update its schedule

		interval := float64(s.interval) + float64(s.interval)*float64(s.maxCpl-next.Cpl)*s.multiplier
		interval *= 1 + rand.Float64()*s.jitter

		next.Due = ts.Add(s.cplInterval(next.Cpl))
		heap.Fix(s.cpls, 0) // update the heap
		return next.Cpl, true
	}

	return -1, false
}

// cplInterval calculates the explore interval for a given cpl
func (s *DynamicExploreSchedule) cplInterval(cpl int) time.Duration {
	interval := float64(s.interval)
	interval += float64(s.interval) * float64(s.maxCpl-cpl) * s.multiplier
	interval += float64(s.interval) * s.jitter * rand.Float64()
	return time.Duration(interval)
}

// A NoWaitExploreSchedule implements an explore schedule that cycles through each cpl without delays
type NoWaitExploreSchedule struct {
	maxCpl  int
	nextCpl int
}

func NewNoWaitExploreSchedule(maxCpl int) *NoWaitExploreSchedule {
	return &NoWaitExploreSchedule{
		maxCpl:  maxCpl,
		nextCpl: maxCpl,
	}
}

func (n *NoWaitExploreSchedule) NextCpl(ts time.Time) (int, bool) {
	next := n.nextCpl
	n.nextCpl--
	if n.nextCpl < 0 {
		n.nextCpl = n.maxCpl
	}
	return next, true
}
