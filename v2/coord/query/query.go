package query

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

type QueryID string

const InvalidQueryID QueryID = ""

type QueryStats struct {
	Start    time.Time
	End      time.Time
	Requests int
	Success  int
	Failure  int
}

// QueryConfig specifies optional configuration for a Query
type QueryConfig[K kad.Key[K]] struct {
	Concurrency    int           // the maximum number of concurrent requests that may be in flight
	NumResults     int           // the minimum number of nodes to successfully contact before considering iteration complete
	RequestTimeout time.Duration // the timeout for contacting a single node
	Clock          clock.Clock   // a clock that may replaced by a mock when testing
}

// Validate checks the configuration options and returns an error if any have invalid values.
func (cfg *QueryConfig[K]) Validate() error {
	if cfg.Clock == nil {
		return &kaderr.ConfigurationError{
			Component: "QueryConfig",
			Err:       fmt.Errorf("clock must not be nil"),
		}
	}
	if cfg.Concurrency < 1 {
		return &kaderr.ConfigurationError{
			Component: "QueryConfig",
			Err:       fmt.Errorf("concurrency must be greater than zero"),
		}
	}
	if cfg.NumResults < 1 {
		return &kaderr.ConfigurationError{
			Component: "QueryConfig",
			Err:       fmt.Errorf("num results must be greater than zero"),
		}
	}
	if cfg.RequestTimeout < 1 {
		return &kaderr.ConfigurationError{
			Component: "QueryConfig",
			Err:       fmt.Errorf("request timeout must be greater than zero"),
		}
	}
	return nil
}

// DefaultQueryConfig returns the default configuration options for a Query.
// Options may be overridden before passing to NewQuery
func DefaultQueryConfig[K kad.Key[K]]() *QueryConfig[K] {
	return &QueryConfig[K]{
		Concurrency:    3,
		NumResults:     20,
		RequestTimeout: time.Minute,
		Clock:          clock.New(), // use standard time
	}
}

type Query[K kad.Key[K], N kad.NodeID[K]] struct {
	self N
	id   QueryID

	// cfg is a copy of the optional configuration supplied to the query
	cfg QueryConfig[K]

	iter   NodeIter[K, N]
	target K
	stats  QueryStats

	// finished indicates that that the query has completed its work or has been stopped.
	finished bool

	// inFlight is number of requests in flight, will be <= concurrency
	inFlight int
}

func NewQuery[K kad.Key[K], N kad.NodeID[K]](self N, id QueryID, target K, iter NodeIter[K, N], knownClosestNodes []N, cfg *QueryConfig[K]) (*Query[K, N], error) {
	if cfg == nil {
		cfg = DefaultQueryConfig[K]()
	} else if err := cfg.Validate(); err != nil {
		return nil, err
	}

	for _, node := range knownClosestNodes {
		// exclude self from closest nodes
		if key.Equal(node.Key(), self.Key()) {
			continue
		}
		iter.Add(&NodeStatus[K, N]{
			NodeID: node,
			State:  &StateNodeNotContacted{},
		})
	}

	return &Query[K, N]{
		self:   self,
		id:     id,
		cfg:    *cfg,
		iter:   iter,
		target: target,
	}, nil
}

func (q *Query[K, N]) Advance(ctx context.Context, ev QueryEvent) QueryState {
	ctx, span := tele.StartSpan(ctx, "Query.Advance")
	defer span.End()
	if q.finished {
		return &StateQueryFinished{
			QueryID: q.id,
			Stats:   q.stats,
		}
	}

	switch tev := ev.(type) {
	case *EventQueryCancel:
		q.markFinished()
		return &StateQueryFinished{
			QueryID: q.id,
			Stats:   q.stats,
		}
	case *EventQueryFindCloserResponse[K, N]:
		q.onMessageResponse(ctx, tev.NodeID, tev.CloserNodes)
	case *EventQueryFindCloserFailure[K, N]:
		q.onMessageFailure(ctx, tev.NodeID)
	case nil:
		// TEMPORARY: no event to process
	default:
		panic(fmt.Sprintf("unexpected event: %T", tev))
	}

	// count number of successes in the order of the iteration
	successes := 0

	// progressing is set to true if any node is still awaiting contact
	progressing := false

	// TODO: if stalled then we should contact all remaining nodes that have not already been queried
	atCapacity := func() bool {
		return q.inFlight >= q.cfg.Concurrency
	}

	// get all the nodes in order of distance from the target
	// TODO: turn this into a walk or iterator on trie.Trie

	var returnState QueryState

	q.iter.Each(ctx, func(ctx context.Context, ni *NodeStatus[K, N]) bool {
		switch st := ni.State.(type) {
		case *StateNodeWaiting:
			if q.cfg.Clock.Now().After(st.Deadline) {
				// mark node as unresponsive
				ni.State = &StateNodeUnresponsive{}
				q.inFlight--
				q.stats.Failure++
			} else if atCapacity() {
				returnState = &StateQueryWaitingAtCapacity{
					QueryID: q.id,
					Stats:   q.stats,
				}
				return true
			} else {
				// The iterator is still waiting for a result from a node so can't be considered done
				progressing = true
			}
		case *StateNodeSucceeded:
			successes++
			// The iterator has attempted to contact all nodes closer than this one.
			// If the iterator is not progressing then it doesn't expect any more nodes to be added to the list.
			// If it has contacted at least NumResults nodes successfully then the iteration is done.
			if !progressing && successes >= q.cfg.NumResults {
				q.markFinished()
				returnState = &StateQueryFinished{
					QueryID: q.id,
					Stats:   q.stats,
				}
				return true
			}

		case *StateNodeNotContacted:
			if !atCapacity() {
				deadline := q.cfg.Clock.Now().Add(q.cfg.RequestTimeout)
				ni.State = &StateNodeWaiting{Deadline: deadline}
				q.inFlight++
				q.stats.Requests++
				if q.stats.Start.IsZero() {
					q.stats.Start = q.cfg.Clock.Now()
				}
				returnState = &StateQueryFindCloser[K, N]{
					NodeID:  ni.NodeID,
					QueryID: q.id,
					Stats:   q.stats,
					Target:  q.target,
				}
				return true

			}
			returnState = &StateQueryWaitingAtCapacity{
				QueryID: q.id,
				Stats:   q.stats,
			}
			return true
		case *StateNodeUnresponsive:
			// ignore
		case *StateNodeFailed:
			// ignore
		default:
			panic(fmt.Sprintf("unexpected state: %T", ni.State))
		}

		return false
	})

	if returnState != nil {
		return returnState
	}

	if q.inFlight > 0 {
		// The iterator is still waiting for results and not at capacity
		return &StateQueryWaitingWithCapacity{
			QueryID: q.id,
			Stats:   q.stats,
		}
	}

	// The iterator is finished because all available nodes have been contacted
	// and the iterator is not waiting for any more results.
	q.markFinished()
	return &StateQueryFinished{
		QueryID: q.id,
		Stats:   q.stats,
	}
}

func (q *Query[K, N]) markFinished() {
	q.finished = true
	if q.stats.End.IsZero() {
		q.stats.End = q.cfg.Clock.Now()
	}
}

// onMessageResponse processes the result of a successful response received from a node.
func (q *Query[K, N]) onMessageResponse(ctx context.Context, node N, closer []N) {
	ni, found := q.iter.Find(node.Key())
	if !found {
		// got a rogue message
		return
	}
	switch st := ni.State.(type) {
	case *StateNodeWaiting:
		q.inFlight--
		q.stats.Success++
	case *StateNodeUnresponsive:
		q.stats.Success++

	case *StateNodeNotContacted:
		// ignore duplicate or late response
		return
	case *StateNodeFailed:
		// ignore duplicate or late response
		return
	case *StateNodeSucceeded:
		// ignore duplicate or late response
		return
	default:
		panic(fmt.Sprintf("unexpected state: %T", st))
	}

	// add closer nodes to list
	for _, id := range closer {
		// exclude self from closest nodes
		if key.Equal(id.Key(), q.self.Key()) {
			continue
		}
		q.iter.Add(&NodeStatus[K, N]{
			NodeID: id,
			State:  &StateNodeNotContacted{},
		})
	}
	ni.State = &StateNodeSucceeded{}
}

// onMessageFailure processes the result of a failed attempt to contact a node.
func (q *Query[K, N]) onMessageFailure(ctx context.Context, node N) {
	ni, found := q.iter.Find(node.Key())
	if !found {
		// got a rogue message
		return
	}
	switch st := ni.State.(type) {
	case *StateNodeWaiting:
		q.inFlight--
		q.stats.Failure++
	case *StateNodeUnresponsive:
		// update node state to failed
		break
	case *StateNodeNotContacted:
		// update node state to failed
		break
	case *StateNodeFailed:
		// ignore duplicate or late response
		return
	case *StateNodeSucceeded:
		// ignore duplicate or late response
		return
	default:
		panic(fmt.Sprintf("unexpected state: %T", st))
	}

	ni.State = &StateNodeFailed{}
}

type QueryState interface {
	queryState()
}

// StateQueryFinished indicates that the [Query] has finished.
type StateQueryFinished struct {
	QueryID QueryID
	Stats   QueryStats
}

// StateQueryFindCloser indicates that the [Query] wants to send a find closer nodes message to a node.
type StateQueryFindCloser[K kad.Key[K], N kad.NodeID[K]] struct {
	QueryID QueryID
	Target  K // the key that the query wants to find closer nodes for
	NodeID  N // the node to send the message to
	Stats   QueryStats
}

// StateQueryWaitingAtCapacity indicates that the [Query] is waiting for results and is at capacity.
type StateQueryWaitingAtCapacity struct {
	QueryID QueryID
	Stats   QueryStats
}

// StateQueryWaitingWithCapacity indicates that the [Query] is waiting for results but has no further nodes to contact.
type StateQueryWaitingWithCapacity struct {
	QueryID QueryID
	Stats   QueryStats
}

// queryState() ensures that only [Query] states can be assigned to a QueryState.
func (*StateQueryFinished) queryState()            {}
func (*StateQueryFindCloser[K, N]) queryState()    {}
func (*StateQueryWaitingAtCapacity) queryState()   {}
func (*StateQueryWaitingWithCapacity) queryState() {}

type QueryEvent interface {
	queryEvent()
}

// EventQueryMessageResponse notifies a query to stop all work and enter the finished state.
type EventQueryCancel struct{}

// EventQueryFindCloserResponse notifies a [Query] that an attempt to find closer nodes has received a successful response.
type EventQueryFindCloserResponse[K kad.Key[K], N kad.NodeID[K]] struct {
	NodeID      N   // the node the message was sent to
	CloserNodes []N // the closer nodes sent by the node
}

// EventQueryFindCloserFailure notifies a [Query] that an attempt to find closer nodes has failed.
type EventQueryFindCloserFailure[K kad.Key[K], N kad.NodeID[K]] struct {
	NodeID N     // the node the message was sent to
	Error  error // the error that caused the failure, if any
}

// queryEvent() ensures that only events accepted by [Query] can be assigned to a [QueryEvent].
func (*EventQueryCancel) queryEvent()                   {}
func (*EventQueryFindCloserResponse[K, N]) queryEvent() {}
func (*EventQueryFindCloserFailure[K, N]) queryEvent()  {}
