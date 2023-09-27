package query

import (
	"context"
	"fmt"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/plprobelab/go-kademlia/kad"
	"github.com/plprobelab/go-kademlia/kaderr"
	"github.com/plprobelab/go-kademlia/key"
	"go.opentelemetry.io/otel/trace"

	"github.com/libp2p/go-libp2p-kad-dht/v2/internal/coord/coordt"
	"github.com/libp2p/go-libp2p-kad-dht/v2/tele"
)

type QueryStats struct {
	Start    time.Time
	End      time.Time
	Requests int
	Success  int
	Failure  int
}

// QueryConfig specifies optional configuration for a Query
type QueryConfig struct {
	Concurrency    int           // the maximum number of concurrent requests that may be in flight
	NumResults     int           // the minimum number of nodes to successfully contact before considering iteration complete
	RequestTimeout time.Duration // the timeout for contacting a single node
	Clock          clock.Clock   // a clock that may replaced by a mock when testing
}

// Validate checks the configuration options and returns an error if any have invalid values.
func (cfg *QueryConfig) Validate() error {
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
func DefaultQueryConfig() *QueryConfig {
	return &QueryConfig{
		Concurrency:    3,
		NumResults:     20,
		RequestTimeout: time.Minute,
		Clock:          clock.New(), // use standard time
	}
}

type Query[K kad.Key[K], N kad.NodeID[K], M coordt.Message] struct {
	self N
	id   coordt.QueryID

	// cfg is a copy of the optional configuration supplied to the query
	cfg QueryConfig

	iter       NodeIter[K, N]
	target     K
	msg        M
	findCloser bool
	stats      QueryStats

	// finished indicates that the query has completed its work or has been stopped.
	finished bool

	// targetNodes is the set of responsive nodes thought to be closest to the target.
	// It is populated once the query has been marked as finished.
	// This will contain up to [QueryConfig.NumResults] nodes.
	targetNodes []N

	// inFlight is number of requests in flight, will be <= concurrency
	inFlight int
}

func NewFindCloserQuery[K kad.Key[K], N kad.NodeID[K], M coordt.Message](self N, id coordt.QueryID, target K, iter NodeIter[K, N], knownClosestNodes []N, cfg *QueryConfig) (*Query[K, N, M], error) {
	var empty M
	q, err := NewQuery[K, N, M](self, id, target, empty, iter, knownClosestNodes, cfg)
	if err != nil {
		return nil, err
	}
	q.findCloser = true
	return q, nil
}

func NewQuery[K kad.Key[K], N kad.NodeID[K], M coordt.Message](self N, id coordt.QueryID, target K, msg M, iter NodeIter[K, N], knownClosestNodes []N, cfg *QueryConfig) (*Query[K, N, M], error) {
	if cfg == nil {
		cfg = DefaultQueryConfig()
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

	return &Query[K, N, M]{
		self:   self,
		id:     id,
		cfg:    *cfg,
		msg:    msg,
		iter:   iter,
		target: target,
	}, nil
}

func (q *Query[K, N, M]) Advance(ctx context.Context, ev QueryEvent) (out QueryState) {
	fmt.Printf("Advance query: %T\n", ev)
	ctx, span := tele.StartSpan(ctx, "Query.Advance", trace.WithAttributes(tele.AttrInEvent(ev)))
	defer func() {
		fmt.Printf("Advanced: %T\n", out)
		span.SetAttributes(tele.AttrOutEvent(out))
		span.End()
	}()

	if q.finished {
		return &StateQueryFinished[K, N]{
			QueryID:      q.id,
			Stats:        q.stats,
			ClosestNodes: q.targetNodes,
		}
	}

	switch tev := ev.(type) {
	case *EventQueryCancel:
		q.markFinished(ctx)
		return &StateQueryFinished[K, N]{
			QueryID:      q.id,
			Stats:        q.stats,
			ClosestNodes: q.targetNodes,
		}
	case *EventQueryNodeResponse[K, N]:
		fmt.Println("Node response from", tev.NodeID.String())
		q.onNodeResponse(ctx, tev.NodeID, tev.CloserNodes)
	case *EventQueryNodeFailure[K, N]:
		fmt.Println("Node failure from", tev.NodeID.String())
		span.RecordError(tev.Error)
		q.onNodeFailure(ctx, tev.NodeID)
	case *EventQueryPoll:
		// no event to process

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
				q.markFinished(ctx)
				returnState = &StateQueryFinished[K, N]{
					QueryID:      q.id,
					Stats:        q.stats,
					ClosestNodes: q.targetNodes,
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

				if q.findCloser {
					returnState = &StateQueryFindCloser[K, N]{
						NodeID:  ni.NodeID,
						QueryID: q.id,
						Stats:   q.stats,
						Target:  q.target,
					}
				} else {
					returnState = &StateQuerySendMessage[K, N, M]{
						NodeID:  ni.NodeID,
						QueryID: q.id,
						Stats:   q.stats,
						Message: q.msg,
					}
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
	q.markFinished(ctx)
	return &StateQueryFinished[K, N]{
		QueryID:      q.id,
		Stats:        q.stats,
		ClosestNodes: q.targetNodes,
	}
}

func (q *Query[K, N, M]) markFinished(ctx context.Context) {
	q.finished = true
	if q.stats.End.IsZero() {
		q.stats.End = q.cfg.Clock.Now()
	}

	q.targetNodes = make([]N, 0, q.cfg.NumResults)

	q.iter.Each(ctx, func(ctx context.Context, ni *NodeStatus[K, N]) bool {
		switch ni.State.(type) {
		case *StateNodeSucceeded:
			q.targetNodes = append(q.targetNodes, ni.NodeID)
			if len(q.targetNodes) >= q.cfg.NumResults {
				return true
			}
		}
		return false
	})
}

// onNodeResponse processes the result of a successful response received from a node.
func (q *Query[K, N, M]) onNodeResponse(ctx context.Context, node N, closer []N) {
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

// onNodeFailure processes the result of a failed attempt to contact a node.
func (q *Query[K, N, M]) onNodeFailure(ctx context.Context, node N) {
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
type StateQueryFinished[K kad.Key[K], N kad.NodeID[K]] struct {
	QueryID      coordt.QueryID
	Stats        QueryStats
	ClosestNodes []N // contains the closest nodes to the target key that were found
}

// StateQueryFindCloser indicates that the [Query] wants to send a find closer nodes message to a node.
type StateQueryFindCloser[K kad.Key[K], N kad.NodeID[K]] struct {
	QueryID coordt.QueryID
	Target  K // the key that the query wants to find closer nodes for
	NodeID  N // the node to send the message to
	Stats   QueryStats
}

// StateQuerySendMessage indicates that the [Query] wants to send a message to a node.
type StateQuerySendMessage[K kad.Key[K], N kad.NodeID[K], M coordt.Message] struct {
	QueryID coordt.QueryID
	NodeID  N // the node to send the message to
	Message M
	Stats   QueryStats
}

// StateQueryWaitingAtCapacity indicates that the [Query] is waiting for results and is at capacity.
type StateQueryWaitingAtCapacity struct {
	QueryID coordt.QueryID
	Stats   QueryStats
}

// StateQueryWaitingWithCapacity indicates that the [Query] is waiting for results but has no further nodes to contact.
type StateQueryWaitingWithCapacity struct {
	QueryID coordt.QueryID
	Stats   QueryStats
}

// queryState() ensures that only [Query] states can be assigned to a QueryState.
func (*StateQueryFinished[K, N]) queryState()       {}
func (*StateQueryFindCloser[K, N]) queryState()     {}
func (*StateQuerySendMessage[K, N, M]) queryState() {}
func (*StateQueryWaitingAtCapacity) queryState()    {}
func (*StateQueryWaitingWithCapacity) queryState()  {}

type QueryEvent interface {
	queryEvent()
}

// EventQueryMessageResponse notifies a query to stop all work and enter the finished state.
type EventQueryCancel struct{}

// EventQueryNodeResponse notifies a [Query] that an attempt to contact a node has received a successful response.
type EventQueryNodeResponse[K kad.Key[K], N kad.NodeID[K]] struct {
	NodeID      N   // the node the message was sent to
	CloserNodes []N // the closer nodes sent by the node
}

// EventQueryNodeFailure notifies a [Query] that an attempt to to contact a node has failed.
type EventQueryNodeFailure[K kad.Key[K], N kad.NodeID[K]] struct {
	NodeID N     // the node the message was sent to
	Error  error // the error that caused the failure, if any
}

// EventQueryPoll is an event that signals a [Query] that it can perform housekeeping work.
type EventQueryPoll struct{}

// queryEvent() ensures that only events accepted by [Query] can be assigned to a [QueryEvent].
func (*EventQueryCancel) queryEvent()             {}
func (*EventQueryNodeResponse[K, N]) queryEvent() {}
func (*EventQueryNodeFailure[K, N]) queryEvent()  {}
func (*EventQueryPoll) queryEvent()               {}
