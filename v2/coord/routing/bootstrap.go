package routing

import (
	"context"
	"fmt"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/plprobelab/go-kademlia/kad"
	"github.com/plprobelab/go-kademlia/kaderr"
	"github.com/plprobelab/go-kademlia/network/address"

	"github.com/libp2p/go-libp2p-kad-dht/v2/coord/query"
	"github.com/libp2p/go-libp2p-kad-dht/v2/tele"
)

type Bootstrap[K kad.Key[K]] struct {
	// self is the node id of the system the bootstrap is running on
	self kad.NodeID[K]

	// qry is the query used by the bootstrap process
	qry *query.Query[K]

	// cfg is a copy of the optional configuration supplied to the Bootstrap
	cfg BootstrapConfig[K]
}

// BootstrapConfig specifies optional configuration for a Bootstrap
type BootstrapConfig[K kad.Key[K]] struct {
	Timeout            time.Duration // the time to wait before terminating a query that is not making progress
	RequestConcurrency int           // the maximum number of concurrent requests that each query may have in flight
	RequestTimeout     time.Duration // the timeout queries should use for contacting a single node
	Clock              clock.Clock   // a clock that may replaced by a mock when testing
}

// Validate checks the configuration options and returns an error if any have invalid values.
func (cfg *BootstrapConfig[K]) Validate() error {
	if cfg.Clock == nil {
		return &kaderr.ConfigurationError{
			Component: "BootstrapConfig",
			Err:       fmt.Errorf("clock must not be nil"),
		}
	}

	if cfg.Timeout < 1 {
		return &kaderr.ConfigurationError{
			Component: "BootstrapConfig",
			Err:       fmt.Errorf("timeout must be greater than zero"),
		}
	}

	if cfg.RequestConcurrency < 1 {
		return &kaderr.ConfigurationError{
			Component: "BootstrapConfig",
			Err:       fmt.Errorf("request concurrency must be greater than zero"),
		}
	}

	if cfg.RequestTimeout < 1 {
		return &kaderr.ConfigurationError{
			Component: "BootstrapConfig",
			Err:       fmt.Errorf("request timeout must be greater than zero"),
		}
	}

	return nil
}

// DefaultBootstrapConfig returns the default configuration options for a Bootstrap.
// Options may be overridden before passing to NewBootstrap
func DefaultBootstrapConfig[K kad.Key[K]]() *BootstrapConfig[K] {
	return &BootstrapConfig[K]{
		Clock:              clock.New(), // use standard time
		Timeout:            5 * time.Minute,
		RequestConcurrency: 3,
		RequestTimeout:     time.Minute,
	}
}

func NewBootstrap[K kad.Key[K]](self kad.NodeID[K], cfg *BootstrapConfig[K]) (*Bootstrap[K], error) {
	if cfg == nil {
		cfg = DefaultBootstrapConfig[K]()
	} else if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return &Bootstrap[K]{
		self: self,
		cfg:  *cfg,
	}, nil
}

// Advance advances the state of the bootstrap by attempting to advance its query if running.
func (b *Bootstrap[K]) Advance(ctx context.Context, ev BootstrapEvent) BootstrapState {
	ctx, span := tele.StartSpan(ctx, "Bootstrap.Advance")
	defer span.End()

	switch tev := ev.(type) {
	case *EventBootstrapStart[K]:

		// TODO: ignore start event if query is already in progress
		iter := query.NewClosestNodesIter(b.self.Key())

		qryCfg := query.DefaultQueryConfig[K]()
		qryCfg.Clock = b.cfg.Clock
		qryCfg.Concurrency = b.cfg.RequestConcurrency
		qryCfg.RequestTimeout = b.cfg.RequestTimeout

		queryID := query.QueryID("bootstrap")

		qry, err := query.NewQuery[K](b.self, queryID, b.self.Key(), iter, tev.KnownClosestNodes, qryCfg)
		if err != nil {
			// TODO: don't panic
			panic(err)
		}
		b.qry = qry
		return b.advanceQuery(ctx, nil)

	case *EventBootstrapFindCloserResponse[K]:
		return b.advanceQuery(ctx, &query.EventQueryFindCloserResponse[K]{
			NodeID:      tev.NodeID,
			CloserNodes: tev.CloserNodes,
		})
	case *EventBootstrapFindCloserFailure[K]:
		return b.advanceQuery(ctx, &query.EventQueryFindCloserFailure[K]{
			NodeID: tev.NodeID,
			Error:  tev.Error,
		})

	case *EventBootstrapPoll:
	// ignore, nothing to do
	default:
		panic(fmt.Sprintf("unexpected event: %T", tev))
	}

	if b.qry != nil {
		return b.advanceQuery(ctx, nil)
	}

	return &StateBootstrapIdle{}
}

func (b *Bootstrap[K]) advanceQuery(ctx context.Context, qev query.QueryEvent) BootstrapState {
	state := b.qry.Advance(ctx, qev)
	switch st := state.(type) {
	case *query.StateQueryFindCloser[K]:
		return &StateBootstrapFindCloser[K]{
			QueryID: st.QueryID,
			Stats:   st.Stats,
			NodeID:  st.NodeID,
			Target:  st.Target,
		}
	case *query.StateQueryFinished:
		return &StateBootstrapFinished{
			Stats: st.Stats,
		}
	case *query.StateQueryWaitingAtCapacity:
		elapsed := b.cfg.Clock.Since(st.Stats.Start)
		if elapsed > b.cfg.Timeout {
			return &StateBootstrapTimeout{
				Stats: st.Stats,
			}
		}
		return &StateBootstrapWaiting{
			Stats: st.Stats,
		}
	case *query.StateQueryWaitingWithCapacity:
		elapsed := b.cfg.Clock.Since(st.Stats.Start)
		if elapsed > b.cfg.Timeout {
			return &StateBootstrapTimeout{
				Stats: st.Stats,
			}
		}
		return &StateBootstrapWaiting{
			Stats: st.Stats,
		}
	default:
		panic(fmt.Sprintf("unexpected state: %T", st))
	}
}

// BootstrapState is the state of a bootstrap.
type BootstrapState interface {
	bootstrapState()
}

// StateBootstrapFindCloser indicates that the bootstrap query wants to send a find closer nodes message to a node.
type StateBootstrapFindCloser[K kad.Key[K]] struct {
	QueryID query.QueryID
	Target  K             // the key that the query wants to find closer nodes for
	NodeID  kad.NodeID[K] // the node to send the message to
	Stats   query.QueryStats
}

// StateBootstrapIdle indicates that the bootstrap is not running its query.
type StateBootstrapIdle struct{}

// StateBootstrapFinished indicates that the bootstrap has finished.
type StateBootstrapFinished struct {
	Stats query.QueryStats
}

// StateBootstrapTimeout indicates that the bootstrap query has timed out.
type StateBootstrapTimeout struct {
	Stats query.QueryStats
}

// StateBootstrapWaiting indicates that the bootstrap query is waiting for a response.
type StateBootstrapWaiting struct {
	Stats query.QueryStats
}

// bootstrapState() ensures that only Bootstrap states can be assigned to a BootstrapState.
func (*StateBootstrapFindCloser[K]) bootstrapState() {}
func (*StateBootstrapIdle) bootstrapState()          {}
func (*StateBootstrapFinished) bootstrapState()      {}
func (*StateBootstrapTimeout) bootstrapState()       {}
func (*StateBootstrapWaiting) bootstrapState()       {}

// BootstrapEvent is an event intended to advance the state of a bootstrap.
type BootstrapEvent interface {
	bootstrapEvent()
}

// EventBootstrapPoll is an event that signals the bootstrap that it can perform housekeeping work such as time out queries.
type EventBootstrapPoll struct{}

// EventBootstrapStart is an event that attempts to start a new bootstrap
type EventBootstrapStart[K kad.Key[K]] struct {
	ProtocolID        address.ProtocolID
	KnownClosestNodes []kad.NodeID[K]
}

// EventBootstrapFindCloserResponse notifies a bootstrap that an attempt to find closer nodes has received a successful response.
type EventBootstrapFindCloserResponse[K kad.Key[K]] struct {
	NodeID      kad.NodeID[K]   // the node the message was sent to
	CloserNodes []kad.NodeID[K] // the closer nodes sent by the node
}

// EventBootstrapFindCloserFailure notifies a bootstrap that an attempt to find closer nodes has failed.
type EventBootstrapFindCloserFailure[K kad.Key[K]] struct {
	NodeID kad.NodeID[K] // the node the message was sent to
	Error  error         // the error that caused the failure, if any
}

// bootstrapEvent() ensures that only events accepted by a [Bootstrap] can be assigned to the [BootstrapEvent] interface.
func (*EventBootstrapPoll) bootstrapEvent()                  {}
func (*EventBootstrapStart[K]) bootstrapEvent()              {}
func (*EventBootstrapFindCloserResponse[K]) bootstrapEvent() {}
func (*EventBootstrapFindCloserFailure[K]) bootstrapEvent()  {}
