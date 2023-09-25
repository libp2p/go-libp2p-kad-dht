package routing

import (
	"context"
	"fmt"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/plprobelab/go-kademlia/kad"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/libp2p/go-libp2p-kad-dht/v2/errs"
	"github.com/libp2p/go-libp2p-kad-dht/v2/internal/coord/coordt"
	"github.com/libp2p/go-libp2p-kad-dht/v2/internal/coord/query"
	"github.com/libp2p/go-libp2p-kad-dht/v2/tele"
)

// BootstrapQueryID is the id for the query operated by the bootstrap process
const BootstrapQueryID = coordt.QueryID("bootstrap")

type Bootstrap[K kad.Key[K], N kad.NodeID[K]] struct {
	// self is the node id of the system the bootstrap is running on
	self N

	// qry is the query used by the bootstrap process
	qry *query.Query[K, N, any]

	// cfg is a copy of the optional configuration supplied to the Bootstrap
	cfg BootstrapConfig
}

// BootstrapConfig specifies optional configuration for a Bootstrap
type BootstrapConfig struct {
	Timeout            time.Duration // the time to wait before terminating a query that is not making progress
	RequestConcurrency int           // the maximum number of concurrent requests that each query may have in flight
	RequestTimeout     time.Duration // the timeout queries should use for contacting a single node
	Clock              clock.Clock   // a clock that may replaced by a mock when testing
}

// Validate checks the configuration options and returns an error if any have invalid values.
func (cfg *BootstrapConfig) Validate() error {
	if cfg.Clock == nil {
		return &errs.ConfigurationError{
			Component: "BootstrapConfig",
			Err:       fmt.Errorf("clock must not be nil"),
		}
	}

	if cfg.Timeout < 1 {
		return &errs.ConfigurationError{
			Component: "BootstrapConfig",
			Err:       fmt.Errorf("timeout must be greater than zero"),
		}
	}

	if cfg.RequestConcurrency < 1 {
		return &errs.ConfigurationError{
			Component: "BootstrapConfig",
			Err:       fmt.Errorf("request concurrency must be greater than zero"),
		}
	}

	if cfg.RequestTimeout < 1 {
		return &errs.ConfigurationError{
			Component: "BootstrapConfig",
			Err:       fmt.Errorf("request timeout must be greater than zero"),
		}
	}

	return nil
}

// DefaultBootstrapConfig returns the default configuration options for a Bootstrap.
// Options may be overridden before passing to NewBootstrap
func DefaultBootstrapConfig() *BootstrapConfig {
	return &BootstrapConfig{
		Clock:              clock.New(),     // use standard time
		Timeout:            5 * time.Minute, // MAGIC
		RequestConcurrency: 3,               // MAGIC
		RequestTimeout:     time.Minute,     // MAGIC
	}
}

func NewBootstrap[K kad.Key[K], N kad.NodeID[K]](self N, cfg *BootstrapConfig) (*Bootstrap[K, N], error) {
	if cfg == nil {
		cfg = DefaultBootstrapConfig()
	} else if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return &Bootstrap[K, N]{
		self: self,
		cfg:  *cfg,
	}, nil
}

// Advance advances the state of the bootstrap by attempting to advance its query if running.
func (b *Bootstrap[K, N]) Advance(ctx context.Context, ev BootstrapEvent) BootstrapState {
	ctx, span := tele.StartSpan(ctx, "Bootstrap.Advance", trace.WithAttributes(tele.AttrInEvent(ev)))
	defer span.End()

	switch tev := ev.(type) {
	case *EventBootstrapStart[K, N]:
		// TODO: ignore start event if query is already in progress
		iter := query.NewClosestNodesIter[K, N](b.self.Key())

		qryCfg := query.DefaultQueryConfig()
		qryCfg.Clock = b.cfg.Clock
		qryCfg.Concurrency = b.cfg.RequestConcurrency
		qryCfg.RequestTimeout = b.cfg.RequestTimeout

		qry, err := query.NewFindCloserQuery[K, N, any](b.self, BootstrapQueryID, b.self.Key(), iter, tev.KnownClosestNodes, qryCfg)
		if err != nil {
			// TODO: don't panic
			panic(err)
		}
		b.qry = qry
		return b.advanceQuery(ctx, &query.EventQueryPoll{})

	case *EventBootstrapFindCloserResponse[K, N]:
		return b.advanceQuery(ctx, &query.EventQueryNodeResponse[K, N]{
			NodeID:      tev.NodeID,
			CloserNodes: tev.CloserNodes,
		})
	case *EventBootstrapFindCloserFailure[K, N]:
		span.RecordError(tev.Error)
		return b.advanceQuery(ctx, &query.EventQueryNodeFailure[K, N]{
			NodeID: tev.NodeID,
			Error:  tev.Error,
		})

	case *EventBootstrapPoll:
		// ignore, nothing to do
	default:
		panic(fmt.Sprintf("unexpected event: %T", tev))
	}

	if b.qry != nil {
		return b.advanceQuery(ctx, &query.EventQueryPoll{})
	}

	return &StateBootstrapIdle{}
}

func (b *Bootstrap[K, N]) advanceQuery(ctx context.Context, qev query.QueryEvent) BootstrapState {
	ctx, span := tele.StartSpan(ctx, "Bootstrap.advanceQuery")
	defer span.End()
	state := b.qry.Advance(ctx, qev)
	switch st := state.(type) {
	case *query.StateQueryFindCloser[K, N]:
		span.SetAttributes(attribute.String("out_state", "StateQueryFindCloser"))
		return &StateBootstrapFindCloser[K, N]{
			QueryID: st.QueryID,
			Stats:   st.Stats,
			NodeID:  st.NodeID,
			Target:  st.Target,
		}
	case *query.StateQueryFinished[K, N]:
		span.SetAttributes(attribute.String("out_state", "StateBootstrapFinished"))
		return &StateBootstrapFinished{
			Stats: st.Stats,
		}
	case *query.StateQueryWaitingAtCapacity:
		elapsed := b.cfg.Clock.Since(st.Stats.Start)
		if elapsed > b.cfg.Timeout {
			span.SetAttributes(attribute.String("out_state", "StateBootstrapTimeout"))
			return &StateBootstrapTimeout{
				Stats: st.Stats,
			}
		}
		span.SetAttributes(attribute.String("out_state", "StateBootstrapWaiting"))
		return &StateBootstrapWaiting{
			Stats: st.Stats,
		}
	case *query.StateQueryWaitingWithCapacity:
		elapsed := b.cfg.Clock.Since(st.Stats.Start)
		if elapsed > b.cfg.Timeout {
			span.SetAttributes(attribute.String("out_state", "StateBootstrapTimeout"))
			return &StateBootstrapTimeout{
				Stats: st.Stats,
			}
		}
		span.SetAttributes(attribute.String("out_state", "StateBootstrapWaiting"))
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
type StateBootstrapFindCloser[K kad.Key[K], N kad.NodeID[K]] struct {
	QueryID coordt.QueryID
	Target  K // the key that the query wants to find closer nodes for
	NodeID  N // the node to send the message to
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
func (*StateBootstrapFindCloser[K, N]) bootstrapState() {}
func (*StateBootstrapIdle) bootstrapState()             {}
func (*StateBootstrapFinished) bootstrapState()         {}
func (*StateBootstrapTimeout) bootstrapState()          {}
func (*StateBootstrapWaiting) bootstrapState()          {}

// BootstrapEvent is an event intended to advance the state of a bootstrap.
type BootstrapEvent interface {
	bootstrapEvent()
}

// EventBootstrapPoll is an event that signals the bootstrap that it can perform housekeeping work such as time out queries.
type EventBootstrapPoll struct{}

// EventBootstrapStart is an event that attempts to start a new bootstrap
type EventBootstrapStart[K kad.Key[K], N kad.NodeID[K]] struct {
	KnownClosestNodes []N
}

// EventBootstrapFindCloserResponse notifies a bootstrap that an attempt to find closer nodes has received a successful response.
type EventBootstrapFindCloserResponse[K kad.Key[K], N kad.NodeID[K]] struct {
	NodeID      N   // the node the message was sent to
	CloserNodes []N // the closer nodes sent by the node
}

// EventBootstrapFindCloserFailure notifies a bootstrap that an attempt to find closer nodes has failed.
type EventBootstrapFindCloserFailure[K kad.Key[K], N kad.NodeID[K]] struct {
	NodeID N     // the node the message was sent to
	Error  error // the error that caused the failure, if any
}

// bootstrapEvent() ensures that only events accepted by a [Bootstrap] can be assigned to the [BootstrapEvent] interface.
func (*EventBootstrapPoll) bootstrapEvent()                     {}
func (*EventBootstrapStart[K, N]) bootstrapEvent()              {}
func (*EventBootstrapFindCloserResponse[K, N]) bootstrapEvent() {}
func (*EventBootstrapFindCloserFailure[K, N]) bootstrapEvent()  {}
