package brdcst

import (
	"context"
	"fmt"
	"time"

	"github.com/libp2p/go-libp2p-kad-dht/v2/kadt"

	"github.com/libp2p/go-libp2p-kad-dht/v2/internal/coord/query"
	"github.com/libp2p/go-libp2p-kad-dht/v2/pb"

	"github.com/benbjohnson/clock"
	"github.com/libp2p/go-libp2p-kad-dht/v2/tele"
	"github.com/plprobelab/go-kademlia/kad"
	"github.com/plprobelab/go-kademlia/kaderr"
	"go.opentelemetry.io/otel/trace"
)

type Broadcast[K kad.Key[K], N kad.NodeID[K]] struct {
	pool *query.Pool[K, N, *pb.Message] // TODO: remove *pb.Message

	// cfg is a copy of the optional configuration supplied to the Broadcast
	cfg BroadcastConfig[K]
}

// BroadcastConfig specifies optional configuration for a Broadcast
type BroadcastConfig[K kad.Key[K]] struct {
	Timeout            time.Duration // the time to wait before terminating a query that is not making progress
	RequestConcurrency int           // the maximum number of concurrent requests that each query may have in flight
	RequestTimeout     time.Duration // the timeout queries should use for contacting a single node
	Clock              clock.Clock   // a clock that may replaced by a mock when testing
}

// Validate checks the configuration options and returns an error if any have invalid values.
func (cfg *BroadcastConfig[K]) Validate() error {
	if cfg.Clock == nil {
		return &kaderr.ConfigurationError{
			Component: "BroadcastConfig",
			Err:       fmt.Errorf("clock must not be nil"),
		}
	}

	if cfg.Timeout < 1 {
		return &kaderr.ConfigurationError{
			Component: "BroadcastConfig",
			Err:       fmt.Errorf("timeout must be greater than zero"),
		}
	}

	if cfg.RequestConcurrency < 1 {
		return &kaderr.ConfigurationError{
			Component: "BroadcastConfig",
			Err:       fmt.Errorf("request concurrency must be greater than zero"),
		}
	}

	if cfg.RequestTimeout < 1 {
		return &kaderr.ConfigurationError{
			Component: "BroadcastConfig",
			Err:       fmt.Errorf("request timeout must be greater than zero"),
		}
	}

	return nil
}

// DefaultBroadcastConfig returns the default configuration options for a Broadcast.
// Options may be overridden before passing to NewBroadcast
func DefaultBroadcastConfig[K kad.Key[K]]() *BroadcastConfig[K] {
	return &BroadcastConfig[K]{
		Clock:              clock.New(), // use standard time
		Timeout:            5 * time.Minute,
		RequestConcurrency: 3,
		RequestTimeout:     time.Minute,
	}
}

// NewBroadcast // TODO: replace *pb.Message
func NewBroadcast[K kad.Key[K], N kad.NodeID[K]](pool *query.Pool[K, N, *pb.Message], cfg *BroadcastConfig[K]) (*Broadcast[K, N], error) {
	if cfg == nil {
		cfg = DefaultBroadcastConfig[K]()
	} else if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return &Broadcast[K, N]{
		pool: pool,
		cfg:  *cfg,
	}, nil
}

// Advance advances the state of the bootstrap by attempting to advance its query if running.
func (b *Broadcast[K, N]) Advance(ctx context.Context, ev BroadcastEvent) BroadcastState {
	ctx, span := tele.StartSpan(ctx, "Broadcast.Advance", trace.WithAttributes(tele.AttrInEvent(ev)))
	defer span.End()

	var cmd query.PoolEvent
	switch ev := ev.(type) {
	case *EventBroadcastStart[K, N]:
		cmd = &query.EventPoolAddFindCloserQuery[K, N]{
			QueryID:           ev.QueryID,
			Target:            ev.Target,
			KnownClosestNodes: ev.KnownClosestNodes,
		}
		return b.advancePool(ctx, cmd)
	case *EventBroadcastStopQuery:
		// TODO: ...
	case *EventBroadcastNodeResponse[K, N]:
		// TODO: for "optimistic provide" check if any of the peers is close enough
		cmd = &query.EventPoolNodeResponse[K, N]{
			QueryID:     ev.QueryID,
			NodeID:      ev.NodeID,
			CloserNodes: ev.CloserNodes,
		}
		return b.advancePool(ctx, cmd)
	case *EventBroadcastNodeFailure[K, N]:
		// TODO: for "optimistic provide" check if any of the peers is close enough
		cmd = &query.EventPoolNodeFailure[K, N]{
			QueryID: ev.QueryID,
			NodeID:  ev.NodeID,
			Error:   ev.Error,
		}
		return b.advancePool(ctx, cmd)
	case *EventBroadcastPoll:
		// ignore, nothing to do
	default:
		panic(fmt.Sprintf("unexpected event: %T", ev))
	}

	return &StateBroadcastIdle{}
}

func (b *Broadcast[K, N]) advancePool(ctx context.Context, qev query.PoolEvent) BroadcastState {
	ctx, span := tele.StartSpan(ctx, "Broadcast.advanceQuery")
	defer span.End()

	state := b.pool.Advance(ctx, qev)
	switch st := state.(type) {
	case *query.StatePoolFindCloser[kadt.Key, kadt.PeerID]:
		return &StateBroadcastFindCloser{
			QueryID: st.QueryID,
			NodeID:  st.NodeID,
			Target:  st.Target,
			Stats:   st.Stats,
		}
	case *query.StatePoolWaitingAtCapacity:
		// nothing to do except wait for message response or timeout
	case *query.StatePoolWaitingWithCapacity:
		// nothing to do except wait for message response or timeout
	case *query.StatePoolQueryFinished[kadt.Key, kadt.PeerID]:
		// TODO: store records
		// here starts the logic of storing records of the 20 closest peers.
	case *query.StatePoolQueryTimeout:
		// TODO
	case *query.StatePoolIdle:
		// nothing to do
	default:
		panic(fmt.Sprintf("unexpected pool state: %T", st))
	}

	return &StateBroadcastIdle{}
}
