package coord

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/benbjohnson/clock"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/plprobelab/go-kademlia/kad"
	"github.com/plprobelab/go-kademlia/kaderr"
	"github.com/plprobelab/go-kademlia/network/address"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap/exp/zapslog"
	"golang.org/x/exp/slog"

	"github.com/libp2p/go-libp2p-kad-dht/v2/coord/query"
	"github.com/libp2p/go-libp2p-kad-dht/v2/coord/routing"
	"github.com/libp2p/go-libp2p-kad-dht/v2/kadt"
)

// A Coordinator coordinates the state machines that comprise a Kademlia DHT
type Coordinator struct {
	// self is the peer id of the system the dht is running on
	self peer.ID

	// cancel is used to cancel all running goroutines when the coordinator is cleaning up
	cancel context.CancelFunc

	// cfg is a copy of the optional configuration supplied to the dht
	cfg CoordinatorConfig

	// rt is the routing table used to look up nodes by distance
	rt kad.RoutingTable[kadt.Key, kadt.PeerID]

	// rtr is the message router used to send messages
	rtr Router

	routingNotifications chan RoutingNotification

	// networkBehaviour is the behaviour responsible for communicating with the network
	networkBehaviour *NetworkBehaviour

	// routingBehaviour is the behaviour responsible for maintaining the routing table
	routingBehaviour Behaviour[BehaviourEvent, BehaviourEvent]

	// queryBehaviour is the behaviour responsible for running user-submitted queries
	queryBehaviour Behaviour[BehaviourEvent, BehaviourEvent]

	// tele provides tracing and metric reporting capabilities
	tele *Telemetry
}

type CoordinatorConfig struct {
	PeerstoreTTL time.Duration // duration for which a peer is kept in the peerstore

	Clock clock.Clock // a clock that may replaced by a mock when testing

	QueryConcurrency int           // the maximum number of queries that may be waiting for message responses at any one time
	QueryTimeout     time.Duration // the time to wait before terminating a query that is not making progress

	RequestConcurrency int           // the maximum number of concurrent requests that each query may have in flight
	RequestTimeout     time.Duration // the timeout queries should use for contacting a single node

	Logger *slog.Logger // a structured logger that should be used when logging.

	MeterProvider  metric.MeterProvider // the meter provider to use when initialising metric instruments
	TracerProvider trace.TracerProvider // the tracer provider to use when initialising tracing
}

// Validate checks the configuration options and returns an error if any have invalid values.
func (cfg *CoordinatorConfig) Validate() error {
	if cfg.Clock == nil {
		return &kaderr.ConfigurationError{
			Component: "CoordinatorConfig",
			Err:       fmt.Errorf("clock must not be nil"),
		}
	}

	if cfg.QueryConcurrency < 1 {
		return &kaderr.ConfigurationError{
			Component: "CoordinatorConfig",
			Err:       fmt.Errorf("query concurrency must be greater than zero"),
		}
	}
	if cfg.QueryTimeout < 1 {
		return &kaderr.ConfigurationError{
			Component: "CoordinatorConfig",
			Err:       fmt.Errorf("query timeout must be greater than zero"),
		}
	}

	if cfg.RequestConcurrency < 1 {
		return &kaderr.ConfigurationError{
			Component: "CoordinatorConfig",
			Err:       fmt.Errorf("request concurrency must be greater than zero"),
		}
	}

	if cfg.RequestTimeout < 1 {
		return &kaderr.ConfigurationError{
			Component: "CoordinatorConfig",
			Err:       fmt.Errorf("request timeout must be greater than zero"),
		}
	}

	if cfg.Logger == nil {
		return &kaderr.ConfigurationError{
			Component: "CoordinatorConfig",
			Err:       fmt.Errorf("logger must not be nil"),
		}
	}

	if cfg.MeterProvider == nil {
		return &kaderr.ConfigurationError{
			Component: "CoordinatorConfig",
			Err:       fmt.Errorf("meter provider must not be nil"),
		}
	}

	if cfg.TracerProvider == nil {
		return &kaderr.ConfigurationError{
			Component: "CoordinatorConfig",
			Err:       fmt.Errorf("tracer provider must not be nil"),
		}
	}

	return nil
}

func DefaultCoordinatorConfig() *CoordinatorConfig {
	return &CoordinatorConfig{
		Clock:              clock.New(),
		PeerstoreTTL:       10 * time.Minute,
		QueryConcurrency:   3,
		QueryTimeout:       5 * time.Minute,
		RequestConcurrency: 3,
		RequestTimeout:     time.Minute,
		Logger:             slog.New(zapslog.NewHandler(logging.Logger("coord").Desugar().Core())),
		MeterProvider:      otel.GetMeterProvider(),
		TracerProvider:     otel.GetTracerProvider(),
	}
}

func NewCoordinator(self peer.ID, rtr Router, rt routing.RoutingTableCpl[kadt.Key, kadt.PeerID], cfg *CoordinatorConfig) (*Coordinator, error) {
	if cfg == nil {
		cfg = DefaultCoordinatorConfig()
	} else if err := cfg.Validate(); err != nil {
		return nil, err
	}

	// initialize a new telemetry struct
	tele, err := NewTelemetry(cfg.MeterProvider, cfg.TracerProvider)
	if err != nil {
		return nil, fmt.Errorf("init telemetry: %w", err)
	}

	qpCfg := query.DefaultPoolConfig()
	qpCfg.Clock = cfg.Clock
	qpCfg.Concurrency = cfg.QueryConcurrency
	qpCfg.Timeout = cfg.QueryTimeout
	qpCfg.QueryConcurrency = cfg.RequestConcurrency
	qpCfg.RequestTimeout = cfg.RequestTimeout

	qp, err := query.NewPool[kadt.Key](kadt.PeerID(self), qpCfg)
	if err != nil {
		return nil, fmt.Errorf("query pool: %w", err)
	}
	queryBehaviour := NewPooledQueryBehaviour(qp, cfg.Logger, tele.Tracer)

	bootstrapCfg := routing.DefaultBootstrapConfig[kadt.Key]()
	bootstrapCfg.Clock = cfg.Clock
	bootstrapCfg.Timeout = cfg.QueryTimeout
	bootstrapCfg.RequestConcurrency = cfg.RequestConcurrency
	bootstrapCfg.RequestTimeout = cfg.RequestTimeout

	bootstrap, err := routing.NewBootstrap[kadt.Key](kadt.PeerID(self), bootstrapCfg)
	if err != nil {
		return nil, fmt.Errorf("bootstrap: %w", err)
	}

	includeCfg := routing.DefaultIncludeConfig()
	includeCfg.Clock = cfg.Clock
	includeCfg.Timeout = cfg.QueryTimeout

	// TODO: expose config
	// includeCfg.QueueCapacity = cfg.IncludeQueueCapacity
	// includeCfg.Concurrency = cfg.IncludeConcurrency
	// includeCfg.Timeout = cfg.IncludeTimeout

	include, err := routing.NewInclude[kadt.Key, kadt.PeerID](rt, includeCfg)
	if err != nil {
		return nil, fmt.Errorf("include: %w", err)
	}

	probeCfg := routing.DefaultProbeConfig()
	probeCfg.Clock = cfg.Clock
	probeCfg.Timeout = cfg.QueryTimeout

	// TODO: expose config
	// probeCfg.Concurrency = cfg.ProbeConcurrency
	probe, err := routing.NewProbe[kadt.Key](rt, probeCfg)
	if err != nil {
		return nil, fmt.Errorf("probe: %w", err)
	}

	routingBehaviour := NewRoutingBehaviour(self, bootstrap, include, probe, cfg.Logger, tele.Tracer)

	networkBehaviour := NewNetworkBehaviour(rtr, cfg.Logger, tele.Tracer)

	ctx, cancel := context.WithCancel(context.Background())

	d := &Coordinator{
		self:   self,
		tele:   tele,
		cfg:    *cfg,
		rtr:    rtr,
		rt:     rt,
		cancel: cancel,

		networkBehaviour: networkBehaviour,
		routingBehaviour: routingBehaviour,
		queryBehaviour:   queryBehaviour,

		routingNotifications: make(chan RoutingNotification, 20), // buffered mainly to allow tests to read the channel after running an operation
	}
	go d.eventLoop(ctx)

	return d, nil
}

// Close cleans up all resources associated with this Coordinator.
func (c *Coordinator) Close() error {
	c.cancel()
	return nil
}

func (c *Coordinator) ID() peer.ID {
	return c.self
}

func (c *Coordinator) Addresses() []ma.Multiaddr {
	// TODO: return configured listen addresses
	info, err := c.rtr.GetNodeInfo(context.TODO(), c.self)
	if err != nil {
		return nil
	}
	return info.Addrs
}

// RoutingNotifications returns a channel that may be read to be notified of routing updates
func (c *Coordinator) RoutingNotifications() <-chan RoutingNotification {
	return c.routingNotifications
}

func (c *Coordinator) eventLoop(ctx context.Context) {
	ctx, span := c.tele.Tracer.Start(ctx, "Coordinator.eventLoop")
	defer span.End()
	for {
		var ev BehaviourEvent
		var ok bool
		select {
		case <-ctx.Done():
			// coordinator is closing
			return
		case <-c.networkBehaviour.Ready():
			ev, ok = c.networkBehaviour.Perform(ctx)
		case <-c.routingBehaviour.Ready():
			ev, ok = c.routingBehaviour.Perform(ctx)
		case <-c.queryBehaviour.Ready():
			ev, ok = c.queryBehaviour.Perform(ctx)
		}

		if ok {
			c.dispatchEvent(ctx, ev)
		}
	}
}

func (c *Coordinator) dispatchEvent(ctx context.Context, ev BehaviourEvent) {
	ctx, span := c.tele.Tracer.Start(ctx, "Coordinator.dispatchEvent", trace.WithAttributes(attribute.String("event_type", fmt.Sprintf("%T", ev))))
	defer span.End()

	switch ev := ev.(type) {
	case NetworkCommand:
		c.networkBehaviour.Notify(ctx, ev)
	case QueryCommand:
		c.queryBehaviour.Notify(ctx, ev)
	case RoutingCommand:
		c.routingBehaviour.Notify(ctx, ev)
	case RoutingNotification:
		select {
		case <-ctx.Done():
		case c.routingNotifications <- ev:
		default:
		}
	default:
		panic(fmt.Sprintf("unexpected event: %T", ev))
	}
}

// GetNode retrieves the node associated with the given node id from the DHT's local routing table.
// If the node isn't found in the table, it returns ErrNodeNotFound.
func (c *Coordinator) GetNode(ctx context.Context, id peer.ID) (Node, error) {
	if _, exists := c.rt.GetNode(kadt.PeerID(id).Key()); !exists {
		return nil, ErrNodeNotFound
	}

	nh, err := c.networkBehaviour.getNodeHandler(ctx, id)
	if err != nil {
		return nil, err
	}
	return nh, nil
}

// GetClosestNodes requests the n closest nodes to the key from the node's local routing table.
func (c *Coordinator) GetClosestNodes(ctx context.Context, k kadt.Key, n int) ([]Node, error) {
	closest := c.rt.NearestNodes(k, n)
	nodes := make([]Node, 0, len(closest))
	for _, id := range closest {
		nh, err := c.networkBehaviour.getNodeHandler(ctx, peer.ID(id))
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, nh)
	}
	return nodes, nil
}

// GetValue requests that the node return any value associated with the supplied key.
// If the node does not have a value for the key it returns ErrValueNotFound.
func (c *Coordinator) GetValue(ctx context.Context, k kadt.Key) (Value, error) {
	panic("not implemented")
}

// PutValue requests that the node stores a value to be associated with the supplied key.
// If the node cannot or chooses not to store the value for the key it returns ErrValueNotAccepted.
func (c *Coordinator) PutValue(ctx context.Context, r Value, q int) error {
	panic("not implemented")
}

// Query traverses the DHT calling fn for each node visited.
func (c *Coordinator) Query(ctx context.Context, target kadt.Key, fn QueryFunc) (QueryStats, error) {
	ctx, span := c.tele.Tracer.Start(ctx, "Coordinator.Query")
	defer span.End()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	seeds, err := c.GetClosestNodes(ctx, target, 20)
	if err != nil {
		return QueryStats{}, err
	}

	seedIDs := make([]peer.ID, 0, len(seeds))
	for _, s := range seeds {
		seedIDs = append(seedIDs, s.ID())
	}

	waiter := NewWaiter[BehaviourEvent]()
	queryID := query.QueryID("foo") // TODO: choose query ID

	cmd := &EventStartQuery{
		QueryID:           queryID,
		Target:            target,
		ProtocolID:        address.ProtocolID("TODO"),
		Message:           &fakeMessage{key: target},
		KnownClosestNodes: seedIDs,
		Notify:            waiter,
	}

	// queue the start of the query
	c.queryBehaviour.Notify(ctx, cmd)

	var lastStats QueryStats
	for {
		select {
		case <-ctx.Done():
			return lastStats, ctx.Err()
		case wev := <-waiter.Chan():
			ctx, ev := wev.Ctx, wev.Event
			switch ev := ev.(type) {
			case *EventQueryProgressed:
				lastStats = QueryStats{
					Start:    ev.Stats.Start,
					Requests: ev.Stats.Requests,
					Success:  ev.Stats.Success,
					Failure:  ev.Stats.Failure,
				}
				nh, err := c.networkBehaviour.getNodeHandler(ctx, ev.NodeID)
				if err != nil {
					// ignore unknown node
					break
				}

				err = fn(ctx, nh, lastStats)
				if errors.Is(err, ErrSkipRemaining) {
					// done
					c.queryBehaviour.Notify(ctx, &EventStopQuery{QueryID: queryID})
					return lastStats, nil
				}
				if errors.Is(err, ErrSkipNode) {
					// TODO: don't add closer nodes from this node
					break
				}
				if err != nil {
					// user defined error that terminates the query
					c.queryBehaviour.Notify(ctx, &EventStopQuery{QueryID: queryID})
					return lastStats, err
				}

			case *EventQueryFinished:
				// query is done
				lastStats.Exhausted = true
				return lastStats, nil

			default:
				panic(fmt.Sprintf("unexpected event: %T", ev))
			}
		}
	}
}

// AddNodes suggests new DHT nodes and their associated addresses to be added to the routing table.
// If the routing table is updated as a result of this operation an EventRoutingUpdated notification
// is emitted on the routing notification channel.
func (c *Coordinator) AddNodes(ctx context.Context, ais []peer.AddrInfo) error {
	ctx, span := c.tele.Tracer.Start(ctx, "Coordinator.AddNodes")
	defer span.End()
	for _, ai := range ais {
		if ai.ID == c.self {
			// skip self
			continue
		}

		// TODO: apply address filter

		c.routingBehaviour.Notify(ctx, &EventAddAddrInfo{
			NodeInfo: ai,
		})

	}

	return nil
}

// Bootstrap instructs the dht to begin bootstrapping the routing table.
func (c *Coordinator) Bootstrap(ctx context.Context, seeds []peer.ID) error {
	ctx, span := c.tele.Tracer.Start(ctx, "Coordinator.Bootstrap")
	defer span.End()
	c.routingBehaviour.Notify(ctx, &EventStartBootstrap{
		// Bootstrap state machine uses the message
		Message:   &fakeMessage{key: kadt.PeerID(c.self).Key()},
		SeedNodes: seeds,
	})

	return nil
}
