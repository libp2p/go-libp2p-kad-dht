package kademlia

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/benbjohnson/clock"
	logging "github.com/ipfs/go-log/v2"
	"github.com/plprobelab/go-kademlia/kad"
	"github.com/plprobelab/go-kademlia/kaderr"
	"github.com/plprobelab/go-kademlia/key"
	"github.com/plprobelab/go-kademlia/network/address"
	"github.com/plprobelab/go-kademlia/query"
	"github.com/plprobelab/go-kademlia/routing"
	"github.com/plprobelab/go-kademlia/util"
	"go.uber.org/zap/exp/zapslog"
	"golang.org/x/exp/slog"
)

// A Coordinator coordinates the state machines that comprise a Kademlia DHT
// It is only one possible configuration of the DHT components, others are possible.
type Coordinator[K kad.Key[K], A kad.Address[A]] struct {
	// self is the node id of the system the dht is running on
	self kad.NodeID[K]

	// cfg is a copy of the optional configuration supplied to the dht
	cfg Config

	// rt is the routing table used to look up nodes by distance
	rt kad.RoutingTable[K, kad.NodeID[K]]

	// rtr is the message router used to send messages
	rtr Router[K, A]

	routingNotifications chan RoutingNotification

	// networkBehaviour is the behaviour responsible for communicating with the network
	networkBehaviour *NetworkBehaviour[K, A]

	// routingBehaviour is the behaviour responsible for maintaining the routing table
	routingBehaviour Behaviour[DhtEvent, DhtEvent]

	// queryBehaviour is the behaviour responsible for running user-submitted queries
	queryBehaviour Behaviour[DhtEvent, DhtEvent]
}

const DefaultChanqueueCapacity = 1024

type Config struct {
	PeerstoreTTL time.Duration // duration for which a peer is kept in the peerstore

	Clock clock.Clock // a clock that may replaced by a mock when testing

	QueryConcurrency int           // the maximum number of queries that may be waiting for message responses at any one time
	QueryTimeout     time.Duration // the time to wait before terminating a query that is not making progress

	RequestConcurrency int           // the maximum number of concurrent requests that each query may have in flight
	RequestTimeout     time.Duration // the timeout queries should use for contacting a single node

	Logger *slog.Logger // a structured logger that should be used when logging.
}

// Validate checks the configuration options and returns an error if any have invalid values.
func (cfg *Config) Validate() error {
	if cfg.Clock == nil {
		return &kaderr.ConfigurationError{
			Component: "DhtConfig",
			Err:       fmt.Errorf("clock must not be nil"),
		}
	}

	if cfg.QueryConcurrency < 1 {
		return &kaderr.ConfigurationError{
			Component: "DhtConfig",
			Err:       fmt.Errorf("query concurrency must be greater than zero"),
		}
	}
	if cfg.QueryTimeout < 1 {
		return &kaderr.ConfigurationError{
			Component: "DhtConfig",
			Err:       fmt.Errorf("query timeout must be greater than zero"),
		}
	}

	if cfg.RequestConcurrency < 1 {
		return &kaderr.ConfigurationError{
			Component: "DhtConfig",
			Err:       fmt.Errorf("request concurrency must be greater than zero"),
		}
	}

	if cfg.RequestTimeout < 1 {
		return &kaderr.ConfigurationError{
			Component: "DhtConfig",
			Err:       fmt.Errorf("request timeout must be greater than zero"),
		}
	}

	if cfg.Logger == nil {
		return &kaderr.ConfigurationError{
			Component: "DhtConfig",
			Err:       fmt.Errorf("logger must not be nil"),
		}
	}
	return nil
}

func DefaultConfig() *Config {
	return &Config{
		Clock:              clock.New(), // use standard time
		PeerstoreTTL:       10 * time.Minute,
		QueryConcurrency:   3,
		QueryTimeout:       5 * time.Minute,
		RequestConcurrency: 3,
		RequestTimeout:     time.Minute,
		Logger:             slog.New(zapslog.NewHandler(logging.Logger("dht").Desugar().Core())),
	}
}

func NewDht[K kad.Key[K], A kad.Address[A]](self kad.NodeID[K], rtr Router[K, A], rt routing.RoutingTableCpl[K, kad.NodeID[K]], cfg *Config) (*Coordinator[K, A], error) {
	if cfg == nil {
		cfg = DefaultConfig()
	} else if err := cfg.Validate(); err != nil {
		return nil, err
	}

	qpCfg := query.DefaultPoolConfig()
	qpCfg.Clock = cfg.Clock
	qpCfg.Concurrency = cfg.QueryConcurrency
	qpCfg.Timeout = cfg.QueryTimeout
	qpCfg.QueryConcurrency = cfg.RequestConcurrency
	qpCfg.RequestTimeout = cfg.RequestTimeout

	qp, err := query.NewPool[K, A](self, qpCfg)
	if err != nil {
		return nil, fmt.Errorf("query pool: %w", err)
	}
	queryBehaviour := NewPooledQueryBehaviour(qp, cfg.Logger)

	bootstrapCfg := routing.DefaultBootstrapConfig[K, A]()
	bootstrapCfg.Clock = cfg.Clock
	bootstrapCfg.Timeout = cfg.QueryTimeout
	bootstrapCfg.RequestConcurrency = cfg.RequestConcurrency
	bootstrapCfg.RequestTimeout = cfg.RequestTimeout

	bootstrap, err := routing.NewBootstrap(self, bootstrapCfg)
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

	include, err := routing.NewInclude[K, A](rt, includeCfg)
	if err != nil {
		return nil, fmt.Errorf("include: %w", err)
	}

	probeCfg := routing.DefaultProbeConfig()
	probeCfg.Clock = cfg.Clock
	probeCfg.Timeout = cfg.QueryTimeout

	// TODO: expose config
	// probeCfg.Concurrency = cfg.ProbeConcurrency
	probe, err := routing.NewProbe[K, A](rt, probeCfg)
	if err != nil {
		return nil, fmt.Errorf("include: %w", err)
	}

	routingBehaviour := NewRoutingBehaviour[K, A](self, bootstrap, include, probe, cfg.Logger)

	networkBehaviour := NewNetworkBehaviour(rtr, cfg.Logger)

	d := &Coordinator[K, A]{
		self: self,
		cfg:  *cfg,
		rtr:  rtr,
		rt:   rt,

		networkBehaviour: networkBehaviour,
		routingBehaviour: routingBehaviour,
		queryBehaviour:   queryBehaviour,

		routingNotifications: make(chan RoutingNotification, 20),
	}
	go d.eventLoop()

	return d, nil
}

func (d *Coordinator[K, A]) ID() kad.NodeID[K] {
	return d.self
}

func (d *Coordinator[K, A]) Addresses() []A {
	// TODO: return configured listen addresses
	info, err := d.rtr.GetNodeInfo(context.TODO(), d.self)
	if err != nil {
		return nil
	}
	return info.Addresses()
}

// RoutingNotifications returns a channel that may be read to be notified of routing updates
func (d *Coordinator[K, A]) RoutingNotifications() <-chan RoutingNotification {
	return d.routingNotifications
}

func (d *Coordinator[K, A]) eventLoop() {
	ctx := context.Background()

	for {
		var ev DhtEvent
		var ok bool
		select {
		case <-d.networkBehaviour.Ready():
			ev, ok = d.networkBehaviour.Perform(ctx)
		case <-d.routingBehaviour.Ready():
			ev, ok = d.routingBehaviour.Perform(ctx)
		case <-d.queryBehaviour.Ready():
			ev, ok = d.queryBehaviour.Perform(ctx)
		}

		if ok {
			d.dispatchDhtNotice(ctx, ev)
		}
	}
}

func (c *Coordinator[K, A]) dispatchDhtNotice(ctx context.Context, ev DhtEvent) {
	ctx, span := util.StartSpan(ctx, "Dht.dispatchDhtNotice")
	defer span.End()

	switch ev := ev.(type) {
	case *EventDhtStartBootstrap[K, A]:
		c.routingBehaviour.Notify(ctx, ev)
	case *EventOutboundGetClosestNodes[K, A]:
		c.networkBehaviour.Notify(ctx, ev)
	case *EventStartQuery[K, A]:
		c.queryBehaviour.Notify(ctx, ev)
	case *EventStopQuery:
		c.queryBehaviour.Notify(ctx, ev)
	case *EventDhtAddNodeInfo[K, A]:
		c.routingBehaviour.Notify(ctx, ev)
	case RoutingNotification:
		select {
		case <-ctx.Done():
		case c.routingNotifications <- ev:
		default:
		}
	}
}

// GetNode retrieves the node associated with the given node id from the DHT's local routing table.
// If the node isn't found in the table, it returns ErrNodeNotFound.
func (d *Coordinator[K, A]) GetNode(ctx context.Context, id kad.NodeID[K]) (Node[K, A], error) {
	if _, exists := d.rt.GetNode(id.Key()); !exists {
		return nil, ErrNodeNotFound
	}

	nh, err := d.networkBehaviour.getNodeHandler(ctx, id)
	if err != nil {
		return nil, err
	}
	return nh, nil
}

// GetClosestNodes requests the n closest nodes to the key from the node's local routing table.
func (d *Coordinator[K, A]) GetClosestNodes(ctx context.Context, k K, n int) ([]Node[K, A], error) {
	closest := d.rt.NearestNodes(k, n)
	nodes := make([]Node[K, A], 0, len(closest))
	for _, id := range closest {
		nh, err := d.networkBehaviour.getNodeHandler(ctx, id)
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, nh)
	}
	return nodes, nil
}

// GetValue requests that the node return any value associated with the supplied key.
// If the node does not have a value for the key it returns ErrValueNotFound.
func (d *Coordinator[K, A]) GetValue(ctx context.Context, key K) (Value[K], error) {
	panic("not implemented")
}

// PutValue requests that the node stores a value to be associated with the supplied key.
// If the node cannot or chooses not to store the value for the key it returns ErrValueNotAccepted.
func (d *Coordinator[K, A]) PutValue(ctx context.Context, r Value[K], q int) error {
	panic("not implemented")
}

// Query traverses the DHT calling fn for each node visited.
func (d *Coordinator[K, A]) Query(ctx context.Context, target K, fn QueryFunc[K, A]) (QueryStats, error) {
	ctx, span := util.StartSpan(ctx, "Dht.Query")
	defer span.End()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	seeds, err := d.GetClosestNodes(ctx, target, 20)
	if err != nil {
		return QueryStats{}, err
	}

	seedIDs := make([]kad.NodeID[K], 0, len(seeds))
	for _, s := range seeds {
		seedIDs = append(seedIDs, s.ID())
	}

	waiter := NewWaiter[DhtEvent]()
	queryID := query.QueryID("foo")

	cmd := &EventStartQuery[K, A]{
		QueryID:           queryID,
		Target:            target,
		ProtocolID:        address.ProtocolID("TODO"),
		Message:           &fakeMessage[K, A]{key: target},
		KnownClosestNodes: seedIDs,
		Notify:            waiter,
	}

	// queue the start of the query
	d.queryBehaviour.Notify(ctx, cmd)

	var lastStats QueryStats
	for {
		select {
		case <-ctx.Done():
			return lastStats, ctx.Err()
		case wev := <-waiter.Chan():
			ctx, ev := wev.Ctx, wev.Event
			switch ev := ev.(type) {
			case *EventQueryProgressed[K, A]:
				lastStats = QueryStats{
					Start:    ev.Stats.Start,
					Requests: ev.Stats.Requests,
					Success:  ev.Stats.Success,
					Failure:  ev.Stats.Failure,
				}
				nh, err := d.networkBehaviour.getNodeHandler(ctx, ev.NodeID)
				if err != nil {
					// ignore unknown node
					break
				}

				err = fn(ctx, nh, lastStats)
				if errors.Is(err, SkipRemaining) {
					// done
					d.queryBehaviour.Notify(ctx, &EventStopQuery{QueryID: queryID})
					return lastStats, nil
				}
				if errors.Is(err, SkipNode) {
					// TODO: don't add closer nodes from this node
					break
				}
				if err != nil {
					// user defined error that terminates the query
					d.queryBehaviour.Notify(ctx, &EventStopQuery{QueryID: queryID})
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
func (d *Coordinator[K, A]) AddNodes(ctx context.Context, infos []kad.NodeInfo[K, A]) error {
	ctx, span := util.StartSpan(ctx, "Dht.AddNodes")
	defer span.End()
	for _, info := range infos {
		if key.Equal(info.ID().Key(), d.self.Key()) {
			// skip self
			continue
		}

		d.routingBehaviour.Notify(ctx, &EventDhtAddNodeInfo[K, A]{
			NodeInfo: info,
		})

	}

	return nil
}

// Bootstrap instructs the dht to begin bootstrapping the routing table.
func (d *Coordinator[K, A]) Bootstrap(ctx context.Context, seeds []kad.NodeID[K]) error {
	ctx, span := util.StartSpan(ctx, "Dht.Bootstrap")
	defer span.End()
	d.routingBehaviour.Notify(ctx, &EventDhtStartBootstrap[K, A]{
		// Bootstrap state machine uses the message
		Message:   &fakeMessage[K, A]{key: d.self.Key()},
		SeedNodes: seeds,
	})

	return nil
}
