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
	"github.com/plprobelab/go-kademlia/key"
	"github.com/plprobelab/go-kademlia/network/address"
	"github.com/plprobelab/go-kademlia/query"
	"github.com/plprobelab/go-kademlia/routing"
	"github.com/plprobelab/go-kademlia/util"
	"go.uber.org/zap/exp/zapslog"
	"golang.org/x/exp/slog"

	"github.com/libp2p/go-libp2p-kad-dht/v2/kadt"
)

// A Coordinator coordinates the state machines that comprise a Kademlia DHT
// It is only one possible configuration of the DHT components, others are possible.
type Coordinator struct {
	// self is the node id of the system the dht is running on
	self peer.ID

	// cfg is a copy of the optional configuration supplied to the dht
	cfg Config

	// rt is the routing table used to look up nodes by distance
	rt kad.RoutingTable[key.Key256, kad.NodeID[key.Key256]]

	// rtr is the message router used to send messages
	rtr Router

	routingNotifications chan RoutingNotification

	// networkBehaviour is the behaviour responsible for communicating with the network
	networkBehaviour *NetworkBehaviour

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

func NewCoordinator(self peer.ID, rtr Router, rt routing.RoutingTableCpl[key.Key256, kad.NodeID[key.Key256]], cfg *Config) (*Coordinator, error) {
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

	qp, err := query.NewPool[key.Key256, ma.Multiaddr](kadt.PeerID(self), qpCfg)
	if err != nil {
		return nil, fmt.Errorf("query pool: %w", err)
	}
	queryBehaviour := NewPooledQueryBehaviour(qp, cfg.Logger)

	bootstrapCfg := routing.DefaultBootstrapConfig[key.Key256, ma.Multiaddr]()
	bootstrapCfg.Clock = cfg.Clock
	bootstrapCfg.Timeout = cfg.QueryTimeout
	bootstrapCfg.RequestConcurrency = cfg.RequestConcurrency
	bootstrapCfg.RequestTimeout = cfg.RequestTimeout

	bootstrap, err := routing.NewBootstrap[key.Key256, ma.Multiaddr](kadt.PeerID(self), bootstrapCfg)
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

	include, err := routing.NewInclude[key.Key256, ma.Multiaddr](rt, includeCfg)
	if err != nil {
		return nil, fmt.Errorf("include: %w", err)
	}

	probeCfg := routing.DefaultProbeConfig()
	probeCfg.Clock = cfg.Clock
	probeCfg.Timeout = cfg.QueryTimeout

	// TODO: expose config
	// probeCfg.Concurrency = cfg.ProbeConcurrency
	probe, err := routing.NewProbe[key.Key256, ma.Multiaddr](rt, probeCfg)
	if err != nil {
		return nil, fmt.Errorf("probe: %w", err)
	}

	routingBehaviour := NewRoutingBehaviour(self, bootstrap, include, probe, cfg.Logger)

	networkBehaviour := NewNetworkBehaviour(rtr, cfg.Logger)

	d := &Coordinator{
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

func (d *Coordinator) ID() peer.ID {
	return d.self
}

func (d *Coordinator) Addresses() []ma.Multiaddr {
	// TODO: return configured listen addresses
	info, err := d.rtr.GetNodeInfo(context.TODO(), d.self)
	if err != nil {
		return nil
	}
	return info.Addrs
}

// RoutingNotifications returns a channel that may be read to be notified of routing updates
func (d *Coordinator) RoutingNotifications() <-chan RoutingNotification {
	return d.routingNotifications
}

func (d *Coordinator) eventLoop() {
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

func (c *Coordinator) dispatchDhtNotice(ctx context.Context, ev DhtEvent) {
	ctx, span := util.StartSpan(ctx, "Coordinator.dispatchDhtNotice")
	defer span.End()

	switch ev := ev.(type) {
	case *EventDhtStartBootstrap:
		c.routingBehaviour.Notify(ctx, ev)
	case *EventOutboundGetClosestNodes:
		c.networkBehaviour.Notify(ctx, ev)
	case *EventStartQuery:
		c.queryBehaviour.Notify(ctx, ev)
	case *EventStopQuery:
		c.queryBehaviour.Notify(ctx, ev)
	case *EventDhtAddNodeInfo:
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
func (d *Coordinator) GetNode(ctx context.Context, id peer.ID) (Node, error) {
	if _, exists := d.rt.GetNode(kadt.PeerID(id).Key()); !exists {
		return nil, ErrNodeNotFound
	}

	nh, err := d.networkBehaviour.getNodeHandler(ctx, id)
	if err != nil {
		return nil, err
	}
	return nh, nil
}

// GetClosestNodes requests the n closest nodes to the key from the node's local routing table.
func (d *Coordinator) GetClosestNodes(ctx context.Context, k key.Key256, n int) ([]Node, error) {
	closest := d.rt.NearestNodes(k, n)
	nodes := make([]Node, 0, len(closest))
	for _, id := range closest {
		nh, err := d.networkBehaviour.getNodeHandler(ctx, NodeIDToPeerID(id))
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, nh)
	}
	return nodes, nil
}

// GetValue requests that the node return any value associated with the supplied key.
// If the node does not have a value for the key it returns ErrValueNotFound.
func (d *Coordinator) GetValue(ctx context.Context, k key.Key256) (Value, error) {
	panic("not implemented")
}

// PutValue requests that the node stores a value to be associated with the supplied key.
// If the node cannot or chooses not to store the value for the key it returns ErrValueNotAccepted.
func (d *Coordinator) PutValue(ctx context.Context, r Value, q int) error {
	panic("not implemented")
}

// Query traverses the DHT calling fn for each node visited.
func (d *Coordinator) Query(ctx context.Context, target key.Key256, fn QueryFunc) (QueryStats, error) {
	ctx, span := util.StartSpan(ctx, "Coordinator.Query")
	defer span.End()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	seeds, err := d.GetClosestNodes(ctx, target, 20)
	if err != nil {
		return QueryStats{}, err
	}

	seedIDs := make([]peer.ID, 0, len(seeds))
	for _, s := range seeds {
		seedIDs = append(seedIDs, s.ID())
	}

	waiter := NewWaiter[DhtEvent]()
	queryID := query.QueryID("foo")

	cmd := &EventStartQuery{
		QueryID:           queryID,
		Target:            target,
		ProtocolID:        address.ProtocolID("TODO"),
		Message:           &fakeMessage{key: target},
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
			case *EventQueryProgressed:
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
func (d *Coordinator) AddNodes(ctx context.Context, infos []peer.AddrInfo) error {
	ctx, span := util.StartSpan(ctx, "Coordinator.AddNodes")
	defer span.End()
	for _, info := range infos {
		if info.ID == d.self {
			// skip self
			continue
		}

		d.routingBehaviour.Notify(ctx, &EventDhtAddNodeInfo{
			NodeInfo: info,
		})

	}

	return nil
}

// Bootstrap instructs the dht to begin bootstrapping the routing table.
func (d *Coordinator) Bootstrap(ctx context.Context, seeds []peer.ID) error {
	ctx, span := util.StartSpan(ctx, "Coordinator.Bootstrap")
	defer span.End()
	d.routingBehaviour.Notify(ctx, &EventDhtStartBootstrap{
		// Bootstrap state machine uses the message
		Message:   &fakeMessage{key: kadt.PeerID(d.self).Key()},
		SeedNodes: seeds,
	})

	return nil
}
