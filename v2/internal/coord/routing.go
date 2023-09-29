package coord

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/benbjohnson/clock"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/exp/slog"

	"github.com/libp2p/go-libp2p-kad-dht/v2/errs"
	"github.com/libp2p/go-libp2p-kad-dht/v2/internal/coord/coordt"
	"github.com/libp2p/go-libp2p-kad-dht/v2/internal/coord/cplutil"
	"github.com/libp2p/go-libp2p-kad-dht/v2/internal/coord/routing"
	"github.com/libp2p/go-libp2p-kad-dht/v2/kadt"
	"github.com/libp2p/go-libp2p-kad-dht/v2/tele"
)

const (
	// IncludeQueryID is the id for connectivity checks performed by the include state machine.
	// This identifier is used for routing network responses to the state machine.
	IncludeQueryID = coordt.QueryID("include")

	// ProbeQueryID is the id for connectivity checks performed by the probe state machine
	// This identifier is used for routing network responses to the state machine.
	ProbeQueryID = coordt.QueryID("probe")
)

type RoutingConfig struct {
	// Clock is a clock that may replaced by a mock when testing
	Clock clock.Clock

	// Logger is a structured logger that will be used when logging.
	Logger *slog.Logger

	// Tracer is the tracer that should be used to trace execution.
	Tracer trace.Tracer

	// Meter is the meter that should be used to record metrics.
	Meter metric.Meter

	// BootstrapTimeout is the time the behaviour should wait before terminating a bootstrap if it is not making progress.
	BootstrapTimeout time.Duration

	// BootstrapRequestConcurrency is the maximum number of concurrent requests that the behaviour may have in flight during bootstrap.
	BootstrapRequestConcurrency int

	// BootstrapRequestTimeout is the timeout the behaviour should use when attempting to contact a node during bootstrap.
	BootstrapRequestTimeout time.Duration

	// ConnectivityCheckTimeout is the timeout the behaviour should use when performing a connectivity check.
	ConnectivityCheckTimeout time.Duration

	// ProbeRequestConcurrency is the maximum number of concurrent requests that the behaviour may have in flight while performing
	// connectivity checks for nodes in the routing table.
	ProbeRequestConcurrency int

	// ProbeCheckInterval is the time interval the behaviour should use between connectivity checks for the same node in the routing table.
	ProbeCheckInterval time.Duration

	// IncludeSkipCheck indicates whether we perform connectivity checks before we add a peer to the routing table.
	IncludeSkipCheck bool

	// IncludeQueueCapacity is the maximum number of nodes the behaviour should keep queued as candidates for inclusion in the routing table.
	IncludeQueueCapacity int

	// IncludeRequestConcurrency is the maximum number of concurrent requests that the behaviour may have in flight while performing
	// connectivity checks for nodes in the inclusion candidate queue.
	IncludeRequestConcurrency int

	// ExploreTimeout is the time the behaviour should wait before terminating an exploration of a routing table bucket if it is not making progress.
	ExploreTimeout time.Duration

	// ExploreRequestConcurrency is the maximum number of concurrent requests that the behaviour may have in flight while exploring the
	// network to increase routing table occupancy.
	ExploreRequestConcurrency int

	// ExploreRequestTimeout is the timeout the behaviour should use when attempting to contact a node while exploring the
	// network to increase routing table occupancy.
	ExploreRequestTimeout time.Duration

	// ExploreMaximumCpl is the maximum CPL (common prefix length) the behaviour should explore to increase routing table occupancy.
	// All CPLs from this value to zero will be explored on a repeating schedule.
	ExploreMaximumCpl int

	// ExploreInterval is the base time interval the behaviour should leave between explorations of the same CPL.
	// See the documentation for [routing.DynamicExploreSchedule] for the precise formula used to calculate explore intervals.
	ExploreInterval time.Duration

	// ExploreIntervalMultiplier is a factor that is applied to the base time interval for CPLs lower than the maximum to increase the delay between
	// explorations for lower CPLs.
	// See the documentation for [routing.DynamicExploreSchedule] for the precise formula used to calculate explore intervals.
	ExploreIntervalMultiplier float64

	// ExploreIntervalJitter is a factor that is used to increase the calculated interval for an exploratiion by a small random amount.
	// It must be between 0 and 0.05. When zero, no jitter is applied.
	// See the documentation for [routing.DynamicExploreSchedule] for the precise formula used to calculate explore intervals.
	ExploreIntervalJitter float64
}

// Validate checks the configuration options and returns an error if any have invalid values.
func (cfg *RoutingConfig) Validate() error {
	if cfg.Clock == nil {
		return &errs.ConfigurationError{
			Component: "RoutingConfig",
			Err:       fmt.Errorf("clock must not be nil"),
		}
	}

	if cfg.Logger == nil {
		return &errs.ConfigurationError{
			Component: "RoutingConfig",
			Err:       fmt.Errorf("logger must not be nil"),
		}
	}

	if cfg.Tracer == nil {
		return &errs.ConfigurationError{
			Component: "RoutingConfig",
			Err:       fmt.Errorf("tracer must not be nil"),
		}
	}

	if cfg.Meter == nil {
		return &errs.ConfigurationError{
			Component: "RoutingConfig",
			Err:       fmt.Errorf("meter must not be nil"),
		}
	}

	if cfg.BootstrapTimeout < 1 {
		return &errs.ConfigurationError{
			Component: "RoutingConfig",
			Err:       fmt.Errorf("bootstrap timeout must be greater than zero"),
		}
	}

	if cfg.BootstrapRequestConcurrency < 1 {
		return &errs.ConfigurationError{
			Component: "RoutingConfig",
			Err:       fmt.Errorf("bootstrap request concurrency must be greater than zero"),
		}
	}

	if cfg.BootstrapRequestTimeout < 1 {
		return &errs.ConfigurationError{
			Component: "RoutingConfig",
			Err:       fmt.Errorf("bootstrap request timeout must be greater than zero"),
		}
	}

	if cfg.ConnectivityCheckTimeout < 1 {
		return &errs.ConfigurationError{
			Component: "RoutingConfig",
			Err:       fmt.Errorf("connectivity check timeout must be greater than zero"),
		}
	}

	if cfg.ProbeRequestConcurrency < 1 {
		return &errs.ConfigurationError{
			Component: "RoutingConfig",
			Err:       fmt.Errorf("probe request concurrency must be greater than zero"),
		}
	}

	if cfg.ProbeCheckInterval < 1 {
		return &errs.ConfigurationError{
			Component: "RoutingConfig",
			Err:       fmt.Errorf("probe check interval must be greater than zero"),
		}
	}

	if cfg.IncludeQueueCapacity < 1 {
		return &errs.ConfigurationError{
			Component: "RoutingConfig",
			Err:       fmt.Errorf("include queue capacity must be greater than zero"),
		}
	}

	if cfg.IncludeRequestConcurrency < 1 {
		return &errs.ConfigurationError{
			Component: "RoutingConfig",
			Err:       fmt.Errorf("include request concurrency must be greater than zero"),
		}
	}

	if cfg.ExploreTimeout < 1 {
		return &errs.ConfigurationError{
			Component: "RoutingConfig",
			Err:       fmt.Errorf("explore timeout must be greater than zero"),
		}
	}

	if cfg.ExploreRequestConcurrency < 1 {
		return &errs.ConfigurationError{
			Component: "RoutingConfig",
			Err:       fmt.Errorf("explore request concurrency must be greater than zero"),
		}
	}

	if cfg.ExploreRequestTimeout < 1 {
		return &errs.ConfigurationError{
			Component: "RoutingConfig",
			Err:       fmt.Errorf("explore request timeout must be greater than zero"),
		}
	}

	if cfg.ExploreMaximumCpl < 1 {
		return &errs.ConfigurationError{
			Component: "RoutingConfig",
			Err:       fmt.Errorf("explore maximum cpl must be greater than zero"),
		}
	}

	// This limit exists because we can only generate 15 bit prefixes [cplutil.GenRandPeerID].
	if cfg.ExploreMaximumCpl > 15 {
		return &errs.ConfigurationError{
			Component: "RoutingConfig",
			Err:       fmt.Errorf("explore maximum cpl must be 15 or less"),
		}
	}

	if cfg.ExploreInterval < 1 {
		return &errs.ConfigurationError{
			Component: "RoutingConfig",
			Err:       fmt.Errorf("explore interval must be greater than zero"),
		}
	}

	if cfg.ExploreIntervalMultiplier < 1 {
		return &errs.ConfigurationError{
			Component: "RoutingConfig",
			Err:       fmt.Errorf("explore interval multiplier must be one or greater"),
		}
	}

	if cfg.ExploreIntervalJitter < 0 {
		return &errs.ConfigurationError{
			Component: "RoutingConfig",
			Err:       fmt.Errorf("explore interval jitter must be greater than 0"),
		}
	}

	if cfg.ExploreIntervalJitter > 0.05 {
		return &errs.ConfigurationError{
			Component: "RoutingConfig",
			Err:       fmt.Errorf("explore interval jitter must be 0.05 or less"),
		}
	}

	return nil
}

func DefaultRoutingConfig() *RoutingConfig {
	return &RoutingConfig{
		Clock:  clock.New(),
		Logger: tele.DefaultLogger("coord"),
		Tracer: tele.NoopTracer(),
		Meter:  tele.NoopMeter(),

		BootstrapTimeout:            5 * time.Minute, // MAGIC
		BootstrapRequestConcurrency: 3,               // MAGIC
		BootstrapRequestTimeout:     time.Minute,     // MAGIC

		ConnectivityCheckTimeout: time.Minute, // MAGIC

		ProbeRequestConcurrency: 3,             // MAGIC
		ProbeCheckInterval:      6 * time.Hour, // MAGIC

		IncludeSkipCheck:          false,
		IncludeRequestConcurrency: 3,   // MAGIC
		IncludeQueueCapacity:      128, // MAGIC

		ExploreTimeout:            5 * time.Minute, // MAGIC
		ExploreRequestConcurrency: 3,               // MAGIC
		ExploreRequestTimeout:     time.Minute,     // MAGIC
		ExploreMaximumCpl:         14,
		ExploreInterval:           time.Hour, // MAGIC
		ExploreIntervalMultiplier: 1,         // MAGIC
		ExploreIntervalJitter:     0,         // MAGIC

	}
}

// A RoutingBehaviour provides the behaviours for bootstrapping and maintaining a DHT's routing table.
type RoutingBehaviour struct {
	// self is the peer id of the system the dht is running on
	self kadt.PeerID

	// cfg is a copy of the optional configuration supplied to the behaviour
	cfg RoutingConfig

	// bootstrap is the bootstrap state machine, responsible for bootstrapping the routing table
	bootstrap coordt.StateMachine[routing.BootstrapEvent, routing.BootstrapState]

	// include is the inclusion state machine, responsible for vetting nodes before including them in the routing table
	include coordt.StateMachine[routing.IncludeEvent, routing.IncludeState]

	// probe is the node probing state machine, responsible for periodically checking connectivity of nodes in the routing table
	probe coordt.StateMachine[routing.ProbeEvent, routing.ProbeState]

	// explore is the routing table explore state machine, responsible for increasing the occupanct of the routing table
	explore coordt.StateMachine[routing.ExploreEvent, routing.ExploreState]

	pendingMu sync.Mutex
	pending   []BehaviourEvent
	ready     chan struct{}
}

type Recording2SM[E any, S any] struct {
	State    S
	Received E
}

func NewRecording2SM[E any, S any](response S) *Recording2SM[E, S] {
	return &Recording2SM[E, S]{
		State: response,
	}
}

func (r *Recording2SM[E, S]) Advance(ctx context.Context, e E) S {
	r.Received = e
	return r.State
}

func NewRoutingBehaviour(self kadt.PeerID, rt routing.RoutingTableCpl[kadt.Key, kadt.PeerID], cfg *RoutingConfig) (*RoutingBehaviour, error) {
	if cfg == nil {
		cfg = DefaultRoutingConfig()
	} else if err := cfg.Validate(); err != nil {
		return nil, err
	}

	bootstrapCfg := routing.DefaultBootstrapConfig()
	bootstrapCfg.Clock = cfg.Clock
	bootstrapCfg.Tracer = cfg.Tracer
	bootstrapCfg.Meter = cfg.Meter
	bootstrapCfg.Timeout = cfg.BootstrapTimeout
	bootstrapCfg.RequestConcurrency = cfg.BootstrapRequestConcurrency
	bootstrapCfg.RequestTimeout = cfg.BootstrapRequestTimeout

	bootstrap, err := routing.NewBootstrap[kadt.Key](self, bootstrapCfg)
	if err != nil {
		return nil, fmt.Errorf("bootstrap: %w", err)
	}

	includeCfg := routing.DefaultIncludeConfig()
	includeCfg.Clock = cfg.Clock
	includeCfg.Tracer = cfg.Tracer
	includeCfg.Meter = cfg.Meter
	includeCfg.SkipCheck = cfg.IncludeSkipCheck
	includeCfg.Timeout = cfg.ConnectivityCheckTimeout
	includeCfg.QueueCapacity = cfg.IncludeQueueCapacity
	includeCfg.Concurrency = cfg.IncludeRequestConcurrency

	include, err := routing.NewInclude[kadt.Key, kadt.PeerID](rt, includeCfg)
	if err != nil {
		return nil, fmt.Errorf("include: %w", err)
	}

	probeCfg := routing.DefaultProbeConfig()
	probeCfg.Clock = cfg.Clock
	probeCfg.Tracer = cfg.Tracer
	probeCfg.Meter = cfg.Meter
	probeCfg.Timeout = cfg.ConnectivityCheckTimeout
	probeCfg.Concurrency = cfg.ProbeRequestConcurrency
	probeCfg.CheckInterval = cfg.ProbeCheckInterval

	probe, err := routing.NewProbe[kadt.Key](rt, probeCfg)
	if err != nil {
		return nil, fmt.Errorf("probe: %w", err)
	}

	exploreCfg := routing.DefaultExploreConfig()
	exploreCfg.Clock = cfg.Clock
	exploreCfg.Tracer = cfg.Tracer
	exploreCfg.Meter = cfg.Meter
	exploreCfg.Timeout = cfg.ExploreTimeout
	exploreCfg.RequestConcurrency = cfg.ExploreRequestConcurrency
	exploreCfg.RequestTimeout = cfg.ExploreRequestTimeout

	schedule, err := routing.NewDynamicExploreSchedule(cfg.ExploreMaximumCpl, cfg.Clock.Now(), cfg.ExploreInterval, cfg.ExploreIntervalMultiplier, cfg.ExploreIntervalJitter)
	if err != nil {
		return nil, fmt.Errorf("explore schedule: %w", err)
	}

	explore, err := routing.NewExplore[kadt.Key](self, rt, cplutil.GenRandPeerID, schedule, exploreCfg)
	if err != nil {
		return nil, fmt.Errorf("explore: %w", err)
	}

	return ComposeRoutingBehaviour(self, bootstrap, include, probe, explore, cfg)
}

// ComposeRoutingBehaviour creates a [RoutingBehaviour] composed of the supplied state machines.
// The state machines are assumed to pre-configured so any [RoutingConfig] values relating to the state machines will not be applied.
func ComposeRoutingBehaviour(
	self kadt.PeerID,
	bootstrap coordt.StateMachine[routing.BootstrapEvent, routing.BootstrapState],
	include coordt.StateMachine[routing.IncludeEvent, routing.IncludeState],
	probe coordt.StateMachine[routing.ProbeEvent, routing.ProbeState],
	explore coordt.StateMachine[routing.ExploreEvent, routing.ExploreState],
	cfg *RoutingConfig,
) (*RoutingBehaviour, error) {
	if cfg == nil {
		cfg = DefaultRoutingConfig()
	} else if err := cfg.Validate(); err != nil {
		return nil, err
	}

	r := &RoutingBehaviour{
		self:      self,
		cfg:       *cfg,
		bootstrap: bootstrap,
		include:   include,
		probe:     probe,
		explore:   explore,
		ready:     make(chan struct{}, 1),
	}
	return r, nil
}

func (r *RoutingBehaviour) Notify(ctx context.Context, ev BehaviourEvent) {
	ctx, span := r.cfg.Tracer.Start(ctx, "RoutingBehaviour.Notify")
	defer span.End()

	r.pendingMu.Lock()
	defer r.pendingMu.Unlock()
	r.notify(ctx, ev)
}

// notify must only be called while r.pendingMu is held
func (r *RoutingBehaviour) notify(ctx context.Context, ev BehaviourEvent) {
	ctx, span := r.cfg.Tracer.Start(ctx, "RoutingBehaviour.notify", trace.WithAttributes(attribute.String("event", fmt.Sprintf("%T", ev))))
	defer span.End()

	switch ev := ev.(type) {
	case *EventStartBootstrap:
		span.SetAttributes(attribute.String("event", "EventStartBootstrap"))
		cmd := &routing.EventBootstrapStart[kadt.Key, kadt.PeerID]{
			KnownClosestNodes: ev.SeedNodes,
		}
		// attempt to advance the bootstrap
		next, ok := r.advanceBootstrap(ctx, cmd)
		if ok {
			r.pending = append(r.pending, next)
		}

	case *EventAddNode:
		span.SetAttributes(attribute.String("event", "EventAddAddrInfo"))
		// Ignore self
		if r.self.Equal(ev.NodeID) {
			break
		}
		// TODO: apply ttl
		cmd := &routing.EventIncludeAddCandidate[kadt.Key, kadt.PeerID]{
			NodeID: ev.NodeID,
		}
		// attempt to advance the include
		next, ok := r.advanceInclude(ctx, cmd)
		if ok {
			r.pending = append(r.pending, next)
		}

	case *EventRoutingUpdated:
		span.SetAttributes(attribute.String("event", "EventRoutingUpdated"), attribute.String("nodeid", ev.NodeID.String()))
		cmd := &routing.EventProbeAdd[kadt.Key, kadt.PeerID]{
			NodeID: ev.NodeID,
		}
		// attempt to advance the probe state machine
		next, ok := r.advanceProbe(ctx, cmd)
		if ok {
			r.pending = append(r.pending, next)
		}

	case *EventGetCloserNodesSuccess:
		span.SetAttributes(attribute.String("event", "EventGetCloserNodesSuccess"), attribute.String("queryid", string(ev.QueryID)), attribute.String("nodeid", ev.To.String()))
		switch ev.QueryID {
		case routing.BootstrapQueryID:
			for _, info := range ev.CloserNodes {
				// TODO: do this after advancing bootstrap
				r.pending = append(r.pending, &EventAddNode{
					NodeID: info,
				})
			}
			cmd := &routing.EventBootstrapFindCloserResponse[kadt.Key, kadt.PeerID]{
				NodeID:      ev.To,
				CloserNodes: ev.CloserNodes,
			}
			// attempt to advance the bootstrap
			next, ok := r.advanceBootstrap(ctx, cmd)
			if ok {
				r.pending = append(r.pending, next)
			}

		case IncludeQueryID:
			var cmd routing.IncludeEvent
			// require that the node responded with at least one closer node
			if len(ev.CloserNodes) > 0 {
				cmd = &routing.EventIncludeConnectivityCheckSuccess[kadt.Key, kadt.PeerID]{
					NodeID: ev.To,
				}
			} else {
				cmd = &routing.EventIncludeConnectivityCheckFailure[kadt.Key, kadt.PeerID]{
					NodeID: ev.To,
					Error:  fmt.Errorf("response did not include any closer nodes"),
				}
			}
			// attempt to advance the include
			next, ok := r.advanceInclude(ctx, cmd)
			if ok {
				r.pending = append(r.pending, next)
			}

		case ProbeQueryID:
			var cmd routing.ProbeEvent
			// require that the node responded with at least one closer node
			if len(ev.CloserNodes) > 0 {
				cmd = &routing.EventProbeConnectivityCheckSuccess[kadt.Key, kadt.PeerID]{
					NodeID: ev.To,
				}
			} else {
				cmd = &routing.EventProbeConnectivityCheckFailure[kadt.Key, kadt.PeerID]{
					NodeID: ev.To,
					Error:  fmt.Errorf("response did not include any closer nodes"),
				}
			}
			// attempt to advance the probe state machine
			next, ok := r.advanceProbe(ctx, cmd)
			if ok {
				r.pending = append(r.pending, next)
			}

		case routing.ExploreQueryID:
			for _, info := range ev.CloserNodes {
				r.pending = append(r.pending, &EventAddNode{
					NodeID: info,
				})
			}
			cmd := &routing.EventExploreFindCloserResponse[kadt.Key, kadt.PeerID]{
				NodeID:      ev.To,
				CloserNodes: ev.CloserNodes,
			}
			next, ok := r.advanceExplore(ctx, cmd)
			if ok {
				r.pending = append(r.pending, next)
			}

		default:
			panic(fmt.Sprintf("unexpected query id: %s", ev.QueryID))
		}
	case *EventGetCloserNodesFailure:
		span.SetAttributes(attribute.String("event", "EventGetCloserNodesFailure"), attribute.String("queryid", string(ev.QueryID)), attribute.String("nodeid", ev.To.String()))
		span.RecordError(ev.Err)
		switch ev.QueryID {
		case routing.BootstrapQueryID:
			cmd := &routing.EventBootstrapFindCloserFailure[kadt.Key, kadt.PeerID]{
				NodeID: ev.To,
				Error:  ev.Err,
			}
			// attempt to advance the bootstrap
			next, ok := r.advanceBootstrap(ctx, cmd)
			if ok {
				r.pending = append(r.pending, next)
			}
		case IncludeQueryID:
			cmd := &routing.EventIncludeConnectivityCheckFailure[kadt.Key, kadt.PeerID]{
				NodeID: ev.To,
				Error:  ev.Err,
			}
			// attempt to advance the include state machine
			next, ok := r.advanceInclude(ctx, cmd)
			if ok {
				r.pending = append(r.pending, next)
			}
		case ProbeQueryID:
			cmd := &routing.EventProbeConnectivityCheckFailure[kadt.Key, kadt.PeerID]{
				NodeID: ev.To,
				Error:  ev.Err,
			}
			// attempt to advance the probe state machine
			next, ok := r.advanceProbe(ctx, cmd)
			if ok {
				r.pending = append(r.pending, next)
			}
		case routing.ExploreQueryID:
			cmd := &routing.EventExploreFindCloserFailure[kadt.Key, kadt.PeerID]{
				NodeID: ev.To,
				Error:  ev.Err,
			}
			// attempt to advance the explore
			next, ok := r.advanceExplore(ctx, cmd)
			if ok {
				r.pending = append(r.pending, next)
			}

		default:
			panic(fmt.Sprintf("unexpected query id: %s", ev.QueryID))
		}
	case *EventNotifyConnectivity:
		span.SetAttributes(attribute.String("event", "EventNotifyConnectivity"), attribute.String("nodeid", ev.NodeID.String()))
		// ignore self
		if r.self.Equal(ev.NodeID) {
			break
		}
		r.cfg.Logger.Debug("peer has connectivity", tele.LogAttrPeerID(ev.NodeID))

		// tell the include state machine in case this is a new peer that could be added to the routing table
		cmd := &routing.EventIncludeAddCandidate[kadt.Key, kadt.PeerID]{
			NodeID: ev.NodeID,
		}
		next, ok := r.advanceInclude(ctx, cmd)
		if ok {
			r.pending = append(r.pending, next)
		}

		// tell the probe state machine in case there is are connectivity checks that could satisfied
		cmdProbe := &routing.EventProbeNotifyConnectivity[kadt.Key, kadt.PeerID]{
			NodeID: ev.NodeID,
		}
		nextProbe, ok := r.advanceProbe(ctx, cmdProbe)
		if ok {
			r.pending = append(r.pending, nextProbe)
		}
	case *EventNotifyNonConnectivity:
		span.SetAttributes(attribute.String("event", "EventNotifyConnectivity"), attribute.String("nodeid", ev.NodeID.String()))

		// tell the probe state machine to remove the node from the routing table and probe list
		cmdProbe := &routing.EventProbeRemove[kadt.Key, kadt.PeerID]{
			NodeID: ev.NodeID,
		}
		nextProbe, ok := r.advanceProbe(ctx, cmdProbe)
		if ok {
			r.pending = append(r.pending, nextProbe)
		}
	case *EventRoutingPoll:
		r.pollChildren(ctx)

	default:
		panic(fmt.Sprintf("unexpected dht event: %T", ev))
	}

	if len(r.pending) > 0 {
		select {
		case r.ready <- struct{}{}:
		default:
		}
	}
}

func (r *RoutingBehaviour) Ready() <-chan struct{} {
	return r.ready
}

func (r *RoutingBehaviour) Perform(ctx context.Context) (BehaviourEvent, bool) {
	ctx, span := r.cfg.Tracer.Start(ctx, "RoutingBehaviour.Perform")
	defer span.End()

	// No inbound work can be done until Perform is complete
	r.pendingMu.Lock()
	defer r.pendingMu.Unlock()

	for {
		// drain queued events first.
		if len(r.pending) > 0 {
			var ev BehaviourEvent
			ev, r.pending = r.pending[0], r.pending[1:]

			if len(r.pending) > 0 {
				select {
				case r.ready <- struct{}{}:
				default:
				}
			}
			return ev, true
		}

		// poll the child state machines in priority order to give each an opportunity to perform work
		r.pollChildren(ctx)

		// finally check if any pending events were accumulated in the meantime
		if len(r.pending) == 0 {
			return nil, false
		}
	}
}

// pollChildren must only be called while r.pendingMu is locked
func (r *RoutingBehaviour) pollChildren(ctx context.Context) {
	ev, ok := r.advanceBootstrap(ctx, &routing.EventBootstrapPoll{})
	if ok {
		r.pending = append(r.pending, ev)
	}

	ev, ok = r.advanceInclude(ctx, &routing.EventIncludePoll{})
	if ok {
		r.pending = append(r.pending, ev)
	}

	ev, ok = r.advanceProbe(ctx, &routing.EventProbePoll{})
	if ok {
		r.pending = append(r.pending, ev)
	}

	ev, ok = r.advanceExplore(ctx, &routing.EventExplorePoll{})
	if ok {
		r.pending = append(r.pending, ev)
	}
}

func (r *RoutingBehaviour) advanceBootstrap(ctx context.Context, ev routing.BootstrapEvent) (BehaviourEvent, bool) {
	ctx, span := r.cfg.Tracer.Start(ctx, "RoutingBehaviour.advanceBootstrap")
	defer span.End()
	bstate := r.bootstrap.Advance(ctx, ev)
	switch st := bstate.(type) {

	case *routing.StateBootstrapFindCloser[kadt.Key, kadt.PeerID]:
		return &EventOutboundGetCloserNodes{
			QueryID: routing.BootstrapQueryID,
			To:      st.NodeID,
			Target:  st.Target,
			Notify:  r,
		}, true

	case *routing.StateBootstrapWaiting:
		// bootstrap waiting for a message response, nothing to do
	case *routing.StateBootstrapFinished:
		r.cfg.Logger.Debug("bootstrap finished", slog.Duration("elapsed", st.Stats.End.Sub(st.Stats.Start)), slog.Int("requests", st.Stats.Requests), slog.Int("failures", st.Stats.Failure))
		return &EventBootstrapFinished{
			Stats: st.Stats,
		}, true
	case *routing.StateBootstrapIdle:
		// bootstrap not running, nothing to do
	default:
		panic(fmt.Sprintf("unexpected bootstrap state: %T", st))
	}

	return nil, false
}

func (r *RoutingBehaviour) advanceInclude(ctx context.Context, ev routing.IncludeEvent) (BehaviourEvent, bool) {
	ctx, span := r.cfg.Tracer.Start(ctx, "RoutingBehaviour.advanceInclude")
	defer span.End()

	istate := r.include.Advance(ctx, ev)
	switch st := istate.(type) {
	case *routing.StateIncludeConnectivityCheck[kadt.Key, kadt.PeerID]:
		span.SetAttributes(attribute.String("out_event", "EventOutboundGetCloserNodes"))
		// include wants to send a find node message to a node
		r.cfg.Logger.Debug("starting connectivity check", tele.LogAttrPeerID(st.NodeID), "source", "include")
		return &EventOutboundGetCloserNodes{
			QueryID: IncludeQueryID,
			To:      st.NodeID,
			Target:  st.NodeID.Key(),
			Notify:  r,
		}, true

	case *routing.StateIncludeRoutingUpdated[kadt.Key, kadt.PeerID]:
		// a node has been included in the routing table

		// notify other routing state machines that there is a new node in the routing table
		r.notify(ctx, &EventRoutingUpdated{
			NodeID: st.NodeID,
		})

		// return the event to notify outwards too
		span.SetAttributes(attribute.String("out_event", "EventRoutingUpdated"))
		r.cfg.Logger.Debug("peer added to routing table", tele.LogAttrPeerID(st.NodeID))
		return &EventRoutingUpdated{
			NodeID: st.NodeID,
		}, true
	case *routing.StateIncludeWaitingAtCapacity:
		// nothing to do except wait for message response or timeout
	case *routing.StateIncludeWaitingWithCapacity:
		// nothing to do except wait for message response or timeout
	case *routing.StateIncludeWaitingFull:
		// nothing to do except wait for message response or timeout
	case *routing.StateIncludeIdle:
		// nothing to do except wait for new nodes to be added to queue
	default:
		panic(fmt.Sprintf("unexpected include state: %T", st))
	}

	return nil, false
}

func (r *RoutingBehaviour) advanceProbe(ctx context.Context, ev routing.ProbeEvent) (BehaviourEvent, bool) {
	ctx, span := r.cfg.Tracer.Start(ctx, "RoutingBehaviour.advanceProbe")
	defer span.End()
	st := r.probe.Advance(ctx, ev)
	switch st := st.(type) {
	case *routing.StateProbeConnectivityCheck[kadt.Key, kadt.PeerID]:
		// include wants to send a find node message to a node
		r.cfg.Logger.Debug("starting connectivity check", tele.LogAttrPeerID(st.NodeID), "source", "probe")
		return &EventOutboundGetCloserNodes{
			QueryID: ProbeQueryID,
			To:      st.NodeID,
			Target:  st.NodeID.Key(),
			Notify:  r,
		}, true
	case *routing.StateProbeNodeFailure[kadt.Key, kadt.PeerID]:
		// a node has failed a connectivity check and been removed from the routing table and the probe list

		// emit an EventRoutingRemoved event to notify clients that the node has been removed
		r.cfg.Logger.Debug("peer removed from routing table", tele.LogAttrPeerID(st.NodeID))
		r.pending = append(r.pending, &EventRoutingRemoved{
			NodeID: st.NodeID,
		})

		// add the node to the inclusion list for a second chance
		r.notify(ctx, &EventAddNode{
			NodeID: st.NodeID,
		})
	case *routing.StateProbeWaitingAtCapacity:
		// the probe state machine is waiting for responses for checks and the maximum number of concurrent checks has been reached.
		// nothing to do except wait for message response or timeout
	case *routing.StateProbeWaitingWithCapacity:
		// the probe state machine is waiting for responses for checks but has capacity to perform more
		// nothing to do except wait for message response or timeout
	case *routing.StateProbeIdle:
		// the probe state machine is not running any checks.
		// nothing to do except wait for message response or timeout
	default:
		panic(fmt.Sprintf("unexpected include state: %T", st))
	}

	return nil, false
}

func (r *RoutingBehaviour) advanceExplore(ctx context.Context, ev routing.ExploreEvent) (BehaviourEvent, bool) {
	ctx, span := r.cfg.Tracer.Start(ctx, "RoutingBehaviour.advanceExplore")
	defer span.End()
	bstate := r.explore.Advance(ctx, ev)
	switch st := bstate.(type) {

	case *routing.StateExploreFindCloser[kadt.Key, kadt.PeerID]:
		r.cfg.Logger.Debug("starting explore", slog.Int("cpl", st.Cpl), tele.LogAttrPeerID(st.NodeID))
		return &EventOutboundGetCloserNodes{
			QueryID: routing.ExploreQueryID,
			To:      st.NodeID,
			Target:  st.Target,
			Notify:  r,
		}, true

	case *routing.StateExploreWaiting:
		// explore waiting for a message response, nothing to do
	case *routing.StateExploreQueryFinished:
		// nothing to do except notify via telemetry
	case *routing.StateExploreQueryTimeout:
		// nothing to do except notify via telemetry
	case *routing.StateExploreFailure:
		r.cfg.Logger.Warn("explore failure", slog.Int("cpl", st.Cpl), tele.LogAttrError(st.Error))
	case *routing.StateExploreIdle:
		// bootstrap not running, nothing to do
	default:
		panic(fmt.Sprintf("unexpected explore state: %T", st))
	}

	return nil, false
}
