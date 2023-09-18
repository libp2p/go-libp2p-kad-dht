package coord

import (
	"context"
	"fmt"
	"sync"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/exp/slog"

	"github.com/libp2p/go-libp2p-kad-dht/v2/coord/routing"
	"github.com/libp2p/go-libp2p-kad-dht/v2/kadt"
)

// A RoutingBehaviour provices the behaviours for bootstrapping and maintaining a DHT's routing table.
type RoutingBehaviour struct {
	// self is the peer id of the system the dht is running on
	self kadt.PeerID

	// bootstrap is the bootstrap state machine, responsible for bootstrapping the routing table
	bootstrap SM[routing.BootstrapEvent, routing.BootstrapState]

	// include is the inclusion state machine, responsible for vetting nodes before including them in the routing table
	include SM[routing.IncludeEvent, routing.IncludeState]

	// probe is the node probing state machine, responsible for periodically checking connectivity of nodes in the routing table
	probe SM[routing.ProbeEvent, routing.ProbeState]

	pendingMu sync.Mutex
	pending   []BehaviourEvent
	ready     chan struct{}

	logger *slog.Logger
	tracer trace.Tracer
}

func NewRoutingBehaviour(self kadt.PeerID, bootstrap SM[routing.BootstrapEvent, routing.BootstrapState], include SM[routing.IncludeEvent, routing.IncludeState], probe SM[routing.ProbeEvent, routing.ProbeState], logger *slog.Logger, tracer trace.Tracer) *RoutingBehaviour {
	r := &RoutingBehaviour{
		self:      self,
		bootstrap: bootstrap,
		include:   include,
		probe:     probe,
		ready:     make(chan struct{}, 1),
		logger:    logger.With("behaviour", "routing"),
		tracer:    tracer,
	}
	return r
}

func (r *RoutingBehaviour) Notify(ctx context.Context, ev BehaviourEvent) {
	ctx, span := r.tracer.Start(ctx, "RoutingBehaviour.Notify")
	defer span.End()

	r.pendingMu.Lock()
	defer r.pendingMu.Unlock()
	r.notify(ctx, ev)
}

// notify must only be called while r.pendingMu is held
func (r *RoutingBehaviour) notify(ctx context.Context, ev BehaviourEvent) {
	ctx, span := r.tracer.Start(ctx, "RoutingBehaviour.notify")
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
		case "bootstrap":
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

		case "include":
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

		case "probe":
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

		default:
			panic(fmt.Sprintf("unexpected query id: %s", ev.QueryID))
		}
	case *EventGetCloserNodesFailure:
		span.SetAttributes(attribute.String("event", "EventGetCloserNodesFailure"), attribute.String("queryid", string(ev.QueryID)), attribute.String("nodeid", ev.To.String()))
		span.RecordError(ev.Err)
		switch ev.QueryID {
		case "bootstrap":
			cmd := &routing.EventBootstrapFindCloserFailure[kadt.Key, kadt.PeerID]{
				NodeID: ev.To,
				Error:  ev.Err,
			}
			// attempt to advance the bootstrap
			next, ok := r.advanceBootstrap(ctx, cmd)
			if ok {
				r.pending = append(r.pending, next)
			}
		case "include":
			cmd := &routing.EventIncludeConnectivityCheckFailure[kadt.Key, kadt.PeerID]{
				NodeID: ev.To,
				Error:  ev.Err,
			}
			// attempt to advance the include state machine
			next, ok := r.advanceInclude(ctx, cmd)
			if ok {
				r.pending = append(r.pending, next)
			}
		case "probe":
			cmd := &routing.EventProbeConnectivityCheckFailure[kadt.Key, kadt.PeerID]{
				NodeID: ev.To,
				Error:  ev.Err,
			}
			// attempt to advance the probe state machine
			next, ok := r.advanceProbe(ctx, cmd)
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
			NodeID: kadt.PeerID(ev.NodeID),
		}
		nextProbe, ok := r.advanceProbe(ctx, cmdProbe)
		if ok {
			r.pending = append(r.pending, nextProbe)
		}

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
	ctx, span := r.tracer.Start(ctx, "RoutingBehaviour.Perform")
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

		ev, ok := r.advanceBootstrap(ctx, &routing.EventBootstrapPoll{})
		if ok {
			return ev, true
		}

		ev, ok = r.advanceInclude(ctx, &routing.EventIncludePoll{})
		if ok {
			return ev, true
		}

		ev, ok = r.advanceProbe(ctx, &routing.EventProbePoll{})
		if ok {
			return ev, true
		}

		// finally check if any pending events were accumulated in the meantime
		if len(r.pending) == 0 {
			return nil, false
		}
	}
}

func (r *RoutingBehaviour) advanceBootstrap(ctx context.Context, ev routing.BootstrapEvent) (BehaviourEvent, bool) {
	ctx, span := r.tracer.Start(ctx, "RoutingBehaviour.advanceBootstrap")
	defer span.End()
	bstate := r.bootstrap.Advance(ctx, ev)
	switch st := bstate.(type) {

	case *routing.StateBootstrapFindCloser[kadt.Key, kadt.PeerID]:
		return &EventOutboundGetCloserNodes{
			QueryID: "bootstrap",
			To:      st.NodeID,
			Target:  st.Target,
			Notify:  r,
		}, true

	case *routing.StateBootstrapWaiting:
		// bootstrap waiting for a message response, nothing to do
	case *routing.StateBootstrapFinished:
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
	ctx, span := r.tracer.Start(ctx, "RoutingBehaviour.advanceInclude")
	defer span.End()

	istate := r.include.Advance(ctx, ev)
	switch st := istate.(type) {
	case *routing.StateIncludeConnectivityCheck[kadt.Key, kadt.PeerID]:
		span.SetAttributes(attribute.String("out_event", "EventOutboundGetCloserNodes"))
		// include wants to send a find node message to a node
		return &EventOutboundGetCloserNodes{
			QueryID: "include",
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
	ctx, span := r.tracer.Start(ctx, "RoutingBehaviour.advanceProbe")
	defer span.End()
	st := r.probe.Advance(ctx, ev)
	switch st := st.(type) {
	case *routing.StateProbeConnectivityCheck[kadt.Key, kadt.PeerID]:
		// include wants to send a find node message to a node
		return &EventOutboundGetCloserNodes{
			QueryID: "probe",
			To:      st.NodeID,
			Target:  st.NodeID.Key(),
			Notify:  r,
		}, true
	case *routing.StateProbeNodeFailure[kadt.Key, kadt.PeerID]:
		// a node has failed a connectivity check and been removed from the routing table and the probe list

		// emit an EventRoutingRemoved event to notify clients that the node has been removed
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
