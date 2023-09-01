package kademlia

import (
	"context"
	"fmt"
	"sync"

	"github.com/plprobelab/go-kademlia/kad"
	"github.com/plprobelab/go-kademlia/key"
	"github.com/plprobelab/go-kademlia/routing"
	"github.com/plprobelab/go-kademlia/util"
	"go.opentelemetry.io/otel/attribute"
	"golang.org/x/exp/slog"
)

type RoutingBehaviour[K kad.Key[K], A kad.Address[A]] struct {
	// self is the node id of the system the dht is running on
	self kad.NodeID[K]
	// bootstrap is the bootstrap state machine, responsible for bootstrapping the routing table
	bootstrap SM[routing.BootstrapEvent, routing.BootstrapState]

	// include is the inclusion state machine, responsible for vetting nodes before including them in the routing table
	include SM[routing.IncludeEvent, routing.IncludeState]

	// probe is the node probing state machine, responsible for periodically checking connectivity of nodes in the routing table
	probe SM[routing.ProbeEvent, routing.ProbeState]

	pendingMu sync.Mutex
	pending   []DhtEvent
	ready     chan struct{}

	logger *slog.Logger
}

func NewRoutingBehaviour[K kad.Key[K], A kad.Address[A]](self kad.NodeID[K], bootstrap SM[routing.BootstrapEvent, routing.BootstrapState], include SM[routing.IncludeEvent, routing.IncludeState], probe SM[routing.ProbeEvent, routing.ProbeState], logger *slog.Logger) *RoutingBehaviour[K, A] {
	r := &RoutingBehaviour[K, A]{
		self:      self,
		bootstrap: bootstrap,
		include:   include,
		probe:     probe,
		ready:     make(chan struct{}, 1),
		logger:    logger,
	}
	return r
}

func (r *RoutingBehaviour[K, A]) Notify(ctx context.Context, ev DhtEvent) {
	ctx, span := util.StartSpan(ctx, "RoutingBehaviour.Notify")
	defer span.End()

	r.pendingMu.Lock()
	defer r.pendingMu.Unlock()
	r.notify(ctx, ev)
}

// notify must only be called while r.pendingMu is held
func (r *RoutingBehaviour[K, A]) notify(ctx context.Context, ev DhtEvent) {
	ctx, span := util.StartSpan(ctx, "RoutingBehaviour.notify")
	defer span.End()
	switch ev := ev.(type) {
	case *EventDhtStartBootstrap[K, A]:
		span.SetAttributes(attribute.String("event", "EventDhtStartBootstrap"))
		cmd := &routing.EventBootstrapStart[K, A]{
			ProtocolID:        ev.ProtocolID,
			Message:           ev.Message,
			KnownClosestNodes: ev.SeedNodes,
		}
		// attempt to advance the bootstrap
		next, ok := r.advanceBootstrap(ctx, cmd)
		if ok {
			r.pending = append(r.pending, next)
		}

	case *EventDhtAddNodeInfo[K, A]:
		span.SetAttributes(attribute.String("event", "EventDhtAddNodeInfo"))
		// Ignore self
		if key.Equal(ev.NodeInfo.ID().Key(), r.self.Key()) {
			break
		}
		cmd := &routing.EventIncludeAddCandidate[K, A]{
			NodeInfo: ev.NodeInfo,
		}
		// attempt to advance the include
		next, ok := r.advanceInclude(ctx, cmd)
		if ok {
			r.pending = append(r.pending, next)
		}

	case *EventRoutingUpdated[K, A]:
		span.SetAttributes(attribute.String("event", "EventRoutingUpdated"))
		cmd := &routing.EventProbeAdd[K]{
			NodeID: ev.NodeInfo.ID(),
		}
		// attempt to advance the probe state machine
		next, ok := r.advanceProbe(ctx, cmd)
		if ok {
			r.pending = append(r.pending, next)
		}

	case *EventGetClosestNodesSuccess[K, A]:
		span.SetAttributes(attribute.String("event", "EventGetClosestNodesFailure"), attribute.String("queryid", string(ev.QueryID)), attribute.String("nodeid", string(ev.To.ID().String())))
		switch ev.QueryID {
		case "bootstrap":
			for _, info := range ev.ClosestNodes {
				// TODO: do this after advancing bootstrap
				r.pending = append(r.pending, &EventDhtAddNodeInfo[K, A]{
					NodeInfo: info,
				})
			}
			cmd := &routing.EventBootstrapMessageResponse[K, A]{
				NodeID:   ev.To.ID(),
				Response: ClosestNodesFakeResponse(ev.Target, ev.ClosestNodes),
			}
			// attempt to advance the bootstrap
			next, ok := r.advanceBootstrap(ctx, cmd)
			if ok {
				r.pending = append(r.pending, next)
			}

		case "include":
			cmd := &routing.EventIncludeMessageResponse[K, A]{
				NodeInfo: ev.To,
				Response: ClosestNodesFakeResponse(ev.Target, ev.ClosestNodes),
			}
			// attempt to advance the include
			next, ok := r.advanceInclude(ctx, cmd)
			if ok {
				r.pending = append(r.pending, next)
			}

		case "probe":
			cmd := &routing.EventProbeMessageResponse[K, A]{
				NodeInfo: ev.To,
				Response: ClosestNodesFakeResponse(ev.Target, ev.ClosestNodes),
			}
			// attempt to advance the probe state machine
			next, ok := r.advanceProbe(ctx, cmd)
			if ok {
				r.pending = append(r.pending, next)
			}

		default:
			panic(fmt.Sprintf("unexpected query id: %s", ev.QueryID))
		}
	case *EventGetClosestNodesFailure[K, A]:
		span.SetAttributes(attribute.String("event", "EventGetClosestNodesFailure"), attribute.String("queryid", string(ev.QueryID)), attribute.String("nodeid", string(ev.To.ID().String())))
		span.RecordError(ev.Err)
		switch ev.QueryID {
		case "bootstrap":
			cmd := &routing.EventBootstrapMessageFailure[K]{
				NodeID: ev.To.ID(),
				Error:  ev.Err,
			}
			// attempt to advance the bootstrap
			next, ok := r.advanceBootstrap(ctx, cmd)
			if ok {
				r.pending = append(r.pending, next)
			}
		case "include":
			cmd := &routing.EventIncludeMessageFailure[K, A]{
				NodeInfo: ev.To,
				Error:    ev.Err,
			}
			// attempt to advance the include state machine
			next, ok := r.advanceInclude(ctx, cmd)
			if ok {
				r.pending = append(r.pending, next)
			}
		case "probe":
			cmd := &routing.EventProbeMessageFailure[K, A]{
				NodeInfo: ev.To,
				Error:    ev.Err,
			}
			// attempt to advance the probe state machine
			next, ok := r.advanceProbe(ctx, cmd)
			if ok {
				r.pending = append(r.pending, next)
			}

		default:
			panic(fmt.Sprintf("unexpected query id: %s", ev.QueryID))
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

func (r *RoutingBehaviour[K, A]) Ready() <-chan struct{} {
	return r.ready
}

func (r *RoutingBehaviour[K, A]) Perform(ctx context.Context) (DhtEvent, bool) {
	ctx, span := util.StartSpan(ctx, "RoutingBehaviour.Perform")
	defer span.End()

	// No inbound work can be done until Perform is complete
	r.pendingMu.Lock()
	defer r.pendingMu.Unlock()

	for {
		// drain queued events first.
		if len(r.pending) > 0 {
			var ev DhtEvent
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

func (r *RoutingBehaviour[K, A]) advanceBootstrap(ctx context.Context, ev routing.BootstrapEvent) (DhtEvent, bool) {
	ctx, span := util.StartSpan(ctx, "RoutingBehaviour.advanceBootstrap")
	defer span.End()
	bstate := r.bootstrap.Advance(ctx, ev)
	switch st := bstate.(type) {

	case *routing.StateBootstrapMessage[K, A]:
		return &EventOutboundGetClosestNodes[K, A]{
			QueryID: "bootstrap",
			To:      NewNodeAddr[K, A](st.NodeID, nil),
			Target:  st.Message.Target(),
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

func (r *RoutingBehaviour[K, A]) advanceInclude(ctx context.Context, ev routing.IncludeEvent) (DhtEvent, bool) {
	ctx, span := util.StartSpan(ctx, "RoutingBehaviour.advanceInclude")
	defer span.End()
	istate := r.include.Advance(ctx, ev)
	switch st := istate.(type) {
	case *routing.StateIncludeFindNodeMessage[K, A]:
		// include wants to send a find node message to a node
		return &EventOutboundGetClosestNodes[K, A]{
			QueryID: "include",
			To:      st.NodeInfo,
			Target:  st.NodeInfo.ID().Key(),
			Notify:  r,
		}, true

	case *routing.StateIncludeRoutingUpdated[K, A]:
		// a node has been included in the routing table

		// notify other routing state machines that there is a new node in the routing table
		r.notify(ctx, &EventRoutingUpdated[K, A]{
			NodeInfo: st.NodeInfo,
		})

		// return the event to notify outwards too
		return &EventRoutingUpdated[K, A]{
			NodeInfo: st.NodeInfo,
		}, true
	case *routing.StateIncludeWaitingAtCapacity:
		// nothing to do except wait for message response or timeout
	case *routing.StateIncludeWaitingWithCapacity:
		// nothing to do except wait for message response or timeout
	case *routing.StateIncludeWaitingFull:
		// nothing to do except wait for message response or timeout
	case *routing.StateIncludeIdle:
		// nothing to do except wait for message response or timeout
	default:
		panic(fmt.Sprintf("unexpected include state: %T", st))
	}

	return nil, false
}

func (r *RoutingBehaviour[K, A]) advanceProbe(ctx context.Context, ev routing.ProbeEvent) (DhtEvent, bool) {
	ctx, span := util.StartSpan(ctx, "RoutingBehaviour.advanceProbe")
	defer span.End()
	st := r.probe.Advance(ctx, ev)
	switch st := st.(type) {
	case *routing.StateProbeConnectivityCheck[K]:
		// include wants to send a find node message to a node
		return &EventOutboundGetClosestNodes[K, A]{
			QueryID: "probe",
			To:      unaddressedNodeInfo[K, A]{NodeID: st.NodeID},
			Target:  st.NodeID.Key(),
			Notify:  r,
		}, true
	case *routing.StateProbeNodeFailure[K]:
		// a node has failed a connectivity check been removed from the routing table and the probe list
		// add the node to the inclusion list for a second chance
		r.notify(ctx, &EventDhtAddNodeInfo[K, A]{
			NodeInfo: unaddressedNodeInfo[K, A]{NodeID: st.NodeID},
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

type unaddressedNodeInfo[K kad.Key[K], A kad.Address[A]] struct {
	NodeID kad.NodeID[K]
}

func (u unaddressedNodeInfo[K, A]) ID() kad.NodeID[K] { return u.NodeID }
func (u unaddressedNodeInfo[K, A]) Addresses() []A    { return nil }
