package routing

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/plprobelab/go-kademlia/kad"
	"github.com/plprobelab/go-kademlia/kaderr"
	"github.com/plprobelab/go-kademlia/key"
	"go.opentelemetry.io/otel/attribute"

	"github.com/libp2p/go-libp2p-kad-dht/v2/tele"
)

type RoutingTableCpl[K kad.Key[K], N kad.NodeID[K]] interface {
	kad.RoutingTable[K, N]

	// Cpl returns the longest common prefix length the supplied key shares with the table's key.
	Cpl(kk K) int

	// CplSize returns the number of nodes in the table whose longest common prefix with the table's key is of length cpl.
	CplSize(cpl int) int
}

// The Probe state machine performs regular connectivity checks for nodes in a routing table.
//
// The state machine is notified of a new entry in the routing table via the [EventProbeAdd] event. This adds the node
// to an internal list and sets a time for a check to be performed, based on the current time plus a configurable
// interval.
//
// Connectivity checks are performed in time order, so older nodes are processed first. The connectivity check performed
// is the same as for the [Include] state machine: ask the node for closest nodes to itself and confirm that the node
// returns at least one node in the list of closer nodes. The state machine emits the [StateProbeConnectivityCheck]
// state when it wants to check the status of a node.
//
// The state machine expects to be notified either with the [EventProbeMessageResponse] or the
// [EventProbeMessageFailure] events to determine the outcome of the check. If neither are received within a
// configurable timeout the node is marked as failed.
//
// Nodes that receive a successful response have their next check time updated to the current time plus the configured
// [ProbeConfig.CheckInterval].
//
// Nodes that fail a connectivity check, or are timed out, are removed from the routing table and from the list of nodes
// to check. The state machine emits the [StateProbeNodeFailure] state to notify callers of this event.
//
// The state machine accepts a [EventProbePoll] event to check for outstanding work such as initiating a new check or
// timing out an existing one.
//
// The [EventProbeRemove] event may be used to remove a node from the check list and from the routing table.
//
// The state machine accepts the [EventProbeNotifyConnectivity] event as a notification that an external system has
// performed a suitable connectivity check, such as when the node responds to a query. The probe state machine treats
// these events as if a successful response had been received from a check by advancing the time of the next check.
type Probe[K kad.Key[K], N kad.NodeID[K]] struct {
	rt RoutingTableCpl[K, N]

	// nvl is a list of nodes with information about their connectivity checks
	// TODO: this will be expanded with more general scoring information related to their utility
	nvl *nodeValueList[K]

	// cfg is a copy of the optional configuration supplied to the Probe
	cfg ProbeConfig
}

// ProbeConfig specifies optional configuration for a Probe
type ProbeConfig struct {
	CheckInterval time.Duration // the minimum time interval between checks for a node
	Concurrency   int           // the maximum number of probe checks that may be in progress at any one time
	Timeout       time.Duration // the time to wait before terminating a check that is not making progress
	Clock         clock.Clock   // a clock that may be replaced by a mock when testing
}

// Validate checks the configuration options and returns an error if any have invalid values.
func (cfg *ProbeConfig) Validate() error {
	if cfg.Clock == nil {
		return &kaderr.ConfigurationError{
			Component: "ProbeConfig",
			Err:       fmt.Errorf("clock must not be nil"),
		}
	}

	if cfg.Concurrency < 1 {
		return &kaderr.ConfigurationError{
			Component: "ProbeConfig",
			Err:       fmt.Errorf("concurrency must be greater than zero"),
		}
	}

	if cfg.Timeout < 1 {
		return &kaderr.ConfigurationError{
			Component: "ProbeConfig",
			Err:       fmt.Errorf("timeout must be greater than zero"),
		}
	}

	if cfg.CheckInterval < 1 {
		return &kaderr.ConfigurationError{
			Component: "ProbeConfig",
			Err:       fmt.Errorf("revisit interval must be greater than zero"),
		}
	}

	return nil
}

// DefaultProbeConfig returns the default configuration options for a Probe.
// Options may be overridden before passing to NewProbe
func DefaultProbeConfig() *ProbeConfig {
	return &ProbeConfig{
		Clock:         clock.New(),   // use standard time
		Concurrency:   3,             // MAGIC
		Timeout:       time.Minute,   // MAGIC
		CheckInterval: 6 * time.Hour, // MAGIC
	}
}

func NewProbe[K kad.Key[K], N kad.NodeID[K]](rt RoutingTableCpl[K, N], cfg *ProbeConfig) (*Probe[K, N], error) {
	if cfg == nil {
		cfg = DefaultProbeConfig()
	} else if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return &Probe[K, N]{
		cfg: *cfg,
		rt:  rt,
		nvl: NewNodeValueList[K](),
	}, nil
}

// Advance advances the state of the probe state machine by attempting to advance its query if running.
func (p *Probe[K, N]) Advance(ctx context.Context, ev ProbeEvent) ProbeState {
	_, span := tele.StartSpan(ctx, "Probe.Advance")
	defer span.End()

	switch tev := ev.(type) {
	case *EventProbePoll:
		// ignore, nothing to do
		span.SetAttributes(tele.AttrEvent("EventProbePoll"))
	case *EventProbeAdd[K]:
		// check presence in routing table
		span.SetAttributes(tele.AttrEvent("EventProbeAdd"), attribute.String("nodeid", tev.NodeID.String()))
		if _, found := p.rt.GetNode(tev.NodeID.Key()); !found {
			// ignore if not in routing table
			span.RecordError(errors.New("node not in routing table"))
			break
		}

		// add a node to the value list
		nv := &nodeValue[K]{
			NodeID:       tev.NodeID,
			NextCheckDue: p.cfg.Clock.Now().Add(p.cfg.CheckInterval),
			Cpl:          p.rt.Cpl(tev.NodeID.Key()),
		}
		// TODO: if node was in ongoing list return a state that can signal the caller to cancel any prior outbound message
		p.nvl.Put(nv)
	case *EventProbeRemove[K]:
		span.SetAttributes(tele.AttrEvent("EventProbeRemove"), attribute.String("nodeid", tev.NodeID.String()))
		p.rt.RemoveKey(tev.NodeID.Key())
		p.nvl.Remove(tev.NodeID)
		return &StateProbeNodeFailure[K]{
			NodeID: tev.NodeID,
		}
	case *EventProbeConnectivityCheckSuccess[K]:
		span.SetAttributes(tele.AttrEvent("EventProbeMessageResponse"), attribute.String("nodeid", tev.NodeID.String()))
		nv, found := p.nvl.Get(tev.NodeID)
		if !found {
			// ignore message for unknown node, which might have been removed
			span.RecordError(errors.New("node not in node value list"))
			break
		}
		// update next check time
		nv.NextCheckDue = p.cfg.Clock.Now().Add(p.cfg.CheckInterval)

		// put into list, which will clear any ongoing check too
		p.nvl.Put(nv)

	case *EventProbeConnectivityCheckFailure[K]:
		// probe failed, so remove from routing table and from list
		span.SetAttributes(tele.AttrEvent("EventProbeMessageFailure"), attribute.String("nodeid", tev.NodeID.String()))
		span.RecordError(tev.Error)
		p.rt.RemoveKey(tev.NodeID.Key())
		p.nvl.Remove(tev.NodeID)
		return &StateProbeNodeFailure[K]{
			NodeID: tev.NodeID,
		}
	case *EventProbeNotifyConnectivity[K]:
		span.SetAttributes(tele.AttrEvent("EventProbeNotifyConnectivity"), attribute.String("nodeid", tev.NodeID.String()))
		nv, found := p.nvl.Get(tev.NodeID)
		if !found {
			// ignore message for unknown node, which might have been removed
			break
		}
		// update next check time
		nv.NextCheckDue = p.cfg.Clock.Now().Add(p.cfg.CheckInterval)

		// put into list, which will clear any ongoing check too
		p.nvl.Put(nv)

	default:
		panic(fmt.Sprintf("unexpected event: %T", tev))
	}

	// Check if there is capacity
	if p.cfg.Concurrency <= p.nvl.OngoingCount() {
		// see if a check can be timed out to free capacity
		candidate, found := p.nvl.FindCheckPastDeadline(p.cfg.Clock.Now())
		if !found {
			// nothing suitable for time out
			return &StateProbeWaitingAtCapacity{}
		}

		// mark the node as failed since it timed out
		p.rt.RemoveKey(candidate.Key())
		p.nvl.Remove(candidate)
		return &StateProbeNodeFailure[K]{
			NodeID: candidate,
		}

	}

	// there is capacity to start a new check
	next, ok := p.nvl.PeekNext(p.cfg.Clock.Now())
	if !ok {
		if p.nvl.OngoingCount() > 0 {
			// waiting for a check but nothing else to do
			return &StateProbeWaitingWithCapacity{}
		}
		// nothing happening and nothing to do
		return &StateProbeIdle{}
	}

	p.nvl.MarkOngoing(next.NodeID, p.cfg.Clock.Now().Add(p.cfg.Timeout))

	// Ask the node to find itself
	return &StateProbeConnectivityCheck[K]{
		NodeID: next.NodeID,
	}
}

// ProbeState is the state of the [Probe] state machine.
type ProbeState interface {
	probeState()
}

// StateProbeConnectivityCheck indicates that the probe subsystem is waiting to send a connectivity check to a node.
// A find node message should be sent to the node, with the target being the node's key.
type StateProbeConnectivityCheck[K kad.Key[K]] struct {
	NodeID kad.NodeID[K] // the node to send the message to
}

// StateProbeIdle indicates that the probe state machine is not running any checks.
type StateProbeIdle struct{}

// StateProbeWaitingAtCapacity indicates that the probe state machine is waiting for responses for checks and
// the maximum number of concurrent checks has been reached.
type StateProbeWaitingAtCapacity struct{}

// StateProbeWaitingWithCapacity indicates that the probe state machine is waiting for responses for checks
// but has capacity to perform more.
type StateProbeWaitingWithCapacity struct{}

// StateProbeNodeFailure indicates a node has failed a connectivity check been removed from the routing table and the probe list
type StateProbeNodeFailure[K kad.Key[K]] struct {
	NodeID kad.NodeID[K]
}

// probeState() ensures that only Probe states can be assigned to the ProbeState interface.
func (*StateProbeConnectivityCheck[K]) probeState() {}
func (*StateProbeIdle) probeState()                 {}
func (*StateProbeWaitingAtCapacity) probeState()    {}
func (*StateProbeWaitingWithCapacity) probeState()  {}
func (*StateProbeNodeFailure[K]) probeState()       {}

// ProbeEvent is an event intended to advance the state of a probe.
type ProbeEvent interface {
	probeEvent()
}

// EventProbePoll is an event that signals the probe that it can perform housekeeping work such as time out queries.
type EventProbePoll struct{}

// EventProbeAdd notifies a probe that a node should be added to its list of nodes.
type EventProbeAdd[K kad.Key[K]] struct {
	NodeID kad.NodeID[K] // the node to be probed
}

// EventProbeRemove notifies a probe that a node should be removed from its list of nodes and the routing table.
type EventProbeRemove[K kad.Key[K]] struct {
	NodeID kad.NodeID[K] // the node to be removed
}

// EventProbeConnectivityCheckSuccess notifies a [Probe] that a requested connectivity check has received a successful response.
type EventProbeConnectivityCheckSuccess[K kad.Key[K]] struct {
	NodeID kad.NodeID[K] // the node the message was sent to
}

// EventProbeConnectivityCheckFailure notifies a [Probe] that a requested connectivity check has failed.
type EventProbeConnectivityCheckFailure[K kad.Key[K]] struct {
	NodeID kad.NodeID[K] // the node the message was sent to
	Error  error         // the error that caused the failure, if any
}

// EventProbeNotifyConnectivity notifies a probe that a node has confirmed connectivity from another source such as a query.
type EventProbeNotifyConnectivity[K kad.Key[K]] struct {
	NodeID kad.NodeID[K]
}

// probeEvent() ensures that only events accepted by a [Probe] can be assigned to the [ProbeEvent] interface.
func (*EventProbePoll) probeEvent()                        {}
func (*EventProbeAdd[K]) probeEvent()                      {}
func (*EventProbeRemove[K]) probeEvent()                   {}
func (*EventProbeConnectivityCheckSuccess[K]) probeEvent() {}
func (*EventProbeConnectivityCheckFailure[K]) probeEvent() {}
func (*EventProbeNotifyConnectivity[K]) probeEvent()       {}

type nodeValue[K kad.Key[K]] struct {
	NodeID        kad.NodeID[K]
	Cpl           int // the longest common prefix length the node shares with the routing table's key
	NextCheckDue  time.Time
	CheckDeadline time.Time
	Index         int // the index of the item in the ordering
}

type nodeValueEntry[K kad.Key[K]] struct {
	nv    *nodeValue[K]
	index int // the index of the item in the ordering
}

type nodeValueList[K kad.Key[K]] struct {
	nodes   map[string]*nodeValueEntry[K]
	pending *nodeValuePendingList[K]
	// ongoing is a list of nodes with ongoing/in-progress probes, loosely ordered earliest to most recent
	ongoing []kad.NodeID[K]
}

func NewNodeValueList[K kad.Key[K]]() *nodeValueList[K] {
	return &nodeValueList[K]{
		nodes:   make(map[string]*nodeValueEntry[K]),
		ongoing: make([]kad.NodeID[K], 0),
		pending: new(nodeValuePendingList[K]),
	}
}

// Put adds a node value to the list, replacing any existing value.
// It is added to the pending list and removed from the ongoing list if it was already present there.
func (l *nodeValueList[K]) Put(nv *nodeValue[K]) {
	mk := key.HexString(nv.NodeID.Key())
	nve, exists := l.nodes[mk]
	if !exists {
		nve = &nodeValueEntry[K]{
			nv: nv,
		}
	} else {
		nve.nv = nv
		heap.Remove(l.pending, nve.index)
	}
	heap.Push(l.pending, nve)
	l.nodes[mk] = nve
	heap.Fix(l.pending, nve.index)
	l.removeFromOngoing(nv.NodeID)
}

func (l *nodeValueList[K]) Get(n kad.NodeID[K]) (*nodeValue[K], bool) {
	mk := key.HexString(n.Key())
	nve, found := l.nodes[mk]
	if !found {
		return nil, false
	}
	return nve.nv, true
}

func (l *nodeValueList[K]) PendingCount() int {
	return len(*l.pending)
}

func (l *nodeValueList[K]) OngoingCount() int {
	return len(l.ongoing)
}

func (l *nodeValueList[K]) NodeCount() int {
	return len(l.nodes)
}

// Put removes a node value from the list, deleting its information.
// It is removed from the pending list andongoing list if it was already present in either.
func (l *nodeValueList[K]) Remove(n kad.NodeID[K]) {
	mk := key.HexString(n.Key())
	nve, ok := l.nodes[mk]
	if !ok {
		return
	}
	delete(l.nodes, mk)
	if nve.index >= 0 {
		heap.Remove(l.pending, nve.index)
	}
	l.removeFromOngoing(n)
}

// FindCheckPastDeadline looks for the first node in the ongoing list whose deadline is
// before the supplied timestamp.
func (l *nodeValueList[K]) FindCheckPastDeadline(ts time.Time) (kad.NodeID[K], bool) {
	// ongoing is in start time order, oldest first
	for _, n := range l.ongoing {
		mk := key.HexString(n.Key())
		nve, ok := l.nodes[mk]
		if !ok {
			// somehow the node doesn't exist so this is an obvious candidate for removal
			return n, true
		}
		if !nve.nv.CheckDeadline.After(ts) {
			return n, true
		}
	}
	return nil, false
}

func (l *nodeValueList[K]) removeFromOngoing(n kad.NodeID[K]) {
	// ongoing list is expected to be small, so linear search is ok
	for i := range l.ongoing {
		if key.Equal(n.Key(), l.ongoing[i].Key()) {
			if len(l.ongoing) > 1 {
				// swap with last entry
				l.ongoing[i], l.ongoing[len(l.ongoing)-1] = l.ongoing[len(l.ongoing)-1], l.ongoing[i]
			}
			// remove last entry
			l.ongoing[len(l.ongoing)-1] = nil
			l.ongoing = l.ongoing[:len(l.ongoing)-1]
			return
		}
	}
}

// PeekNext returns the next node that is due a connectivity check without removing it
// from the pending list.
func (l *nodeValueList[K]) PeekNext(ts time.Time) (*nodeValue[K], bool) {
	if len(*l.pending) == 0 {
		return nil, false
	}

	nve := (*l.pending)[0]

	// Is the check due yet?
	if nve.nv.NextCheckDue.After(ts) {
		return nil, false
	}

	return (*l.pending)[0].nv, true
}

// MarkOngoing marks a node as having an ongoing connectivity check.
// It has no effect if the node is not already present in the list.
func (l *nodeValueList[K]) MarkOngoing(n kad.NodeID[K], deadline time.Time) {
	mk := key.HexString(n.Key())
	nve, ok := l.nodes[mk]
	if !ok {
		return
	}
	nve.nv.CheckDeadline = deadline
	l.nodes[mk] = nve
	heap.Remove(l.pending, nve.index)
	l.ongoing = append(l.ongoing, nve.nv.NodeID)
}

// nodeValuePendingList is a min-heap of NodeValue ordered by NextCheckDue
type nodeValuePendingList[K kad.Key[K]] []*nodeValueEntry[K]

func (o nodeValuePendingList[K]) Len() int { return len(o) }
func (o nodeValuePendingList[K]) Less(i, j int) bool {
	// if due times are equal, then sort higher cpls first
	if o[i].nv.NextCheckDue.Equal(o[j].nv.NextCheckDue) {
		return o[i].nv.Cpl > o[j].nv.Cpl
	}

	return o[i].nv.NextCheckDue.Before(o[j].nv.NextCheckDue)
}

func (o nodeValuePendingList[K]) Swap(i, j int) {
	o[i], o[j] = o[j], o[i]
	o[i].index = i
	o[j].index = j
}

func (o *nodeValuePendingList[K]) Push(x any) {
	n := len(*o)
	v := x.(*nodeValueEntry[K])
	v.index = n
	*o = append(*o, v)
}

func (o *nodeValuePendingList[K]) Pop() any {
	if len(*o) == 0 {
		return nil
	}
	old := *o
	n := len(old)
	v := old[n-1]
	old[n-1] = nil
	v.index = -1
	*o = old[0 : n-1]
	return v
}
