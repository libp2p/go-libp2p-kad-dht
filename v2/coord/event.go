package coord

import (
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/plprobelab/go-kademlia/kad"
	"github.com/plprobelab/go-kademlia/network/address"
	"github.com/plprobelab/go-kademlia/query"
)

type BehaviourEvent interface {
	behaviourEvent()
}

// RoutingCommand is a type of [BehaviourEvent] that instructs a [RoutingBehaviour] to perform an action.
type RoutingCommand interface {
	BehaviourEvent
	routingCommand()
}

// NetworkCommand is a type of [BehaviourEvent] that instructs a [NetworkBehaviour] to perform an action.
type NetworkCommand interface {
	BehaviourEvent
	networkCommand()
}

// QueryCommand is a type of [BehaviourEvent] that instructs a [QueryBehaviour] to perform an action.
type QueryCommand interface {
	BehaviourEvent
	queryCommand()
}

type NodeHandlerRequest interface {
	BehaviourEvent
	nodeHandlerRequest()
}

type NodeHandlerResponse interface {
	BehaviourEvent
	nodeHandlerResponse()
}

type RoutingNotification interface {
	BehaviourEvent
	routingNotification()
}

type EventStartBootstrap struct {
	ProtocolID address.ProtocolID
	Message    kad.Request[KadKey, ma.Multiaddr]
	SeedNodes  []peer.ID // TODO: peer.AddrInfo
}

func (*EventStartBootstrap) behaviourEvent() {}
func (*EventStartBootstrap) routingCommand() {}

type EventOutboundGetCloserNodes struct {
	QueryID query.QueryID
	To      peer.AddrInfo
	Target  KadKey
	Notify  Notify[BehaviourEvent]
}

func (*EventOutboundGetCloserNodes) behaviourEvent()     {}
func (*EventOutboundGetCloserNodes) nodeHandlerRequest() {}
func (*EventOutboundGetCloserNodes) networkCommand()     {}

type EventStartQuery struct {
	QueryID           query.QueryID
	Target            KadKey
	ProtocolID        address.ProtocolID
	Message           kad.Request[KadKey, ma.Multiaddr]
	KnownClosestNodes []peer.ID
	Notify            NotifyCloser[BehaviourEvent]
}

func (*EventStartQuery) behaviourEvent() {}
func (*EventStartQuery) queryCommand()   {}

type EventStopQuery struct {
	QueryID query.QueryID
}

func (*EventStopQuery) behaviourEvent() {}
func (*EventStopQuery) queryCommand()   {}

// EventAddAddrInfo notifies the routing behaviour of a potential new peer or of additional addresses for
// an existing peer.
type EventAddAddrInfo struct {
	NodeInfo peer.AddrInfo
	TTL      time.Duration
}

func (*EventAddAddrInfo) behaviourEvent() {}
func (*EventAddAddrInfo) routingCommand() {}

// EventGetCloserNodesSuccess notifies a behaviour that a GetCloserNodes request, initiated by an
// [EventOutboundGetCloserNodes] event has produced a successful response.
type EventGetCloserNodesSuccess struct {
	QueryID     query.QueryID
	To          peer.AddrInfo // To is the peer address that the GetCloserNodes request was sent to.
	Target      KadKey
	CloserNodes []peer.AddrInfo
}

func (*EventGetCloserNodesSuccess) behaviourEvent()      {}
func (*EventGetCloserNodesSuccess) nodeHandlerResponse() {}

// EventGetCloserNodesFailure notifies a behaviour that a GetCloserNodes request, initiated by an
// [EventOutboundGetCloserNodes] event has failed to produce a valid response.
type EventGetCloserNodesFailure struct {
	QueryID query.QueryID
	To      peer.AddrInfo // To is the peer address that the GetCloserNodes request was sent to.
	Target  KadKey
	Err     error
}

func (*EventGetCloserNodesFailure) behaviourEvent()      {}
func (*EventGetCloserNodesFailure) nodeHandlerResponse() {}

// EventQueryProgressed is emitted by the coordinator when a query has received a
// response from a node.
type EventQueryProgressed struct {
	QueryID  query.QueryID
	NodeID   peer.ID
	Response kad.Response[KadKey, ma.Multiaddr]
	Stats    query.QueryStats
}

func (*EventQueryProgressed) behaviourEvent() {}

// EventQueryFinished is emitted by the coordinator when a query has finished, either through
// running to completion or by being canceled.
type EventQueryFinished struct {
	QueryID query.QueryID
	Stats   query.QueryStats
}

func (*EventQueryFinished) behaviourEvent() {}

// EventRoutingUpdated is emitted by the coordinator when a new node has been verified and added to the routing table.
type EventRoutingUpdated struct {
	NodeInfo peer.AddrInfo
}

func (*EventRoutingUpdated) behaviourEvent()      {}
func (*EventRoutingUpdated) routingNotification() {}

// EventRoutingRemoved is emitted by the coordinator when new node has been removed from the routing table.
type EventRoutingRemoved struct {
	NodeID peer.ID
}

func (*EventRoutingRemoved) behaviourEvent()      {}
func (*EventRoutingRemoved) routingNotification() {}

// EventBootstrapFinished is emitted by the coordinator when a bootstrap has finished, either through
// running to completion or by being canceled.
type EventBootstrapFinished struct {
	Stats query.QueryStats
}

func (*EventBootstrapFinished) behaviourEvent()      {}
func (*EventBootstrapFinished) routingNotification() {}

// EventNotifyConnectivity notifies a behaviour that a peer's connectivity and support for finding closer nodes
// has been confirmed such as from a successful query response or an inbound query. This should not be used for
// general connections to the host but only when it is confirmed that the peer responds to requests for closer
// nodes.
type EventNotifyConnectivity struct {
	NodeInfo peer.AddrInfo
}

func (*EventNotifyConnectivity) behaviourEvent()      {}
func (*EventNotifyConnectivity) routingNotification() {}

// EventNotifyNonConnectivity notifies a behaviour that a peer does not have connectivity and/or does not support
// finding closer nodes is known.
type EventNotifyNonConnectivity struct {
	NodeID peer.ID
}

func (*EventNotifyNonConnectivity) behaviourEvent() {}
func (*EventNotifyNonConnectivity) routingCommand() {}
