package kademlia

import (
	"github.com/plprobelab/go-kademlia/kad"
	"github.com/plprobelab/go-kademlia/network/address"
	"github.com/plprobelab/go-kademlia/query"
)

type DhtEvent interface {
	dhtEvent()
}

type DhtCommand interface {
	DhtEvent
	dhtCommand()
}

type NodeHandlerRequest interface {
	DhtEvent
	nodeHandlerRequest()
}

type NodeHandlerResponse interface {
	DhtEvent
	nodeHandlerResponse()
}

type RoutingNotification interface {
	DhtEvent
	routingNotificationEvent()
}

type EventDhtStartBootstrap[K kad.Key[K], A kad.Address[A]] struct {
	ProtocolID address.ProtocolID
	Message    kad.Request[K, A]
	SeedNodes  []kad.NodeID[K]
}

func (EventDhtStartBootstrap[K, A]) dhtEvent()   {}
func (EventDhtStartBootstrap[K, A]) dhtCommand() {}

type EventOutboundGetClosestNodes[K kad.Key[K], A kad.Address[A]] struct {
	QueryID query.QueryID
	To      kad.NodeInfo[K, A]
	Target  K
	Notify  Notify[DhtEvent]
}

func (EventOutboundGetClosestNodes[K, A]) dhtEvent()           {}
func (EventOutboundGetClosestNodes[K, A]) nodeHandlerRequest() {}

type EventStartQuery[K kad.Key[K], A kad.Address[A]] struct {
	QueryID           query.QueryID
	Target            K
	ProtocolID        address.ProtocolID
	Message           kad.Request[K, A]
	KnownClosestNodes []kad.NodeID[K]
	Notify            NotifyCloser[DhtEvent]
}

func (EventStartQuery[K, A]) dhtEvent()   {}
func (EventStartQuery[K, A]) dhtCommand() {}

type EventStopQuery struct {
	QueryID query.QueryID
}

func (EventStopQuery) dhtEvent()   {}
func (EventStopQuery) dhtCommand() {}

type EventDhtAddNodeInfo[K kad.Key[K], A kad.Address[A]] struct {
	NodeInfo kad.NodeInfo[K, A]
}

func (EventDhtAddNodeInfo[K, A]) dhtEvent()   {}
func (EventDhtAddNodeInfo[K, A]) dhtCommand() {}

type EventGetClosestNodesSuccess[K kad.Key[K], A kad.Address[A]] struct {
	QueryID      query.QueryID
	To           kad.NodeInfo[K, A]
	Target       K
	ClosestNodes []kad.NodeInfo[K, A]
}

func (EventGetClosestNodesSuccess[K, A]) dhtEvent()            {}
func (EventGetClosestNodesSuccess[K, A]) nodeHandlerResponse() {}

type EventGetClosestNodesFailure[K kad.Key[K], A kad.Address[A]] struct {
	QueryID query.QueryID
	To      kad.NodeInfo[K, A]
	Target  K
	Err     error
}

func (EventGetClosestNodesFailure[K, A]) dhtEvent()            {}
func (EventGetClosestNodesFailure[K, A]) nodeHandlerResponse() {}

// EventQueryProgressed is emitted by the dht when a query has received a
// response from a node.
type EventQueryProgressed[K kad.Key[K], A kad.Address[A]] struct {
	QueryID  query.QueryID
	NodeID   kad.NodeID[K]
	Response kad.Response[K, A]
	Stats    query.QueryStats
}

func (*EventQueryProgressed[K, A]) dhtEvent() {}

// EventQueryFinished is emitted by the dht when a query has finished, either through
// running to completion or by being canceled.
type EventQueryFinished struct {
	QueryID query.QueryID
	Stats   query.QueryStats
}

func (*EventQueryFinished) dhtEvent() {}

// EventRoutingUpdated is emitted by the dht when a new node has been verified and added to the routing table.
type EventRoutingUpdated[K kad.Key[K], A kad.Address[A]] struct {
	NodeInfo kad.NodeInfo[K, A]
}

func (*EventRoutingUpdated[K, A]) dhtEvent()                 {}
func (*EventRoutingUpdated[K, A]) routingNotificationEvent() {}

// EventBootstrapFinished is emitted by the dht when a bootstrap has finished, either through
// running to completion or by being canceled.
type EventBootstrapFinished struct {
	Stats query.QueryStats
}

func (*EventBootstrapFinished) dhtEvent()                 {}
func (*EventBootstrapFinished) routingNotificationEvent() {}
