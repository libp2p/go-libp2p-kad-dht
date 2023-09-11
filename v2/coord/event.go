package coord

import (
	"time"

	"github.com/libp2p/go-libp2p-kad-dht/v2/kadt"

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
	Message    kad.Request[kadt.Key, kadt.PeerID]
	SeedNodes  []kadt.PeerID
}

func (*EventStartBootstrap) behaviourEvent() {}
func (*EventStartBootstrap) routingCommand() {}

type EventOutboundGetCloserNodes struct {
	QueryID query.QueryID
	To      kadt.PeerID
	Target  kadt.Key
	Notify  Notify[BehaviourEvent]
}

func (*EventOutboundGetCloserNodes) behaviourEvent()     {}
func (*EventOutboundGetCloserNodes) nodeHandlerRequest() {}
func (*EventOutboundGetCloserNodes) networkCommand()     {}

type EventStartQuery struct {
	QueryID           query.QueryID
	Target            kadt.Key
	ProtocolID        address.ProtocolID
	Message           kad.Request[kadt.Key, kadt.PeerID]
	KnownClosestNodes []kadt.PeerID
	Notify            NotifyCloser[BehaviourEvent]
}

func (*EventStartQuery) behaviourEvent() {}
func (*EventStartQuery) queryCommand()   {}

type EventStopQuery struct {
	QueryID query.QueryID
}

func (*EventStopQuery) behaviourEvent() {}
func (*EventStopQuery) queryCommand()   {}

type EventAddNode struct {
	NodeID kadt.PeerID
	TTL    time.Duration
}

func (*EventAddNode) behaviourEvent() {}
func (*EventAddNode) routingCommand() {}

type EventGetCloserNodesSuccess struct {
	QueryID     query.QueryID
	To          kadt.PeerID
	Target      kadt.Key
	CloserNodes []kadt.PeerID
}

func (*EventGetCloserNodesSuccess) behaviourEvent()      {}
func (*EventGetCloserNodesSuccess) nodeHandlerResponse() {}

type EventGetCloserNodesFailure struct {
	QueryID query.QueryID
	To      kadt.PeerID
	Target  kadt.Key
	Err     error
}

func (*EventGetCloserNodesFailure) behaviourEvent()      {}
func (*EventGetCloserNodesFailure) nodeHandlerResponse() {}

// EventQueryProgressed is emitted by the coordinator when a query has received a
// response from a node.
type EventQueryProgressed struct {
	QueryID  query.QueryID
	NodeID   kadt.PeerID
	Response kad.Response[kadt.Key, kadt.PeerID]
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
	NodeID kadt.PeerID
}

func (*EventRoutingUpdated) behaviourEvent()      {}
func (*EventRoutingUpdated) routingNotification() {}

// EventBootstrapFinished is emitted by the coordinator when a bootstrap has finished, either through
// running to completion or by being canceled.
type EventBootstrapFinished struct {
	Stats query.QueryStats
}

func (*EventBootstrapFinished) behaviourEvent()      {}
func (*EventBootstrapFinished) routingNotification() {}
