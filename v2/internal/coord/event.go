package coord

import (
	"github.com/libp2p/go-libp2p-kad-dht/v2/internal/coord/query"
	"github.com/libp2p/go-libp2p-kad-dht/v2/kadt"
	"github.com/libp2p/go-libp2p-kad-dht/v2/pb"
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

// BrdcstCommand is a type of [BehaviourEvent] that instructs a [BrdcstBehaviour] to perform an action.
type BrdcstCommand interface {
	BehaviourEvent
	brdcstCommand()
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
	SeedNodes []kadt.PeerID
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

type EventOutboundSendMessage struct {
	QueryID query.QueryID
	To      kadt.PeerID
	Message *pb.Message
	Notify  Notify[BehaviourEvent]
}

func (*EventOutboundSendMessage) behaviourEvent()     {}
func (*EventOutboundSendMessage) nodeHandlerRequest() {}
func (*EventOutboundSendMessage) networkCommand()     {}

type EventStartMessageQuery struct {
	QueryID           query.QueryID
	Target            kadt.Key
	Message           *pb.Message
	KnownClosestNodes []kadt.PeerID
	Notify            NotifyCloser[BehaviourEvent]
	NumResults        int // the minimum number of nodes to successfully contact before considering iteration complete
}

func (*EventStartMessageQuery) behaviourEvent() {}
func (*EventStartMessageQuery) queryCommand()   {}

type EventStartFindCloserQuery struct {
	QueryID           query.QueryID
	Target            kadt.Key
	KnownClosestNodes []kadt.PeerID
	Notify            NotifyCloser[BehaviourEvent]
	NumResults        int // the minimum number of nodes to successfully contact before considering iteration complete
}

func (*EventStartFindCloserQuery) behaviourEvent() {}
func (*EventStartFindCloserQuery) queryCommand()   {}

type EventStopQuery struct {
	QueryID query.QueryID
}

func (*EventStopQuery) behaviourEvent() {}
func (*EventStopQuery) queryCommand()   {}

// EventAddNode notifies the routing behaviour of a potential new peer.
type EventAddNode struct {
	NodeID kadt.PeerID
}

func (*EventAddNode) behaviourEvent() {}
func (*EventAddNode) routingCommand() {}

// EventGetCloserNodesSuccess notifies a behaviour that a GetCloserNodes request, initiated by an
// [EventOutboundGetCloserNodes] event has produced a successful response.
type EventGetCloserNodesSuccess struct {
	QueryID     query.QueryID
	To          kadt.PeerID // To is the peer that the GetCloserNodes request was sent to.
	Target      kadt.Key
	CloserNodes []kadt.PeerID
}

func (*EventGetCloserNodesSuccess) behaviourEvent()      {}
func (*EventGetCloserNodesSuccess) nodeHandlerResponse() {}

// EventGetCloserNodesFailure notifies a behaviour that a GetCloserNodes request, initiated by an
// [EventOutboundGetCloserNodes] event has failed to produce a valid response.
type EventGetCloserNodesFailure struct {
	QueryID query.QueryID
	To      kadt.PeerID // To is the peer that the GetCloserNodes request was sent to.
	Target  kadt.Key
	Err     error
}

func (*EventGetCloserNodesFailure) behaviourEvent()      {}
func (*EventGetCloserNodesFailure) nodeHandlerResponse() {}

// EventSendMessageSuccess notifies a behaviour that a SendMessage request, initiated by an
// [EventOutboundSendMessage] event has produced a successful response.
type EventSendMessageSuccess struct {
	QueryID     query.QueryID
	To          kadt.PeerID // To is the peer that the SendMessage request was sent to.
	Response    *pb.Message
	CloserNodes []kadt.PeerID
}

func (*EventSendMessageSuccess) behaviourEvent()      {}
func (*EventSendMessageSuccess) nodeHandlerResponse() {}

// EventSendMessageFailure notifies a behaviour that a SendMessage request, initiated by an
// [EventOutboundSendMessage] event has failed to produce a valid response.
type EventSendMessageFailure struct {
	QueryID query.QueryID
	To      kadt.PeerID // To is the peer that the SendMessage request was sent to.
	Target  kadt.Key
	Err     error
}

func (*EventSendMessageFailure) behaviourEvent()      {}
func (*EventSendMessageFailure) nodeHandlerResponse() {}

// EventQueryProgressed is emitted by the coordinator when a query has received a
// response from a node.
type EventQueryProgressed struct {
	QueryID  query.QueryID
	NodeID   kadt.PeerID
	Response *pb.Message
	Stats    query.QueryStats
}

func (*EventQueryProgressed) behaviourEvent() {}

// EventQueryFinished is emitted by the coordinator when a query has finished, either through
// running to completion or by being canceled.
type EventQueryFinished struct {
	QueryID      query.QueryID
	Stats        query.QueryStats
	ClosestNodes []kadt.PeerID
}

func (*EventQueryFinished) behaviourEvent() {}

// EventRoutingUpdated is emitted by the coordinator when a new node has been verified and added to the routing table.
type EventRoutingUpdated struct {
	NodeID kadt.PeerID
}

func (*EventRoutingUpdated) behaviourEvent()      {}
func (*EventRoutingUpdated) routingNotification() {}

// EventRoutingRemoved is emitted by the coordinator when new node has been removed from the routing table.
type EventRoutingRemoved struct {
	NodeID kadt.PeerID
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
	NodeID kadt.PeerID
}

func (*EventNotifyConnectivity) behaviourEvent()      {}
func (*EventNotifyConnectivity) routingNotification() {}

// EventNotifyNonConnectivity notifies a behaviour that a peer does not have connectivity and/or does not support
// finding closer nodes is known.
type EventNotifyNonConnectivity struct {
	NodeID kadt.PeerID
}

func (*EventNotifyNonConnectivity) behaviourEvent() {}
func (*EventNotifyNonConnectivity) routingCommand() {}

// EventBroadcastFinished is emitted by the coordinator when a broadcasting
// a record to the network has finished, either through running to completion or
// by being canceled.
type EventBroadcastFinished struct {
	QueryID query.QueryID
	Stats   query.QueryStats
}

func (*EventBroadcastFinished) behaviourEvent() {}
