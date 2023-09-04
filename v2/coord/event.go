package coord

import (
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/plprobelab/go-kademlia/kad"
	"github.com/plprobelab/go-kademlia/key"
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

type EventDhtStartBootstrap struct {
	ProtocolID address.ProtocolID
	Message    kad.Request[key.Key256, ma.Multiaddr]
	SeedNodes  []peer.ID // TODO: peer.AddrInfo
}

func (EventDhtStartBootstrap) dhtEvent()   {}
func (EventDhtStartBootstrap) dhtCommand() {}

type EventOutboundGetClosestNodes struct {
	QueryID query.QueryID
	To      peer.AddrInfo
	Target  key.Key256
	Notify  Notify[DhtEvent]
}

func (EventOutboundGetClosestNodes) dhtEvent()           {}
func (EventOutboundGetClosestNodes) nodeHandlerRequest() {}

type EventStartQuery struct {
	QueryID           query.QueryID
	Target            key.Key256
	ProtocolID        address.ProtocolID
	Message           kad.Request[key.Key256, ma.Multiaddr]
	KnownClosestNodes []peer.ID
	Notify            NotifyCloser[DhtEvent]
}

func (EventStartQuery) dhtEvent()   {}
func (EventStartQuery) dhtCommand() {}

type EventStopQuery struct {
	QueryID query.QueryID
}

func (EventStopQuery) dhtEvent()   {}
func (EventStopQuery) dhtCommand() {}

type EventDhtAddNodeInfo struct {
	NodeInfo peer.AddrInfo
}

func (EventDhtAddNodeInfo) dhtEvent()   {}
func (EventDhtAddNodeInfo) dhtCommand() {}

type EventGetCloserNodesSuccess struct {
	QueryID     query.QueryID
	To          peer.AddrInfo
	Target      key.Key256
	CloserNodes []peer.AddrInfo
}

func (EventGetCloserNodesSuccess) dhtEvent()            {}
func (EventGetCloserNodesSuccess) nodeHandlerResponse() {}

type EventGetCloserNodesFailure struct {
	QueryID query.QueryID
	To      peer.AddrInfo
	Target  key.Key256
	Err     error
}

func (EventGetCloserNodesFailure) dhtEvent()            {}
func (EventGetCloserNodesFailure) nodeHandlerResponse() {}

// EventQueryProgressed is emitted by the dht when a query has received a
// response from a node.
type EventQueryProgressed struct {
	QueryID  query.QueryID
	NodeID   peer.ID
	Response kad.Response[key.Key256, ma.Multiaddr]
	Stats    query.QueryStats
}

func (*EventQueryProgressed) dhtEvent() {}

// EventQueryFinished is emitted by the dht when a query has finished, either through
// running to completion or by being canceled.
type EventQueryFinished struct {
	QueryID query.QueryID
	Stats   query.QueryStats
}

func (*EventQueryFinished) dhtEvent() {}

// EventRoutingUpdated is emitted by the dht when a new node has been verified and added to the routing table.
type EventRoutingUpdated struct {
	NodeInfo kad.NodeInfo[key.Key256, ma.Multiaddr]
}

func (*EventRoutingUpdated) dhtEvent()                 {}
func (*EventRoutingUpdated) routingNotificationEvent() {}

// EventBootstrapFinished is emitted by the dht when a bootstrap has finished, either through
// running to completion or by being canceled.
type EventBootstrapFinished struct {
	Stats query.QueryStats
}

func (*EventBootstrapFinished) dhtEvent()                 {}
func (*EventBootstrapFinished) routingNotificationEvent() {}
