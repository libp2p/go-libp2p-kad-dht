package brdcst

import (
	"github.com/plprobelab/go-kademlia/kad"

	"github.com/libp2p/go-libp2p-kad-dht/v2/internal/coord/query"
)

// BroadcastEvent is an event intended to advance the state of a broadcast state machine.
type BroadcastEvent interface {
	broadcastEvent()
}

// EventBroadcastPoll is an event that signals the broadcast state machine that
// it can perform housekeeping work such as time out queries.
type EventBroadcastPoll struct{}

// EventBroadcastStart is an event that attempts to start a new broadcast
type EventBroadcastStart[K kad.Key[K], N kad.NodeID[K]] struct {
	QueryID           query.QueryID
	Target            K
	KnownClosestNodes []N
	Strategy          Strategy
}

// EventBroadcastStop notifies a [Broadcast] to stop a query.
type EventBroadcastStop struct {
	QueryID query.QueryID // the id of the query that should be stopped
}

type EventBroadcastNodeResponse[K kad.Key[K], N kad.NodeID[K]] struct {
	QueryID     query.QueryID // the id of the query that sent the message
	NodeID      N             // the node the message was sent to
	CloserNodes []N           // the closer nodes sent by the node
}

// EventBroadcastNodeFailure notifies a [Broadcast] that an attempt to contact a node has failed.
type EventBroadcastNodeFailure[K kad.Key[K], N kad.NodeID[K]] struct {
	QueryID query.QueryID // the id of the query that sent the message
	NodeID  N             // the node the message was sent to
	Error   error         // the error that caused the failure, if any
}

// broadcastEvent() ensures that only events accepted by a [Broadcast] can be assigned to the [BroadcastEvent] interface.
func (*EventBroadcastStop) broadcastEvent()               {}
func (*EventBroadcastPoll) broadcastEvent()               {}
func (*EventBroadcastStart[K, N]) broadcastEvent()        {}
func (*EventBroadcastNodeResponse[K, N]) broadcastEvent() {}
func (*EventBroadcastNodeFailure[K, N]) broadcastEvent()  {}
