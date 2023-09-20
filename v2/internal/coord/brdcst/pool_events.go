package brdcst

import (
	"github.com/plprobelab/go-kademlia/kad"

	"github.com/libp2p/go-libp2p-kad-dht/v2/internal/coord/query"
)

// PoolEvent is an event intended to advance the state of the [Pool] state
// machine.
type PoolEvent interface {
	poolEvent()
}

// EventPoolPoll is an event that signals the broadcast state machine that
// it can perform housekeeping work such as time out queries.
type EventPoolPoll struct{}

// EventPoolAddBroadcast is an event that attempts to start a new broadcast
type EventPoolAddBroadcast[K kad.Key[K], N kad.NodeID[K]] struct {
	QueryID           query.QueryID
	Target            K
	KnownClosestNodes []N
	Strategy          Strategy
}

// EventPoolStopBroadcast notifies a [Pool] to stop a query.
type EventPoolStopBroadcast struct {
	QueryID query.QueryID // the id of the query that should be stopped
}

type EventPoolNodeResponse[K kad.Key[K], N kad.NodeID[K]] struct {
	QueryID     query.QueryID // the id of the query that sent the message
	NodeID      N             // the node the message was sent to
	CloserNodes []N           // the closer nodes sent by the node
}

// EventPoolNodeFailure notifies a [Pool] that an attempt to contact a node has failed.
type EventPoolNodeFailure[K kad.Key[K], N kad.NodeID[K]] struct {
	QueryID query.QueryID // the id of the query that sent the message
	NodeID  N             // the node the message was sent to
	Error   error         // the error that caused the failure, if any
}

// broadcastEvent() ensures that only events accepted by a [Pool] can be assigned to the [PoolEvent] interface.
func (*EventPoolStopBroadcast) poolEvent()      {}
func (*EventPoolPoll) poolEvent()               {}
func (*EventPoolAddBroadcast[K, N]) poolEvent() {}
func (*EventPoolNodeResponse[K, N]) poolEvent() {}
func (*EventPoolNodeFailure[K, N]) poolEvent()  {}
