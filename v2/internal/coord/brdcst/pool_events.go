package brdcst

import (
	"github.com/plprobelab/go-kademlia/kad"

	"github.com/libp2p/go-libp2p-kad-dht/v2/internal/coord/coordt"
	"github.com/libp2p/go-libp2p-kad-dht/v2/internal/coord/query"
)

// PoolEvent is an event intended to advance the state of the [Pool] state
// machine. The [Pool] state machine only operates on events that implement
// this interface
type PoolEvent interface {
	poolEvent()
}

// EventPoolPoll is an event that signals the [Pool] state machine that
// it can perform housekeeping work such as time out queries.
type EventPoolPoll struct{}

// EventPoolAddBroadcast is an event that attempts to start a new broadcast
type EventPoolAddBroadcast[K kad.Key[K], N kad.NodeID[K], M coordt.Message] struct {
	QueryID           query.QueryID
	Target            K
	Message           M
	KnownClosestNodes []N
	Strategy          Strategy
}

// EventPoolStopBroadcast notifies a [Pool] to stop a query.
type EventPoolStopBroadcast struct {
	QueryID query.QueryID // the id of the query that should be stopped
}

type EventPoolGetCloserNodesSuccess[K kad.Key[K], N kad.NodeID[K]] struct {
	QueryID     query.QueryID // the id of the query that sent the message
	NodeID      N             // the node the message was sent to
	Target      K
	CloserNodes []N // the closer nodes sent by the node
}

// EventPoolGetCloserNodesFailure notifies a [Pool] that an attempt to contact a node has failed.
type EventPoolGetCloserNodesFailure[K kad.Key[K], N kad.NodeID[K]] struct {
	QueryID query.QueryID // the id of the query that sent the message
	NodeID  N             // the node the message was sent to
	Target  K
	Error   error // the error that caused the failure, if any
}

type EventPoolStoreRecordSuccess[K kad.Key[K], N kad.NodeID[K], M coordt.Message] struct {
	QueryID  query.QueryID // the id of the query that sent the message
	NodeID   N             // the node the message was sent to
	Request  M
	Response M
}

// EventPoolStoreRecordFailure notifies a [Pool] that an attempt to contact a node has failed.
type EventPoolStoreRecordFailure[K kad.Key[K], N kad.NodeID[K], M coordt.Message] struct {
	QueryID query.QueryID // the id of the query that sent the message
	NodeID  N             // the node the message was sent to
	Request M
	Error   error // the error that caused the failure, if any
}

// poolEvent() ensures that only events accepted by a [Pool] can be assigned to
// the [PoolEvent] interface.
func (*EventPoolStopBroadcast) poolEvent()               {}
func (*EventPoolPoll) poolEvent()                        {}
func (*EventPoolAddBroadcast[K, N, M]) poolEvent()       {}
func (*EventPoolGetCloserNodesSuccess[K, N]) poolEvent() {}
func (*EventPoolGetCloserNodesFailure[K, N]) poolEvent() {}
func (*EventPoolStoreRecordSuccess[K, N, M]) poolEvent() {}
func (*EventPoolStoreRecordFailure[K, N, M]) poolEvent() {}
