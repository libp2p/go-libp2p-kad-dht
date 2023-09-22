package brdcst

import (
	"github.com/plprobelab/go-kademlia/kad"

	"github.com/libp2p/go-libp2p-kad-dht/v2/internal/coord/coordt"
)

// BroadcastState must be implemented by all states that a [Broadcast] state
// machine can reach. There are multiple different broadcast state machines that
// all have in common to "emit" a [BroadcastState] and accept a
// [BroadcastEvent]. Recall, states are basically the "events" that a state
// machine emits which other state machines or behaviours could react upon.
type BroadcastState interface {
	broadcastState()
}

// StateBroadcastFindCloser indicates to the broadcast [Pool] or any other upper
// layer that a [Broadcast] state machine wants to query the given node (NodeID)
// for closer nodes to the target key (Target).
type StateBroadcastFindCloser[K kad.Key[K], N kad.NodeID[K]] struct {
	QueryID coordt.QueryID // the id of the broadcast operation that wants to send the message
	NodeID  N              // the node to send the message to
	Target  K              // the key that the query wants to find closer nodes for
}

// StateBroadcastStoreRecord indicates to the broadcast [Pool] or any other
// upper layer that a [Broadcast] state machine wants to store a record using
// the given Message with the given NodeID.
type StateBroadcastStoreRecord[K kad.Key[K], N kad.NodeID[K], M coordt.Message] struct {
	QueryID coordt.QueryID // the id of the broadcast operation that wants to send the message
	NodeID  N              // the node to send the message to
	Message M              // the message the broadcast behaviour wants to send
}

// StateBroadcastWaiting indicates that a [Broadcast] state machine is waiting
// for network I/O to finish. It means the state machine isn't idle, but that
// there are operations in-flight that it is waiting on to finish.
type StateBroadcastWaiting struct {
	QueryID coordt.QueryID // the id of the broadcast operation that is waiting
}

// StateBroadcastFinished indicates that a [Broadcast] state machine has
// finished its operation. During that operation, all nodes in Contacted have
// been contacted to store the record. The Contacted slice does not contain
// the nodes we have queried to find the closest nodes to the target key - only
// the ones that we eventually contacted to store the record. The Errors map
// maps the string representation of any node N in the Contacted slice to a
// potential error struct that contains the original Node and error. In the best
// case, this Errors map is empty.
type StateBroadcastFinished[K kad.Key[K], N kad.NodeID[K]] struct {
	QueryID   coordt.QueryID      // the id of the broadcast operation that has finished
	Contacted []N                 // all nodes we contacted to store the record (successful or not)
	Errors    map[string]struct { // any error that occurred for any node that we contacted
		Node N     // a node from the Contacted slice
		Err  error // the error that happened when contacting that Node
	}
}

// StateBroadcastIdle means that a [Broadcast] state machine has finished all of
// its operation. This state will be emitted if the state machine is polled to
// advance its state but has already finished its operation. The last meaningful
// state will be [StateBroadcastFinished]. Being idle is different from waiting
// for network I/O to finish (see [StateBroadcastWaiting]).
type StateBroadcastIdle struct{}

func (*StateBroadcastFindCloser[K, N]) broadcastState()     {}
func (*StateBroadcastStoreRecord[K, N, M]) broadcastState() {}
func (*StateBroadcastWaiting) broadcastState()              {}
func (*StateBroadcastFinished[K, N]) broadcastState()       {}
func (*StateBroadcastIdle) broadcastState()                 {}

// BroadcastEvent is an event intended to advance the state of a [Broadcast]
// state machine. [Broadcast] state machines only operate on events that
// implement this interface. An "Event" is the opposite of a "State." An "Event"
// flows into the state machine and a "State" flows out of it.
//
// Currently, there are the [FollowUp] and [Optimistic] state machines.
type BroadcastEvent interface {
	broadcastEvent()
}

// EventBroadcastPoll is an event that signals a [Broadcast] state machine that
// it can perform housekeeping work such as time out queries.
type EventBroadcastPoll struct{}

// EventBroadcastStart is an event that instructs a broadcast state machine to
// start the operation.
type EventBroadcastStart[K kad.Key[K], N kad.NodeID[K]] struct {
	Target K   // the key we want to store the record for
	Seed   []N // the closest nodes we know so far and from where we start the operation
}

// EventBroadcastStop notifies a [Broadcast] state machine to stop the
// operation. This comprises all in-flight queries.
type EventBroadcastStop struct{}

// EventBroadcastNodeResponse notifies a [Broadcast] state machine that a remote
// node (NodeID) has successfully responded with closer nodes (CloserNodes) to
// the Target key that's stored on the [Broadcast] state machine
type EventBroadcastNodeResponse[K kad.Key[K], N kad.NodeID[K]] struct {
	NodeID      N   // the node the message was sent to and that replied
	CloserNodes []N // the closer nodes sent by the node
}

// EventBroadcastNodeFailure notifies a [Broadcast] state machine that a remote
// node (NodeID) has failed responding with closer nodes to the target key.
type EventBroadcastNodeFailure[K kad.Key[K], N kad.NodeID[K]] struct {
	NodeID N     // the node the message was sent to and that has replied
	Error  error // the error that caused the failure, if any
}

// EventBroadcastStoreRecordSuccess notifies a broadcast [Broadcast] state
// machine that storing a record with a remote node (NodeID) was successful. The
// message that was sent is held in Request, and the returned value is contained
// in Response. However, in the case of the Amino DHT, nodes do not respond with
// a confirmation, so Response will always be nil. Check out
// [pb.Message.ExpectResponse] for information about which requests should
// receive a response.
type EventBroadcastStoreRecordSuccess[K kad.Key[K], N kad.NodeID[K], M coordt.Message] struct {
	NodeID   N // the node the message was sent to
	Request  M // the message that was sent to the remote node
	Response M // the reply we got from the remote node (nil in many cases of the Amino DHT)
}

// EventBroadcastStoreRecordFailure notifies a broadcast [Broadcast] state
// machine that storing a record with a remote node (NodeID) has failed. The
// message that was sent is held in Request, and the error will be in Error.
type EventBroadcastStoreRecordFailure[K kad.Key[K], N kad.NodeID[K], M coordt.Message] struct {
	NodeID  N     // the node the message was sent to
	Request M     // the message that was sent to the remote node
	Error   error // the error that caused the failure, if any
}

// broadcastEvent() ensures that only events accepted by a [Broadcast] state
// machine can be assigned to the [BroadcastEvent] interface.
func (*EventBroadcastStop) broadcastEvent()                        {}
func (*EventBroadcastPoll) broadcastEvent()                        {}
func (*EventBroadcastStart[K, N]) broadcastEvent()                 {}
func (*EventBroadcastNodeResponse[K, N]) broadcastEvent()          {}
func (*EventBroadcastNodeFailure[K, N]) broadcastEvent()           {}
func (*EventBroadcastStoreRecordSuccess[K, N, M]) broadcastEvent() {}
func (*EventBroadcastStoreRecordFailure[K, N, M]) broadcastEvent() {}
