package brdcst

import (
	"github.com/plprobelab/go-kademlia/kad"

	"github.com/libp2p/go-libp2p-kad-dht/v2/internal/coord/query"
)

// BroadcastState is the state of a bootstrap.
type BroadcastState interface {
	broadcastState()
}

type StateBroadcastFindCloser[K kad.Key[K], N kad.NodeID[K]] struct {
	QueryID query.QueryID
	Target  K // the key that the query wants to find closer nodes for
	NodeID  N // the node to send the message to
	Stats   query.QueryStats
}

type StateBroadcastStoreRecord[K kad.Key[K], N kad.NodeID[K]] struct {
	QueryID query.QueryID
	NodeID  N
}

type StateBroadcastWaiting struct{}

type StateBroadcastFinished struct {
	QueryID query.QueryID
	Stats   query.QueryStats
}

type StateBroadcastIdle struct{}

func (*StateBroadcastFindCloser[K, N]) broadcastState()  {}
func (*StateBroadcastStoreRecord[K, N]) broadcastState() {}
func (*StateBroadcastWaiting) broadcastState()           {}
func (*StateBroadcastFinished) broadcastState()          {}
func (*StateBroadcastIdle) broadcastState()              {}

func (*StateBroadcastFindCloser[K, N]) IsTerminal() bool  { return true }
func (*StateBroadcastStoreRecord[K, N]) IsTerminal() bool { return true }
func (*StateBroadcastFinished) IsTerminal() bool          { return true }
func (*StateBroadcastIdle) IsTerminal() bool              { return true }
