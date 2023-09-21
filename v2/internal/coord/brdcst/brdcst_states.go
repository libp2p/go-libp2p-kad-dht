package brdcst

import (
	"github.com/plprobelab/go-kademlia/kad"

	"github.com/libp2p/go-libp2p-kad-dht/v2/internal/coord/coordt"
	"github.com/libp2p/go-libp2p-kad-dht/v2/internal/coord/query"
)

// BroadcastState is the state of a bootstrap.
type BroadcastState interface {
	broadcastState()
}

type StateBroadcastFindCloser[K kad.Key[K], N kad.NodeID[K]] struct {
	QueryID coordt.QueryID
	Target  K // the key that the query wants to find closer nodes for
	NodeID  N // the node to send the message to
	Stats   query.QueryStats
}

type StateBroadcastStoreRecord[K kad.Key[K], N kad.NodeID[K], M coordt.Message] struct {
	QueryID coordt.QueryID
	NodeID  N
	Message M
}

type StateBroadcastWaiting struct {
	QueryID coordt.QueryID
}

type StateBroadcastFinished[K kad.Key[K], N kad.NodeID[K]] struct {
	QueryID   coordt.QueryID
	Contacted []N
	Errors    map[string]struct {
		Node N
		Err  error
	}
}

type StateBroadcastIdle struct{}

func (*StateBroadcastFindCloser[K, N]) broadcastState()     {}
func (*StateBroadcastStoreRecord[K, N, M]) broadcastState() {}
func (*StateBroadcastWaiting) broadcastState()              {}
func (*StateBroadcastFinished[K, N]) broadcastState()       {}
func (*StateBroadcastIdle) broadcastState()                 {}
