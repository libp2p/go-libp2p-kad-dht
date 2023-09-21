package brdcst

import (
	"github.com/plprobelab/go-kademlia/kad"

	"github.com/libp2p/go-libp2p-kad-dht/v2/internal/coord/coordt"
	"github.com/libp2p/go-libp2p-kad-dht/v2/internal/coord/query"
)

// PoolState is the state of a bootstrap.
type PoolState interface {
	poolState()
}

type StatePoolFindCloser[K kad.Key[K], N kad.NodeID[K]] struct {
	QueryID query.QueryID
	Target  K // the key that the query wants to find closer nodes for
	NodeID  N // the node to send the message to
	Stats   query.QueryStats
}

type StatePoolWaiting struct {
	QueryID query.QueryID
}

type StatePoolStoreRecord[K kad.Key[K], N kad.NodeID[K], M coordt.Message] struct {
	QueryID query.QueryID
	NodeID  N
	Message M
}

type StatePoolBroadcastFinished[K kad.Key[K], N kad.NodeID[K]] struct {
	QueryID   query.QueryID
	Contacted []N
	Errors    map[string]struct {
		Node N
		Err  error
	}
}

type StatePoolIdle struct{}

func (*StatePoolFindCloser[K, N]) poolState()        {}
func (*StatePoolWaiting) poolState()                 {}
func (*StatePoolStoreRecord[K, N, M]) poolState()    {}
func (*StatePoolBroadcastFinished[K, N]) poolState() {}
func (*StatePoolIdle) poolState()                    {}
