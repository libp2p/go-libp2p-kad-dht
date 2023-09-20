package brdcst

import (
	"github.com/libp2p/go-libp2p-kad-dht/v2/internal/coord/query"
	"github.com/libp2p/go-libp2p-kad-dht/v2/kadt"
)

// BroadcastState is the state of a bootstrap.
type BroadcastState interface {
	broadcastState()
}

type StateBroadcastFindCloser struct {
	QueryID query.QueryID
	Target  kadt.Key    // the key that the query wants to find closer nodes for
	NodeID  kadt.PeerID // the node to send the message to
	Stats   query.QueryStats
}

func (*StateBroadcastFindCloser) broadcastState() {}

type StateBroadcastFinished struct {
	QueryID query.QueryID
	Stats   query.QueryStats
}

func (*StateBroadcastFinished) broadcastState() {}

type StateBroadcastIdle struct{}

func (*StateBroadcastIdle) broadcastState() {}
