package dht

import (
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-kbucket"
)

type RoutingTable interface {
	Update(p peer.ID) (evicted peer.ID, err error)
	Remove(p peer.ID)
	Find(id peer.ID) peer.ID
	NearestPeer(id kbucket.ID) peer.ID
	NearestPeers(id kbucket.ID, count int) []peer.ID
	ListPeers() []peer.ID
	Size() int
	Print()
}
