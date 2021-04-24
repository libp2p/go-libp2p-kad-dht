package dht

import (
	"github.com/libp2p/go-libp2p-core/routing"
	dhtrouting "github.com/libp2p/go-libp2p-kad-dht/routing"
)

// Quorum is a DHT option that tells the DHT how many peers it needs to get
// values from before returning the best one. Zero means the DHT query
// should complete instead of returning early.
//
// Default: 0
//
// Deprecated: use github.com/libp2p/go-libp2p-kad-dht/routing.Quorum
func Quorum(n int) routing.Option {
	return dhtrouting.Quorum(n)
}
