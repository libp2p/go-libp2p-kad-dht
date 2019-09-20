package persist

import (
	peer "github.com/libp2p/go-libp2p-core/peer"
	kbucket "github.com/libp2p/go-libp2p-kbucket"

	log "github.com/ipfs/go-log"
)

var logSeed = log.Logger("dht/seeder")
var logSnapshot = log.Logger("dht/snapshot")

type Seeder interface {
	// Seed takes an optional set of candidates from a snapshot (or nil if none could be loaded),
	// and a set of fallback peers, and it seeds a routing table instance with working peers.
	Seed(into *kbucket.RoutingTable, candidates []peer.ID, fallback []peer.ID) error
}

// A Snapshotter provides the ability to save and restore a routing table from a persistent medium.
type Snapshotter interface {
	// Load recovers a snapshot from storage, and returns candidates to integrate in a fresh routing table.
	Load() ([]peer.ID, error)

	// Store persists the current state of the routing table.
	Store(rt *kbucket.RoutingTable) error
}
