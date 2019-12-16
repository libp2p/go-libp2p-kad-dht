package persist

import (
	"context"

	peer "github.com/libp2p/go-libp2p-core/peer"
	kbucket "github.com/libp2p/go-libp2p-kbucket"

	log "github.com/ipfs/go-log"
)

var logSnapshot = log.Logger("dht/snapshot")
var logSeedProposer = log.Logger("dht/seeds-proposer")

// A SeedsProposer proposes a set of eligible peers from a given set of candidates & fallback peers
// for seeding the RT.
type SeedsProposer interface {
	// Propose takes an optional set of candidates from a snapshot (or nil if none could be loaded),
	// and a set of fallback peers, and it returns a channel of peers that can be
	// used to seed the given routing table.
	// Returns an empty channel if it has no proposals.
	// Note: Seeding a routing table with the eligible peers will work only if the the dht uses a persistent peerstore across restarts.
	// This is because we can recover metadata for the candidate peers only if the peerstore was/is persistent.
	Propose(ctx context.Context, rt *kbucket.RoutingTable, candidates []peer.ID, fallback []peer.ID) chan peer.ID
}

// A Snapshotter provides the ability to save and restore a routing table from a persistent medium.
type Snapshotter interface {
	// Load recovers a snapshot from storage, and returns candidates to integrate in a fresh routing table.
	Load() ([]peer.ID, error)

	// Store persists the current state of the routing table.
	Store(rt *kbucket.RoutingTable) error
}
