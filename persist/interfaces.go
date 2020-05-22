package persist

import (
	"github.com/libp2p/go-libp2p-core/host"

	kbucket "github.com/libp2p/go-libp2p-kbucket"

	"github.com/ipfs/go-log"
)

var logSnapshot = log.Logger("dht/snapshot")

// A Snapshotter provides the ability to save and restore a routing table from a Persistent medium.
type Snapshotter interface {
	// Load recovers a snapshot from storage, and returns candidates to integrate in a fresh routing table.
	Load() ([]*RtSnapshotPeerInfo, error)

	// Store persists the current state of the routing table.
	Store(h host.Host, rt *kbucket.RoutingTable) error
}
