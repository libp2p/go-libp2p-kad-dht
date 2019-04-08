package persist

import (
	"context"
	"math/rand"

	"github.com/libp2p/go-libp2p-host"
	kbucket "github.com/libp2p/go-libp2p-kbucket"
	peer "github.com/libp2p/go-libp2p-peer"
	"github.com/libp2p/go-libp2p-peerstore"

	logging "github.com/ipfs/go-log"
)

var logSeed = logging.Logger("dht/seeder")

type Seeder interface {
	// Seeds a routing table provided:
	//  * a set of nillable candidates from a previous snapshot.
	//  * a set of fallback peers.
	//
	// No guarantees are made about the availability of addresses for any peers in the peerstore.
	Seed(into *kbucket.RoutingTable, candidates []peer.ID, fallback []peerstore.PeerInfo) error
}

type randomSeeder struct {
	host   host.Host
	target int
}

var _ Seeder = (*randomSeeder)(nil)

// NewRandomSeeder returns a Seeder that seeds a routing table with `target` random peers from the supplied candidate
// set, resorting to fallback peers if the candidates are unworkable.
//
// The caller must guarantee that fallback peers are available in the peerstore.
func NewRandomSeeder(host host.Host, target int) Seeder {
	return &randomSeeder{host, target}
}

// TODO TODO TODO
//  WIP not finished.
func (rs *randomSeeder) Seed(into *kbucket.RoutingTable, candidates []peer.ID, fallback []peerstore.PeerInfo) error {
	cpy := make([]peer.ID, len(candidates))
	copy(cpy, candidates)
	rand.Shuffle(len(cpy), func(i, j int) {
		cpy[i], cpy[j] = cpy[j], cpy[i]
	})

	left := rs.target
	// TODO: do this in parallel via worker threads.
	//  Could use todocounter.
	//  Currently using background context, ensure proper timeouts.
	//  Use fallback peers.
	for _, p := range cpy {
		if addrs := rs.host.Peerstore().Addrs(p); len(addrs) == 0 {
			// we have no addresses for this peer.
			continue
		}

		if _, err := rs.host.Network().DialPeer(context.Background(), p); err != nil {
			// peer no longer reachable.
			continue
		}

		// we evicted a peer to add this one, so the added count has not changed.
		if evicted, err := into.Update(p); evicted != "" {
			continue
		} else if err != nil {
			logSeed.Warningf("error while adding candidate to routing table: %s", err)
			continue
		}

		left--
		if left == 0 {
			break
		}
	}

	return nil
}
