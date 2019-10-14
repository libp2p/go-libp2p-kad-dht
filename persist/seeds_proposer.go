package persist

import (
	"math/rand"

	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-kbucket"
)

var DefaultRndSeederTarget = 35

type randomSeedsProposer struct {
	host   host.Host
	target int
}

var _ SeedsProposer = (*randomSeedsProposer)(nil)

// NewRandomSeedsProposer returns a SeedsProposer that proposes seeds for the routing table
// till the routing table has atleast 'target' peers or it has NO more seeds to propose.
// It proposes random seeds from the supplied candidate set, resorting to fallback peers if the candidates are unworkable.
// The fallback peers are guaranteed to exist in the peerstore.
func NewRandomSeedsProposer(host host.Host, target int) SeedsProposer {
	return &randomSeedsProposer{host, target}
}

func (rs *randomSeedsProposer) Propose(rt *kbucket.RoutingTable, candidates []peer.ID, fallback []peer.ID) []peer.ID {
	// return if RT already has atleast 'target' peers
	if rt.Size() >= rs.target {
		logSeedProposer.Infof("not returning any proposals as RT already has %d peers and target was %d", rt.Size(), rs.target)
		return nil
	}

	// return if both candidates and fallback peers are empty
	if len(candidates) == 0 && len(fallback) == 0 {
		logSeedProposer.Info("not returning any proposals as both candidate & fallback peer set is empty")
		return nil
	}

	// copy the candidates & shuffle to randomize seeding
	cpy := make([]peer.ID, len(candidates))
	copy(cpy, candidates)
	rand.Shuffle(len(cpy), func(i, j int) {
		cpy[i], cpy[j] = cpy[j], cpy[i]
	})

	// left is the maximum number of peers we should "try" to propose.
	// Note that the number of peers in the RT can change while we are proposing
	// if the connection notification service has already been started because it adds peers to the  RT on every connection.
	// so, this is really on a best effort basis.
	left := rs.target - rt.Size()

	findEligible := func(peers []peer.ID) []peer.ID {
		var eligiblePeers []peer.ID
		for _, p := range peers {
			if left <= 0 {
				return eligiblePeers
			}

			if rt.Find(p) == p {
				logSeedProposer.Info("discarding candidate as it is already in the RT: %s", p)
				continue
			}

			if addrs := rs.host.Peerstore().Addrs(p); len(addrs) == 0 {
				logSeedProposer.Infof("discarding candidate as we no longer have addresses: %s", p)
				continue
			}

			eligiblePeers = append(eligiblePeers, p)
			left--
		}
		return eligiblePeers
	}

	// attempt to propose peers from among the candidates
	ps := findEligible(cpy)
	// use fallback peers if we haven't hit target
	if left > 0 {
		return append(ps, findEligible(fallback)...)
	}
	return ps
}
