package persist

import (
	"context"
	"math/rand"

	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-kbucket"
)

type randomSeedsProposer struct{}

var _ SeedsProposer = (*randomSeedsProposer)(nil)

// NewRandomSeedsProposer returns a SeedsProposer that proposes seeds for the routing table
// It proposes random seeds from the supplied candidates & fallback peer set, prioritizing candidates over fallbacks
// The fallback peers are guaranteed to exist in the peerstore.
func NewRandomSeedsProposer() SeedsProposer {
	return &randomSeedsProposer{}
}

func (rs *randomSeedsProposer) Propose(ctx context.Context, rt *kbucket.RoutingTable, candidates []peer.ID, fallback []peer.ID) chan peer.ID {
	peerChan := make(chan peer.ID)

	go func() {
		defer close(peerChan)

		// return if both candidates and fallback peers are empty
		if len(candidates) == 0 && len(fallback) == 0 {
			logSeedProposer.Info("not returning any proposals as both candidate & fallback peer set is empty")
			return
		}

		// copy the candidates & shuffle to randomize seeding
		cpy := make([]peer.ID, len(candidates))
		copy(cpy, candidates)
		rand.Shuffle(len(cpy), func(i, j int) {
			cpy[i], cpy[j] = cpy[j], cpy[i]
		})

		for _, p := range append(cpy, fallback...) {
			select {
			case <-ctx.Done():
				return
			case peerChan <- p:
			}
		}
	}()

	return peerChan
}
