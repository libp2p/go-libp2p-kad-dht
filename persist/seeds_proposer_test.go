package persist

import (
	"context"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/test"
	kb "github.com/libp2p/go-libp2p-kbucket"
	"github.com/libp2p/go-libp2p-peerstore"
	"github.com/stretchr/testify/require"
)

func TestRandomSeedsProposer(t *testing.T) {
	testCases := map[string]struct {
		nTotalCandidates int // snapshotted candidate list
		nFallbacks       int // fallback list

		expectedNumPeersInProposal int // number of proposals we expect
	}{
		"No Proposals -> candidate & fallback sets are empty": {0, 0, 0},

		"Success -> only candidates": {4, 0, 4},

		"Success -> candidates + fallbacks": {4, 5, 9},

		"Success -> only fallbacks": {0, 6, 6},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for name, testcase := range testCases {

		// create candidate hosts & add them to the peer store
		candidatePids := make([]peer.ID, testcase.nTotalCandidates)
		for i := 0; i < testcase.nTotalCandidates; i++ {
			candidatePids[i] = test.RandPeerIDFatal(t)
		}

		fallbackPids := make([]peer.ID, testcase.nFallbacks)
		for i := 0; i < testcase.nFallbacks; i++ {
			fallbackPids[i] = test.RandPeerIDFatal(t)
		}

		// create RT & fill it with required number of peers
		rt := kb.NewRoutingTable(50, kb.ConvertPeerID(test.RandPeerIDFatal(t)), time.Hour, peerstore.NewMetrics())

		// run seeds proposer & assert
		rs := NewRandomSeedsProposer()
		pChan := rs.Propose(ctx, rt, candidatePids, fallbackPids)

		nPeers := 0
		for _ = range pChan {
			nPeers++
		}
		require.Equalf(t, testcase.expectedNumPeersInProposal, nPeers, "expected %d peers in proposal, but got only %d for test %s",
			testcase.expectedNumPeersInProposal, nPeers, name)
	}
}
