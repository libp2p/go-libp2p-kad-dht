package persist

import (
	"context"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/test"
	kb "github.com/libp2p/go-libp2p-kbucket"
	"github.com/libp2p/go-libp2p-peerstore"
	swarmt "github.com/libp2p/go-libp2p-swarm/testing"
	bhost "github.com/libp2p/go-libp2p/p2p/host/basic"

	"github.com/stretchr/testify/assert"
)

func TestRandomSeedsProposer(t *testing.T) {
	testCases := map[string]struct {
		nTotalCandidates           int // snapshotted candidate list
		nNonCandidatePeersInRT     int // non candidate peers that are already in the routing table
		nCandidatesNotInPeerStore  int // candidates that are absent in the peer store
		nCandidatesAlreadyInRT     int // candidates that are "not-diallable"/not alive
		nFallbacks                 int // fallback list
		nFallbacksAlreadyInRT      int // fallback peers that are already in RT
		expectedNumPeersInProposal int // number of peers we expect in the routing table after seeding is complete
	}{
		"No Proposals -> RT already has target peers": {
			10, 10, 0, 0, 10, 0,
			0,
		},
		"No Proposals -> candidate & fallback sets are empty": {
			0, 0, 0, 0, 0, 0,
			0,
		},
		"No Proposals -> candidates & fallbacks already in RT": {
			3, 0, 0, 3, 3, 3,
			0,
		},
		"No Proposals -> candidates not in peerstore & fallbacks already in RT": {
			3, 0, 3, 0, 3, 3,
			0,
		},
		"Success -> only candidates": {
			15, 2, 5, 2, 0, 0,
			6,
		},
		"Success -> candidates + fallbacks": {
			5, 2, 1, 1, 1, 0,
			4,
		},
		"Success -> only fallbacks": {
			7, 2, 5, 2, 10, 2,
			4,
		},
	}

	for name, testcase := range testCases {
		// create host for self
		self := bhost.New(swarmt.GenSwarm(t, context.Background(), swarmt.OptDisableReuseport))
		target := 10

		// create candidate hosts & add them to the peer store
		candidateHosts := make([]bhost.BasicHost, testcase.nTotalCandidates)
		candidatePids := make([]peer.ID, testcase.nTotalCandidates)
		for i := 0; i < testcase.nTotalCandidates; i++ {
			h := *bhost.New(swarmt.GenSwarm(t, context.Background(), swarmt.OptDisableReuseport))
			candidateHosts[i] = h
			candidatePids[i] = h.ID()
			self.Peerstore().AddAddrs(candidatePids[i], h.Addrs(), 5*time.Minute)
		}

		// remove the ones that shouldn't be in the peer store
		for i := 0; i < testcase.nCandidatesNotInPeerStore; i++ {
			self.Peerstore().ClearAddrs(candidatePids[i])
		}

		// sanity check, peerstore also has an entry for self
		assert.Len(t, self.Peerstore().Peers(), testcase.nTotalCandidates-testcase.nCandidatesNotInPeerStore+1)

		// create fallback hosts & add them to the peerstore
		fallbackHosts := make([]bhost.BasicHost, testcase.nFallbacks)
		fallbackPids := make([]peer.ID, testcase.nFallbacks)
		for i := 0; i < testcase.nFallbacks; i++ {
			h := *bhost.New(swarmt.GenSwarm(t, context.Background(), swarmt.OptDisableReuseport))
			fallbackHosts[i] = h
			fallbackPids[i] = h.ID()
			self.Peerstore().AddAddrs(fallbackPids[i], h.Addrs(), 5*time.Minute)
		}

		// create RT & fill it with required number of peers
		rt := kb.NewRoutingTable(50, kb.ConvertPeerID(self.ID()), time.Hour, peerstore.NewMetrics())
		for i := 0; i < testcase.nNonCandidatePeersInRT; i++ {
			_, err := rt.Update(test.RandPeerIDFatal(t))
			assert.NoError(t, err, "should not get error while adding peer to RT")
		}
		assert.True(t, rt.Size() == testcase.nNonCandidatePeersInRT)

		// add candidates that are already supposed to be in RT
		for i := testcase.nCandidatesNotInPeerStore; i < testcase.nCandidatesNotInPeerStore+testcase.nCandidatesAlreadyInRT; i++ {
			_, err := rt.Update(candidateHosts[i].ID())
			assert.NoError(t, err)
		}
		assert.True(t, rt.Size() == testcase.nCandidatesAlreadyInRT+testcase.nNonCandidatePeersInRT)

		// add fallbacks that are already supposed to be in RT
		for i := 0; i < testcase.nFallbacksAlreadyInRT; i++ {
			_, err := rt.Update(fallbackHosts[i].ID())
			assert.NoError(t, err)
		}
		assert.True(t, rt.Size() == testcase.nCandidatesAlreadyInRT+testcase.nNonCandidatePeersInRT+testcase.nFallbacksAlreadyInRT)

		// run seeds proposer & assert
		rs := NewRandomSeedsProposer(self, target)
		proposed := rs.Propose(rt, candidatePids, fallbackPids)

		// assert we got required number of proposals
		assert.Equalf(t, testcase.expectedNumPeersInProposal, len(proposed), "for test case %s, proposed set should have %d peers, but has %d",
			name, testcase.expectedNumPeersInProposal, len(proposed))

		// cleanup
		assert.NoError(t, self.Close())
		allHosts := append(fallbackHosts, candidateHosts...)
		for _, h := range allHosts {
			assert.NoError(t, h.Close())
		}
	}

}
