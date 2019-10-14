package dht

import (
	"context"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/test"
	opts "github.com/libp2p/go-libp2p-kad-dht/opts"
	"github.com/libp2p/go-libp2p-kad-dht/persist"
	kb "github.com/libp2p/go-libp2p-kbucket"
	swarmt "github.com/libp2p/go-libp2p-swarm/testing"
	bhost "github.com/libp2p/go-libp2p/p2p/host/basic"

	"github.com/stretchr/testify/assert"
)

// mockSeedsProposer returns first candidate if candidate set is not empty, otherwise tries
// returning first from fallback
type mockSeedsProposer struct {
}

func (m *mockSeedsProposer) Propose(rt *kb.RoutingTable, candidates []peer.ID, fallback []peer.ID) []peer.ID {
	if len(candidates) != 0 {
		return []peer.ID{candidates[0]}
	} else if len(fallback) != 0 {
		return []peer.ID{fallback[0]}
	} else {
		return nil
	}
}

var _ persist.SeedsProposer = (*mockSeedsProposer)(nil)

func TestRTSeeder(t *testing.T) {
	testCases := map[string]struct {
		nTotalCandidates               int // snapshotted candidate list
		nCandidatesNotAlive            int // candidates that are "not-diallable"/not alive
		nFallbacks                     int // fallback list
		expectedNumPeersInRoutingTable int // number of peers we expect in the routing table after seeding is complete
	}{
		"Only Candidates": {10, 1, 0,
			9},

		"Candidates + Fallbacks": {10, 4, 7,
			13},

		"Only Fallbacks": {5, 5, 9,
			9},

		"Empty Candidates": {0, 0, 5,
			5},
	}

	for name, testcase := range testCases {
		// create host for self
		self := bhost.New(swarmt.GenSwarm(t, context.Background(), swarmt.OptDisableReuseport))

		// create candidate hosts & add them to the peer store
		candidateHosts := make([]bhost.BasicHost, testcase.nTotalCandidates)
		candidatePids := make([]peer.ID, testcase.nTotalCandidates)
		for i := 0; i < testcase.nTotalCandidates; i++ {
			h := *bhost.New(swarmt.GenSwarm(t, context.Background(), swarmt.OptDisableReuseport))
			candidateHosts[i] = h
			candidatePids[i] = h.ID()
			self.Peerstore().AddAddrs(candidatePids[i], h.Addrs(), 5*time.Minute)
		}

		// disconnect the number of peers required
		for i := 0; i < testcase.nCandidatesNotAlive; i++ {
			assert.NoError(t, candidateHosts[i].Close())
		}

		// create fallback hosts & add them to the peerstore
		fallbackHosts := make([]bhost.BasicHost, testcase.nFallbacks)
		fallbackPids := make([]peer.ID, testcase.nFallbacks)
		for i := 0; i < testcase.nFallbacks; i++ {
			h := *bhost.New(swarmt.GenSwarm(t, context.Background(), swarmt.OptDisableReuseport))
			fallbackHosts[i] = h
			fallbackPids[i] = h.ID()
			self.Peerstore().AddAddrs(fallbackPids[i], h.Addrs(), 5*time.Minute)
		}

		// crete dht instance with the mock seedsproposer
		// make sure the default fallback peers are not in the peerstore so rt remains empty
		newDht, err := New(context.Background(), self, opts.SeedsProposer(&mockSeedsProposer{}), opts.FallbackPeers([]peer.ID{test.RandPeerIDFatal(t)}))
		assert.NoError(t, err)
		defer newDht.Close()
		assert.True(t, newDht.routingTable.Size() == 0)

		// assert routing table has been seeded with the expected number of peers
		assert.NoError(t, newDht.seedRoutingTable(candidatePids, fallbackPids))
		assert.Equalf(t, testcase.expectedNumPeersInRoutingTable, newDht.routingTable.Size(), "for test case %s, rt should have %d peers, but has %d",
			name, testcase.expectedNumPeersInRoutingTable, newDht.routingTable.Size())

		// cleanup
		assert.NoError(t, self.Close())
		allHosts := append(fallbackHosts, candidateHosts...)
		for _, h := range allHosts {
			assert.NoError(t, h.Close())
		}
	}
}
