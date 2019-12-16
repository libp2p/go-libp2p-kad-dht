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
	"github.com/stretchr/testify/require"
)

// mockSeedsProposer returns all the peers passed to it
type mockSeedsProposer struct {
}

func (m *mockSeedsProposer) Propose(ctx context.Context, rt *kb.RoutingTable, candidates []peer.ID, fallback []peer.ID) chan peer.ID {
	pChan := make(chan peer.ID)

	go func() {
		defer close(pChan)

		for _, p := range append(candidates, fallback...) {
			pChan <- p
		}
	}()

	return pChan

}

var _ persist.SeedsProposer = (*mockSeedsProposer)(nil)

func TestRTSeeder(t *testing.T) {
	testCases := map[string]struct {
		nTotalCandidates int // snapshotted candidate list

		nCandidatesNotInPeerStore int // candidates which do not exist in the peerstore

		nCandidatesAlreadyInRT int // candidates which already exist in the RT

		nFallbacks int // fallback list

		nCandidatesNotAlive int // candidates that are "not-diallable"/not alive

		seederTarget int // RT size target of the seeder

		expectedNumPeersInRoutingTable int // number of peers we expect in the routing table after seeding is complete
	}{
		"Only Candidates":        {10, 1, 1, 0, 1, 9, 8},
		"Candidates + Fallbacks": {10, 2, 2, 7, 4, 20, 11},
		"Only Fallbacks":         {10, 2, 1, 9, 7, 11, 10},
		"Empty Candidates":       {0, 0, 0, 5, 0, 5, 5},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for name, testcase := range testCases {
		// create host for self
		self := bhost.New(swarmt.GenSwarm(t, ctx, swarmt.OptDisableReuseport))

		// create candidate hosts & add them to the peer store
		candidateHosts := make([]bhost.BasicHost, testcase.nTotalCandidates)
		candidatePids := make([]peer.ID, testcase.nTotalCandidates)

		for i := 0; i < testcase.nTotalCandidates; i++ {
			h := *bhost.New(swarmt.GenSwarm(t, ctx, swarmt.OptDisableReuseport))
			candidateHosts[i] = h
			candidatePids[i] = h.ID()
			self.Peerstore().AddAddrs(candidatePids[i], h.Addrs(), 5*time.Minute)
		}

		// remove candidates from peerstore
		for i := 0; i < testcase.nCandidatesNotInPeerStore; i++ {
			self.Peerstore().ClearAddrs(candidatePids[i])
		}

		// disconnect the number of peers required
		for i := testcase.nCandidatesNotInPeerStore; i < testcase.nCandidatesNotAlive+testcase.nCandidatesNotInPeerStore; i++ {
			require.NoError(t, candidateHosts[i].Close())
		}

		// create fallback hosts & add them to the peerstore
		fallbackHosts := make([]bhost.BasicHost, testcase.nFallbacks)
		fallbackPids := make([]peer.ID, testcase.nFallbacks)
		for i := 0; i < testcase.nFallbacks; i++ {
			h := *bhost.New(swarmt.GenSwarm(t, ctx, swarmt.OptDisableReuseport))
			fallbackHosts[i] = h
			fallbackPids[i] = h.ID()
			self.Peerstore().AddAddrs(fallbackPids[i], h.Addrs(), 5*time.Minute)
		}

		// crete dht instance with the mock seedsproposer
		// make sure the default fallback peers are not in the peerstore so rt remains empty
		newDht, err := New(ctx, self, opts.SeedsProposer(&mockSeedsProposer{}), opts.FallbackPeers([]peer.ID{test.RandPeerIDFatal(t)}),
			opts.SeederRTSizeTarget(testcase.seederTarget))
		require.NoError(t, err)
		defer newDht.Close()
		require.True(t, newDht.routingTable.Size() == 0)

		// add candidates to RT
		for i := testcase.nCandidatesNotInPeerStore + testcase.nCandidatesNotAlive; i < testcase.nCandidatesNotInPeerStore+testcase.nCandidatesAlreadyInRT+testcase.nCandidatesNotAlive; i++ {
			_, err := newDht.routingTable.Update(candidatePids[i])
			require.NoError(t, err)
		}

		// assert routing table has been seeded with the expected number of peers
		require.NoError(t, newDht.seedRoutingTable(candidatePids, fallbackPids))
		require.Equalf(t, testcase.expectedNumPeersInRoutingTable, newDht.routingTable.Size(), "for test case %s, rt should have %d peers, but has %d",
			name, testcase.expectedNumPeersInRoutingTable, newDht.routingTable.Size())

		// cleanup
		require.NoError(t, self.Close())
		allHosts := append(fallbackHosts, candidateHosts...)
		for _, h := range allHosts {
			require.NoError(t, h.Close())
		}
	}
}
