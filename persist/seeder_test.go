package persist

import (
	"context"
	"testing"
	"time"

	kb "github.com/libp2p/go-libp2p-kbucket"
	"github.com/libp2p/go-libp2p-peer"
	"github.com/libp2p/go-libp2p-peerstore"
	swarmt "github.com/libp2p/go-libp2p-swarm/testing"
	bhost "github.com/libp2p/go-libp2p/p2p/host/basic"
	"github.com/stretchr/testify/assert"
)

/*
func TestSeedBasic(t *testing.T) {
	local := test.RandPeerIDFatal(t)
	rt := kb.NewRoutingTable(10, kb.ConvertPeerID(local), time.Hour, peerstore.NewMetrics())

	// partial error
	TotalSeedDialGracePeriod = 1 * time.Second
	rs := NewRandomSeeder(bhost.New(swarmt.GenSwarm(t, context.Background(), swarmt.OptDisableReuseport)), 1)
	assert.Equal(t, ErrPartialSeed, rs.Seed(rt, nil, nil))
	assert.Empty(t, rt.ListPeers())

	// no partial error
	rs = NewRandomSeeder(bhost.New(swarmt.GenSwarm(t, context.Background(), swarmt.OptDisableReuseport)), 0)
	assert.NoError(t, rs.Seed(rt, nil, nil))
	assert.Empty(t, rt.ListPeers())
}*/

func TestSeed(t *testing.T) {
	testCases := map[string]struct {
		nTotalCandidates               int   // main list of candidates
		nCandidatesNotInPeerStore      int   // candidates from the main list that are absent in the peer store
		nCandidatesNotAlive            int   // candidates from the main list that are "not-diallable"/not alive
		nFallbacks                     int   // fallback list
		expectedNumPeersInRoutingTable int   // number of peers we expect in the routing table after seeding is complete
		expectedErr                    error // error we expect from call to the random seeder
	}{
		"Success -> Only Candidates": {10, 0, 0, 0,
			10, nil},

		"Success -> (Candidates + Fallbacks)": {10, 3, 4, 7,
			10, nil},

		"Success -> Only Fallbacks": {10, 2, 8, 10,
			10, nil},

		"Success -> Empty Candidates": {0, 0, 0, 10,
			10, nil},

		"Partial -> Only Candidates": {10, 3, 2, 0, 5,
			ErrPartialSeed},

		"Partial -> (Candidates + Fallbacks)": {10, 0, 3, 1,
			8, ErrPartialSeed},

		"Partial -> Only Fallbacks": {10, 3, 7, 9, 9,
			ErrPartialSeed},
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

		// disconnect the number of peers required
		for i := testcase.nCandidatesNotInPeerStore; i < testcase.nCandidatesNotInPeerStore+testcase.nCandidatesNotAlive; i++ {
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

		// run seed & assert
		rt := kb.NewRoutingTable(10, kb.ConvertPeerID(self.ID()), time.Hour, peerstore.NewMetrics())
		rs := NewRandomSeeder(self, target)

		// assert we got the required error
		assert.Equalf(t, testcase.expectedErr, rs.Seed(rt, candidatePids, fallbackPids), "test case %s failed, did not get "+
			"expected error value", name)

		// assert routing table has been seeded with the expected number of peers
		assert.Equalf(t, testcase.expectedNumPeersInRoutingTable, len(rt.ListPeers()), "for test case %s, rt should have %d peers, but has %d",
			name, testcase.expectedNumPeersInRoutingTable, len(rt.ListPeers()))

		// cleanup
		assert.NoError(t, self.Close())
		allHosts := append(fallbackHosts, candidateHosts...)
		for _, h := range allHosts {
			assert.NoError(t, h.Close())
		}
	}
}
