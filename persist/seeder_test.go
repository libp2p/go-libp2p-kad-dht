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
	/*
			Target, Available Candidates, Dead Candidates, Fallbacks, Result
		1)	10		10						0				0			Success -> 10 peers in RT
		2)	10		6						2				5			Success -> 10 peers in RT
		3)  10		0						0				10			Success -> 10 peers in RT
		4)  10		6						0				4           Success -> 10 peers in RT
		5)  10		7						0				2			Partial Failure -> 9 peers in RT
		6)	10		3						7				6			Partial Failure -> 9 peers in RT
	*/
	self := bhost.New(swarmt.GenSwarm(t, context.Background(), swarmt.OptDisableReuseport))
	target := 10

	testCases := []struct {
		nTotalCandidates               int
		nCandidatesNotInPeerStore      int
		nCandidatesNotAlive            int
		nFallbacks                     int
		expectedNumPeersInRoutingTable int
		expectedErr                    error
	}{
		{10, 0, 0, 0, 10, nil},
	}

	for index, testcase := range testCases {
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
		fallbackPids := make([]peer.ID, testcase.nFallbacks)
		for i := 0; i < testcase.nFallbacks; i++ {
			h := *bhost.New(swarmt.GenSwarm(t, context.Background(), swarmt.OptDisableReuseport))
			fallbackPids[i] = h.ID()
			self.Peerstore().AddAddrs(fallbackPids[i], h.Addrs(), 5*time.Minute)
		}

		// run seed & assert
		rt := kb.NewRoutingTable(10, kb.ConvertPeerID(self.ID()), time.Hour, peerstore.NewMetrics())
		rs := NewRandomSeeder(self, target)

		// assert we got the required error
		assert.Equalf(t, testcase.expectedErr, rs.Seed(rt, candidatePids, nil), "test case %d failed, did not get "+
			"expected error value", index)

		// assert routing table has been seeded with the expected number of peers
		assert.Equalf(t, testcase.expectedNumPeersInRoutingTable, len(rt.ListPeers()), "for test case %d, rt should have %d peers, but has %d",
			index, testcase.expectedNumPeersInRoutingTable, len(rt.ListPeers()))
	}
}
