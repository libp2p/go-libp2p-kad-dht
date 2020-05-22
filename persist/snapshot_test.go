package persist

import (
	"context"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/test"

	blankhost "github.com/libp2p/go-libp2p-blankhost"
	kb "github.com/libp2p/go-libp2p-kbucket"
	peerstore "github.com/libp2p/go-libp2p-peerstore"
	swarmt "github.com/libp2p/go-libp2p-swarm/testing"

	ds "github.com/ipfs/go-datastore"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/require"
)

func TestNewDatastoreSnapshotter(t *testing.T) {
	_, err := NewDatastoreSnapshotter(nil, "rand")
	require.Error(t, err)
	_, err = NewDatastoreSnapshotter(ds.NewMapDatastore(), "")
	require.Error(t, err)
	_, err = NewDatastoreSnapshotter(ds.NewMapDatastore(), "rand")
	require.NoError(t, err)
}

func TestStoreAndLoad(t *testing.T) {
	nBuckets := 5
	nPeers := 2
	ctx := context.Background()

	h := blankhost.NewBlankHost(swarmt.GenSwarm(t, ctx))

	// create a routing table with nBuckets & nPeers in each bucket
	local := test.RandPeerIDFatal(t)
	rt, err := kb.NewRoutingTable(nPeers, kb.ConvertPeerID(local), time.Hour, peerstore.NewMetrics(), 1*time.Hour)
	require.NoError(t, err)
	require.NotNil(t, rt)
	addrsForPeer := make(map[peer.ID][]ma.Multiaddr)

	for i := 0; i < nBuckets; i++ {
		for j := 0; j < nPeers; j++ {
			p, err := rt.GenRandPeerID(uint(i))
			require.NoError(t, err)
			b, err := rt.TryAddPeer(p, true)
			require.NoError(t, err)
			require.True(t, b)
			addrs := test.GenerateTestAddrs(2)
			addrsForPeer[p] = addrs
			h.Peerstore().AddAddrs(p, addrs, 1*time.Hour)
		}
	}

	require.Len(t, rt.ListPeers(), nBuckets*nPeers)
	require.Len(t, addrsForPeer, nBuckets*nPeers)

	// create a snapshotter with an in-memory ds
	snapshotter, err := NewDatastoreSnapshotter(ds.NewMapDatastore(), "test")
	require.NoError(t, err)

	// store snapshot
	require.NoError(t, snapshotter.Store(h, rt))

	// load snapshot & verify it is as expected
	rtPeers, err := snapshotter.Load()
	require.NoError(t, err)
	require.Len(t, rtPeers, nBuckets*nPeers)
	for i := range rtPeers {
		pi := rtPeers[i]
		addrs, ok := addrsForPeer[pi.ID]
		require.True(t, ok)
		require.ElementsMatch(t, addrs, pi.Addrs)
		require.False(t, pi.AddedAt.IsZero())
		delete(addrsForPeer, pi.ID)
	}
	require.Empty(t, addrsForPeer)

	// Load an empty snapshot
	snapshotter, err = NewDatastoreSnapshotter(ds.NewMapDatastore(), "test")
	require.NoError(t, err)
	peers, err := snapshotter.Load()
	require.NoError(t, err)
	require.Empty(t, peers)
}
