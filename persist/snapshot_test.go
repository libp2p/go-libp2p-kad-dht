package persist

import (
	"testing"
	"time"

	kb "github.com/libp2p/go-libp2p-kbucket"
	"github.com/libp2p/go-libp2p-peerstore"

	ds "github.com/ipfs/go-datastore"
	"github.com/libp2p/go-libp2p-core/test"
	"github.com/stretchr/testify/assert"
)

func TestNewDatastoreSnapshotter(t *testing.T) {
	_, err := NewDatastoreSnapshotter(nil, "rand")
	assert.Error(t, err)
	_, err = NewDatastoreSnapshotter(ds.NewMapDatastore(), "")
	assert.Error(t, err)
	_, err = NewDatastoreSnapshotter(ds.NewMapDatastore(), "rand")
	assert.NoError(t, err)
}

func TestStoreAndLoad(t *testing.T) {
	nBuckets := 5
	nPeers := 2

	// create a routing table with nBuckets & nPeers in each bucket
	local := test.RandPeerIDFatal(t)
	rt := kb.NewRoutingTable(nPeers, kb.ConvertPeerID(local), time.Hour, peerstore.NewMetrics())
	for i := 0; i < nBuckets; i++ {
		for j := 0; j < nPeers; j++ {
			for {
				if p := test.RandPeerIDFatal(t); kb.CommonPrefixLen(kb.ConvertPeerID(local), kb.ConvertPeerID(p)) == i {
					rt.Update(p)
					break
				}
			}
		}
	}

	assert.Lenf(t, rt.Buckets, nBuckets, "test setup failed..should have %d buckets", nBuckets)
	assert.Len(t, rt.ListPeers(), nBuckets*nPeers, "test setup failed..should have %d peers", nBuckets*nPeers)

	// create a snapshotter with an in-memory ds
	snapshotter, err := NewDatastoreSnapshotter(ds.NewMapDatastore(), "test")
	assert.NoError(t, err)

	// store snapshot
	assert.NoError(t, snapshotter.Store(rt))

	// load snapshot & verify it is as expected
	peers, err := snapshotter.Load()
	assert.NoError(t, err)
	assert.Lenf(t, peers, nBuckets*nPeers, "should have got %d peers but got only %d", nBuckets*nPeers, len(peers))

	assert.Equal(t, rt.ListPeers(), peers, "peers obtained from the stored snapshot do match the peers in the routing table")

	// Load an empty snapshot
	snapshotter, err = NewDatastoreSnapshotter(ds.NewMapDatastore(), "test")
	assert.NoError(t, err)
	peers, err = snapshotter.Load()
	assert.NoError(t, err)
	assert.Empty(t, peers)
}
