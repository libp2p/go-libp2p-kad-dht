package dht

import (
	"context"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p-kbucket/peerdiversity"
	swarmt "github.com/libp2p/go-libp2p-swarm/testing"
	bhost "github.com/libp2p/go-libp2p/p2p/host/basic"

	"github.com/stretchr/testify/require"
)

func TestRTPeerDiversityFilter(t *testing.T) {
	ctx := context.Background()
	h := bhost.New(swarmt.GenSwarm(t, ctx, swarmt.OptDisableReuseport))
	r := NewRTPeerDiversityFilter(h, 2, 3)

	// table should only have 2 for each prefix per cpl
	key := "key"
	g := peerdiversity.PeerGroupInfo{Cpl: 1, IPGroupKey: peerdiversity.PeerIPGroupKey(key)}
	require.True(t, r.Allow(g))
	r.Increment(g)
	require.True(t, r.Allow(g))
	r.Increment(g)
	require.False(t, r.Allow(g))

	// table should ONLY have  3 for a Prefix
	key = "random"
	g2 := peerdiversity.PeerGroupInfo{Cpl: 2, IPGroupKey: peerdiversity.PeerIPGroupKey(key)}
	require.True(t, r.Allow(g2))
	r.Increment(g2)

	g2.Cpl = 3
	require.True(t, r.Allow(g2))
	r.Increment(g2)

	g2.Cpl = 4
	require.True(t, r.Allow(g2))
	r.Increment(g2)

	require.False(t, r.Allow(g2))

	// remove a peer with a prefix and it works
	r.Decrement(g2)
	require.True(t, r.Allow(g2))
	r.Increment(g2)

	// and then it dosen't work again
	require.False(t, r.Allow(g2))
}

func TestRoutingTableEndToEnd(t *testing.T) {
	ctx := context.Background()
	h := bhost.New(swarmt.GenSwarm(t, ctx, swarmt.OptDisableReuseport))
	r := NewRTPeerDiversityFilter(h, 2, 3)

	d, err := New(
		ctx,
		h,
		testPrefix,
		NamespacedValidator("v", blankValidator{}),
		Mode(ModeServer),
		DisableAutoRefresh(),
		RoutingTablePeerDiversityFilter(r),
	)
	require.NoError(t, err)

	// only 3 peers per prefix for a cpl
	d2 := setupDHT(ctx, t, false, DisableAutoRefresh())
	connect(t, ctx, d, d2)
	waitForWellFormedTables(t, []*IpfsDHT{d}, 1, 1, 1*time.Second)

	d3 := setupDHT(ctx, t, false, DisableAutoRefresh())
	connect(t, ctx, d, d3)
	waitForWellFormedTables(t, []*IpfsDHT{d}, 2, 2, 1*time.Second)

	d4 := setupDHT(ctx, t, false, DisableAutoRefresh())
	connect(t, ctx, d, d4)
	waitForWellFormedTables(t, []*IpfsDHT{d}, 3, 3, 1*time.Second)

	d5 := setupDHT(ctx, t, false, DisableAutoRefresh())
	connectNoSync(t, ctx, d, d5)
	time.Sleep(1 * time.Second)
	require.Len(t, d.routingTable.ListPeers(), 3)
}
