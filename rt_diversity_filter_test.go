package dht

import (
	"context"
	"testing"
	"time"

	kb "github.com/libp2p/go-libp2p-kbucket"
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

func TestRoutingTableEndToEndMaxPerCpl(t *testing.T) {
	ctx := context.Background()
	h := bhost.New(swarmt.GenSwarm(t, ctx, swarmt.OptDisableReuseport))
	r := NewRTPeerDiversityFilter(h, 1, 2)

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

	var d2 *IpfsDHT
	var d3 *IpfsDHT

	for {
		d2 = setupDHT(ctx, t, false)
		if kb.CommonPrefixLen(d.selfKey, kb.ConvertPeerID(d2.self)) == 1 {
			break
		}
	}

	for {
		d3 = setupDHT(ctx, t, false)
		if kb.CommonPrefixLen(d.selfKey, kb.ConvertPeerID(d3.self)) == 1 {
			break
		}
	}

	// d2 will be allowed in the Routing table but
	// d3 will not be allowed.
	connectNoSync(t, ctx, d, d2)
	require.Eventually(t, func() bool {
		return d.routingTable.Find(d2.self) != ""
	}, 1*time.Second, 100*time.Millisecond)

	connectNoSync(t, ctx, d, d3)
	time.Sleep(1 * time.Second)
	require.Len(t, d.routingTable.ListPeers(), 1)
	require.True(t, d.routingTable.Find(d3.self) == "")

	// it works after removing d2
	d.routingTable.RemovePeer(d2.self)
	b, err := d.routingTable.TryAddPeer(d3.self, true, false)
	require.NoError(t, err)
	require.True(t, b)
	require.Len(t, d.routingTable.ListPeers(), 1)
	require.True(t, d.routingTable.Find(d3.self) != "")
}

func TestRoutingTableEndToEndMaxPerTable(t *testing.T) {
	ctx := context.Background()
	h := bhost.New(swarmt.GenSwarm(t, ctx, swarmt.OptDisableReuseport))
	r := NewRTPeerDiversityFilter(h, 100, 3)

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

	// only 3 peers per prefix for the table.
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
	require.True(t, d.routingTable.Find(d5.self) == "")
}
