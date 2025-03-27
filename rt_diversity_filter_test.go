package dht

import (
	"context"
	"crypto/rand"
	"testing"
	"time"

	kb "github.com/libp2p/go-libp2p-kbucket"
	"github.com/libp2p/go-libp2p-kbucket/peerdiversity"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	bhost "github.com/libp2p/go-libp2p/p2p/host/basic"
	swarmt "github.com/libp2p/go-libp2p/p2p/net/swarm/testing"
	ma "github.com/multiformats/go-multiaddr"

	"github.com/stretchr/testify/require"
)

func TestRTPeerDiversityFilter(t *testing.T) {
	h, err := bhost.NewHost(swarmt.GenSwarm(t, swarmt.OptDisableReuseport), new(bhost.HostOpts))
	require.NoError(t, err)
	h.Start()
	defer h.Close()
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

	// and then it doesn't work again
	require.False(t, r.Allow(g2))
}

func TestRoutingTableEndToEndMaxPerCpl(t *testing.T) {
	ctx := context.Background()
	h, err := bhost.NewHost(swarmt.GenSwarm(t, swarmt.OptDisableReuseport), new(bhost.HostOpts))
	require.NoError(t, err)
	h.Start()
	defer h.Close()
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
	defer d.Close()

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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h, err := bhost.NewHost(swarmt.GenSwarm(t, swarmt.OptDisableReuseport), new(bhost.HostOpts))
	require.NoError(t, err)
	h.Start()
	defer h.Close()
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
	defer d.Close()

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

func TestFilterPeersByIPDiversity(t *testing.T) {
	maxIPsPerGroup := 2

	type addr struct {
		ipv6 bool
		addr string
	}
	createPeer := func(ips ...addr) *peer.AddrInfo {
		_, pubKey, err := crypto.GenerateEd25519Key(rand.Reader)
		require.NoError(t, err)
		pid, err := peer.IDFromPublicKey(pubKey)
		require.NoError(t, err)
		p := &peer.AddrInfo{ID: pid, Addrs: make([]ma.Multiaddr, 0, len(ips))}
		for _, ip := range ips {
			var a ma.Multiaddr
			var err error
			if ip.ipv6 {
				a, err = ma.NewMultiaddr("/ip6/" + ip.addr + "/tcp/4001")
			} else {
				a, err = ma.NewMultiaddr("/ip4/" + ip.addr + "/tcp/4001")
			}
			require.NoError(t, err)
			p.Addrs = append(p.Addrs, a)
		}
		return p
	}

	t.Run("Different IPv4 blocks", func(t *testing.T) {
		peers := []*peer.AddrInfo{
			createPeer(addr{ipv6: false, addr: "1.1.1.1"}),
			createPeer(addr{ipv6: false, addr: "2.2.2.2"}),
			createPeer(addr{ipv6: false, addr: "3.3.3.3"}),
			createPeer(addr{ipv6: false, addr: "4.4.4.4"}),
		}
		filtered := filterPeersByIPDiversity(peers, maxIPsPerGroup)
		require.Equal(t, peers, filtered)
	})

	t.Run("Same IPv4 block, but acceptable diversity", func(t *testing.T) {
		peers := []*peer.AddrInfo{
			createPeer(addr{ipv6: false, addr: "1.1.1.1"}),
			createPeer(addr{ipv6: false, addr: "1.1.2.2"}),
			createPeer(addr{ipv6: false, addr: "3.3.3.3"}),
			createPeer(addr{ipv6: false, addr: "4.4.4.4"}),
		}
		filtered := filterPeersByIPDiversity(peers, maxIPsPerGroup)
		require.Equal(t, peers, filtered)
	})

	t.Run("Overrepresented IPv4 block", func(t *testing.T) {
		peers := []*peer.AddrInfo{
			createPeer(addr{ipv6: false, addr: "1.1.1.1"}),
			createPeer(addr{ipv6: false, addr: "1.1.1.2"}),
			createPeer(addr{ipv6: false, addr: "1.1.3.3"}),
			createPeer(addr{ipv6: false, addr: "4.4.4.4"}),
		}
		filtered := filterPeersByIPDiversity(peers, maxIPsPerGroup)
		require.Len(t, filtered, 1)
		require.Equal(t, peers[3], filtered[0])
	})

	t.Run("Different IPv6 ASNs", func(t *testing.T) {
		peers := []*peer.AddrInfo{
			createPeer(addr{ipv6: true, addr: "2001:4860:4860::1"}),
			createPeer(addr{ipv6: true, addr: "2606:4700:4700::2"}),
			createPeer(addr{ipv6: true, addr: "2620:fe::3"}),
			createPeer(addr{ipv6: true, addr: "2a02:6b8::4"}),
		}
		filtered := filterPeersByIPDiversity(peers, maxIPsPerGroup)
		require.Equal(t, filtered, peers)
	})

	t.Run("Same IPv6 ASNs, but acceptable diversity", func(t *testing.T) {
		peers := []*peer.AddrInfo{
			createPeer(addr{ipv6: true, addr: "2001:4860:4860::1"}),
			createPeer(addr{ipv6: true, addr: "2001:4860:4860::2"}),
			createPeer(addr{ipv6: true, addr: "2620:fe::3"}),
			createPeer(addr{ipv6: true, addr: "2a02:6b8::4"}),
		}
		filtered := filterPeersByIPDiversity(peers, maxIPsPerGroup)
		require.Equal(t, filtered, peers)
	})

	t.Run("Overrepresented IPv6 ASN", func(t *testing.T) {
		peers := []*peer.AddrInfo{
			createPeer(addr{ipv6: true, addr: "2001:4860:4860::1"}),
			createPeer(addr{ipv6: true, addr: "2001:4860:4860::2"}),
			createPeer(addr{ipv6: true, addr: "2001:4860:4860::3"}),
			createPeer(addr{ipv6: true, addr: "2a02:6b8::4"}),
		}
		filtered := filterPeersByIPDiversity(peers, maxIPsPerGroup)
		require.Len(t, filtered, 1)
		require.Equal(t, peers[3], filtered[0])
	})

	t.Run("IPv4+IPv6 acceptable representation", func(t *testing.T) {
		peers := []*peer.AddrInfo{
			createPeer(addr{ipv6: false, addr: "1.1.1.1"}, addr{ipv6: true, addr: "2001:4860:4860::1"}),
			createPeer(addr{ipv6: false, addr: "2.2.2.2"}, addr{ipv6: true, addr: "2001:4860:4860::2"}),
			createPeer(addr{ipv6: false, addr: "2.2.3.3"}, addr{ipv6: true, addr: "2620:fe::3"}),
			createPeer(addr{ipv6: false, addr: "4.4.4.4"}, addr{ipv6: true, addr: "2a02:6b8::4"}),
		}
		filtered := filterPeersByIPDiversity(peers, maxIPsPerGroup)
		require.Equal(t, filtered, peers)
	})

	t.Run("IPv4+IPv6 overrepresentation", func(t *testing.T) {
		peers := []*peer.AddrInfo{
			createPeer(addr{ipv6: false, addr: "1.1.1.1"}, addr{ipv6: true, addr: "2001:4860:4860::1"}),
			createPeer(addr{ipv6: false, addr: "1.1.1.2"}, addr{ipv6: true, addr: "2606:4700:4700::2"}),
			createPeer(addr{ipv6: false, addr: "1.1.3.3"}, addr{ipv6: true, addr: "2620:fe::3"}),
			createPeer(addr{ipv6: false, addr: "4.4.4.4"}, addr{ipv6: true, addr: "2620:fe::4"}),
		}
		filtered := filterPeersByIPDiversity(peers, maxIPsPerGroup)
		require.Len(t, filtered, 1)
		require.Equal(t, peers[3], filtered[0])
	})

	t.Run("Disabled IP diversity filter", func(t *testing.T) {
		peers := []*peer.AddrInfo{
			createPeer(addr{ipv6: false, addr: "1.1.1.1"}),
			createPeer(addr{ipv6: false, addr: "1.1.1.2"}),
			createPeer(addr{ipv6: false, addr: "1.1.3.3"}),
			createPeer(addr{ipv6: false, addr: "1.1.4.4"}),
		}
		filtered := filterPeersByIPDiversity(peers, 0)
		require.Equal(t, filtered, peers)
	})
}
