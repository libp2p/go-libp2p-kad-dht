package dht

import (
	"context"
	"errors"
	"testing"
	"time"

	tu "github.com/libp2p/go-libp2p-testing/etc"
	"github.com/libp2p/go-libp2p/core/peer"
	bhost "github.com/libp2p/go-libp2p/p2p/host/basic"
	swarmt "github.com/libp2p/go-libp2p/p2p/net/swarm/testing"
	ma "github.com/multiformats/go-multiaddr"

	"github.com/stretchr/testify/require"
)

// TODO Debug test failures due to timing issue on windows
// Tests  are timing dependent as can be seen in the 2 seconds timed context that we use in "tu.WaitFor".
// While the tests work fine on OSX and complete in under a second,
// they repeatedly fail to complete in the stipulated time on Windows.
// However, increasing the timeout makes them pass on Windows.

func TestRTEvictionOnFailedQuery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	d1 := setupDHT(ctx, t, false)
	d2 := setupDHT(ctx, t, false)

	for i := 0; i < 10; i++ {
		connect(t, ctx, d1, d2)
		for _, conn := range d1.host.Network().ConnsToPeer(d2.self) {
			conn.Close()
		}
	}

	// peers should be in the RT because of fixLowPeers
	require.NoError(t, tu.WaitFor(ctx, func() error {
		if !checkRoutingTable(d1, d2) {
			return errors.New("should have routes")
		}
		return nil
	}))

	// close both hosts so query fails
	require.NoError(t, d1.host.Close())
	require.NoError(t, d2.host.Close())
	// peers will still be in the RT because we have decoupled membership from connectivity
	require.NoError(t, tu.WaitFor(ctx, func() error {
		if !checkRoutingTable(d1, d2) {
			return errors.New("should have routes")
		}
		return nil
	}))

	// failed queries should remove the peers from the RT
	_, err := d1.GetClosestPeers(ctx, "test")
	require.NoError(t, err)

	_, err = d2.GetClosestPeers(ctx, "test")
	require.NoError(t, err)

	require.NoError(t, tu.WaitFor(ctx, func() error {
		if checkRoutingTable(d1, d2) {
			return errors.New("should not have routes")
		}
		return nil
	}))
}

func TestRTAdditionOnSuccessfulQuery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	d1 := setupDHT(ctx, t, false)
	d2 := setupDHT(ctx, t, false)
	d3 := setupDHT(ctx, t, false)

	connect(t, ctx, d1, d2)
	connect(t, ctx, d2, d3)
	// validate RT states

	// d1 has d2
	require.NoError(t, tu.WaitFor(ctx, func() error {
		if !checkRoutingTable(d1, d2) {
			return errors.New("should  have routes")
		}
		return nil
	}))
	// d2 has d3
	require.NoError(t, tu.WaitFor(ctx, func() error {
		if !checkRoutingTable(d2, d3) {
			return errors.New("should  have routes")
		}
		return nil
	}))

	// however, d1 does not know about d3
	require.NoError(t, tu.WaitFor(ctx, func() error {
		if checkRoutingTable(d1, d3) {
			return errors.New("should not have routes")
		}
		return nil
	}))

	// but when d3 queries d2, d1 and d3 discover each other
	_, err := d3.GetClosestPeers(ctx, "something")
	require.NoError(t, err)
	require.NoError(t, tu.WaitFor(ctx, func() error {
		if !checkRoutingTable(d1, d3) {
			return errors.New("should have routes")
		}
		return nil
	}))
}

func checkRoutingTable(a, b *IpfsDHT) bool {
	// loop until connection notification has been received.
	// under high load, this may not happen as immediately as we would like.
	return a.routingTable.Find(b.self) != "" && b.routingTable.Find(a.self) != ""
}

func TestFilterPeersByIPDiversity(t *testing.T) {
	maxIPsPerGroup := 2

	ctx := context.Background()
	host, err := bhost.NewHost(swarmt.GenSwarm(t, swarmt.OptDisableReuseport), new(bhost.HostOpts))
	require.NoError(t, err)
	opts := []Option{
		RoutingTablePeerDiversityFilter(NewRTPeerDiversityFilter(host, maxIPsPerGroup, maxIPsPerGroup)),
	}
	dht, err := New(ctx, host, opts...)
	require.NoError(t, err)
	q := query{
		dht:            dht,
		maxIPsPerGroup: maxIPsPerGroup,
	}

	type addr struct {
		ipv6 bool
		addr string
	}
	createPeer := func(ips ...addr) *peer.AddrInfo {
		p := &peer.AddrInfo{ID: "", Addrs: make([]ma.Multiaddr, 0, len(ips))}
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
		filtered := q.filterPeersByIPDiversity(peers)
		require.Equal(t, peers, filtered)
	})

	t.Run("Same IPv4 block, but acceptable diversity", func(t *testing.T) {
		peers := []*peer.AddrInfo{
			createPeer(addr{ipv6: false, addr: "1.1.1.1"}),
			createPeer(addr{ipv6: false, addr: "1.1.2.2"}),
			createPeer(addr{ipv6: false, addr: "3.3.3.3"}),
			createPeer(addr{ipv6: false, addr: "4.4.4.4"}),
		}
		filtered := q.filterPeersByIPDiversity(peers)
		require.Equal(t, peers, filtered)
	})

	t.Run("Overrepresented IPv4 block", func(t *testing.T) {
		peers := []*peer.AddrInfo{
			createPeer(addr{ipv6: false, addr: "1.1.1.1"}),
			createPeer(addr{ipv6: false, addr: "1.1.1.2"}),
			createPeer(addr{ipv6: false, addr: "1.1.3.3"}),
			createPeer(addr{ipv6: false, addr: "4.4.4.4"}),
		}
		filtered := q.filterPeersByIPDiversity(peers)
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
		filtered := q.filterPeersByIPDiversity(peers)
		require.Equal(t, filtered, peers)
	})

	t.Run("Same IPv6 ASNs, but acceptable diversity", func(t *testing.T) {
		peers := []*peer.AddrInfo{
			createPeer(addr{ipv6: true, addr: "2001:4860:4860::1"}),
			createPeer(addr{ipv6: true, addr: "2001:4860:4860::2"}),
			createPeer(addr{ipv6: true, addr: "2620:fe::3"}),
			createPeer(addr{ipv6: true, addr: "2a02:6b8::4"}),
		}
		filtered := q.filterPeersByIPDiversity(peers)
		require.Equal(t, filtered, peers)
	})

	t.Run("Overrepresented IPv6 ASN", func(t *testing.T) {
		peers := []*peer.AddrInfo{
			createPeer(addr{ipv6: true, addr: "2001:4860:4860::1"}),
			createPeer(addr{ipv6: true, addr: "2001:4860:4860::2"}),
			createPeer(addr{ipv6: true, addr: "2001:4860:4860::3"}),
			createPeer(addr{ipv6: true, addr: "2a02:6b8::4"}),
		}
		filtered := q.filterPeersByIPDiversity(peers)
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
		filtered := q.filterPeersByIPDiversity(peers)
		require.Equal(t, filtered, peers)
	})

	t.Run("IPv4+IPv6 overrepresentation", func(t *testing.T) {
		peers := []*peer.AddrInfo{
			createPeer(addr{ipv6: false, addr: "1.1.1.1"}, addr{ipv6: true, addr: "2001:4860:4860::1"}),
			createPeer(addr{ipv6: false, addr: "1.1.1.2"}, addr{ipv6: true, addr: "2606:4700:4700::2"}),
			createPeer(addr{ipv6: false, addr: "1.1.3.3"}, addr{ipv6: true, addr: "2620:fe::3"}),
			createPeer(addr{ipv6: false, addr: "4.4.4.4"}, addr{ipv6: true, addr: "2620:fe::4"}),
		}
		filtered := q.filterPeersByIPDiversity(peers)
		require.Len(t, filtered, 1)
		require.Equal(t, peers[3], filtered[0])
	})

	q.maxIPsPerGroup = 0
	t.Run("Disabled IP diversity filter", func(t *testing.T) {
		peers := []*peer.AddrInfo{
			createPeer(addr{ipv6: false, addr: "1.1.1.1"}),
			createPeer(addr{ipv6: false, addr: "1.1.1.2"}),
			createPeer(addr{ipv6: false, addr: "1.1.3.3"}),
			createPeer(addr{ipv6: false, addr: "1.1.4.4"}),
		}
		filtered := q.filterPeersByIPDiversity(peers)
		require.Equal(t, filtered, peers)
	})
}
