package dht

import (
	"context"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"

	"github.com/libp2p/go-libp2p-kad-dht/v2/coord"
	"github.com/libp2p/go-libp2p-kad-dht/v2/internal/kadtest"
	"github.com/libp2p/go-libp2p-kad-dht/v2/kadt"
)

func newServerHost(t testing.TB) host.Host {
	listenAddr := libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0")

	h, err := libp2p.New(listenAddr)
	require.NoError(t, err)

	t.Cleanup(func() {
		if err = h.Close(); err != nil {
			t.Logf("unexpected error when closing host: %s", err)
		}
	})

	return h
}

func newClientHost(t testing.TB) host.Host {
	h, err := libp2p.New(libp2p.NoListenAddrs)
	require.NoError(t, err)

	t.Cleanup(func() {
		if err = h.Close(); err != nil {
			t.Logf("unexpected error when closing host: %s", err)
		}
	})

	return h
}

func newServerDht(t testing.TB, cfg *Config) *DHT {
	h := newServerHost(t)

	var err error
	if cfg == nil {
		cfg = DefaultConfig()
	}
	cfg.Mode = ModeOptServer

	d, err := New(h, cfg)
	require.NoError(t, err)

	// add at least 1 entry in the routing table so the server will pass connectivity checks
	fillRoutingTable(t, d, 1)
	require.NotEmpty(t, d.rt.NearestNodes(kadt.PeerID(d.host.ID()).Key(), 1))

	t.Cleanup(func() {
		if err = d.Close(); err != nil {
			t.Logf("unexpected error when closing dht: %s", err)
		}
	})
	return d
}

func newClientDht(t testing.TB, cfg *Config) *DHT {
	h := newClientHost(t)

	var err error
	if cfg == nil {
		cfg = DefaultConfig()
	}
	cfg.Mode = ModeOptClient
	d, err := New(h, cfg)
	require.NoError(t, err)

	t.Cleanup(func() {
		if err = d.Close(); err != nil {
			t.Logf("unexpected error when closing dht: %s", err)
		}
	})
	return d
}

func connect(t *testing.T, ctx context.Context, a, b *DHT, arn *coord.BufferedRoutingNotifier) {
	t.Helper()

	remoteAddrInfo := peer.AddrInfo{
		ID:    b.host.ID(),
		Addrs: b.host.Addrs(),
	}

	// Add b's addresss to a
	err := a.AddAddresses(ctx, []peer.AddrInfo{remoteAddrInfo}, time.Minute)
	require.NoError(t, err)

	// the include state machine runs in the background for a and eventually should add the node to routing table
	_, err = arn.ExpectRoutingUpdated(ctx, kadt.PeerID(b.host.ID()))
	require.NoError(t, err)

	// the routing table should now contain the node
	_, err = a.kad.GetNode(ctx, kadt.PeerID(b.host.ID()))
	require.NoError(t, err)
}

func TestRTAdditionOnSuccessfulQuery(t *testing.T) {
	ctx := kadtest.CtxShort(t)

	// create dhts and associated routing notifiers so we can inspect routing events
	cfg1 := DefaultConfig()
	rn1 := coord.NewBufferedRoutingNotifier()
	cfg1.Kademlia.RoutingNotifier = rn1
	d1 := newServerDht(t, cfg1)

	cfg2 := DefaultConfig()
	rn2 := coord.NewBufferedRoutingNotifier()
	cfg2.Kademlia.RoutingNotifier = rn2
	d2 := newServerDht(t, cfg2)

	cfg3 := DefaultConfig()
	rn3 := coord.NewBufferedRoutingNotifier()
	cfg3.Kademlia.RoutingNotifier = rn3
	d3 := newServerDht(t, cfg3)

	connect(t, ctx, d1, d2, rn1)
	connect(t, ctx, d2, d1, rn2)
	connect(t, ctx, d2, d3, rn2)
	connect(t, ctx, d3, d2, rn3)

	// d3 does not know about d1
	_, err := d3.kad.GetNode(ctx, kadt.PeerID(d1.host.ID()))
	require.ErrorIs(t, err, coord.ErrNodeNotFound)

	// d1 does not know about d3
	_, err = d1.kad.GetNode(ctx, kadt.PeerID(d3.host.ID()))
	require.ErrorIs(t, err, coord.ErrNodeNotFound)

	// // but when d3 queries d2, d1 and d3 discover each other
	_, _ = d3.FindPeer(ctx, "something")
	// ignore the error

	// d3 should update its routing table to include d1 during the query
	_, err = rn3.ExpectRoutingUpdated(ctx, kadt.PeerID(d1.host.ID()))
	require.NoError(t, err)

	// d3 now has d1 in its routing table
	_, err = d3.kad.GetNode(ctx, kadt.PeerID(d1.host.ID()))
	require.NoError(t, err)

	// d1 should update its routing table to include d3 during the query
	_, err = rn1.ExpectRoutingUpdated(ctx, kadt.PeerID(d3.host.ID()))
	require.NoError(t, err)

	// d1 now has d3 in its routing table
	_, err = d1.kad.GetNode(ctx, kadt.PeerID(d3.host.ID()))
	require.NoError(t, err)
}

func TestRTEvictionOnFailedQuery(t *testing.T) {
	ctx := kadtest.CtxShort(t)

	cfg1 := DefaultConfig()
	rn1 := coord.NewBufferedRoutingNotifier()
	cfg1.Kademlia.RoutingNotifier = rn1
	d1 := newServerDht(t, cfg1)

	cfg2 := DefaultConfig()
	rn2 := coord.NewBufferedRoutingNotifier()
	cfg2.Kademlia.RoutingNotifier = rn2
	d2 := newServerDht(t, cfg2)

	connect(t, ctx, d1, d2, rn1)
	connect(t, ctx, d2, d1, rn2)

	// close both hosts so query fails
	require.NoError(t, d1.host.Close())
	require.NoError(t, d2.host.Close())

	// peers will still be in the RT because time is paused and
	// no scheduled probes will have taken place

	// d1 still has d2 in the routing table
	_, err := d1.kad.GetNode(ctx, kadt.PeerID(d2.host.ID()))
	require.NoError(t, err)

	// d2 still has d1 in the routing table
	_, err = d2.kad.GetNode(ctx, kadt.PeerID(d1.host.ID()))
	require.NoError(t, err)

	// failed queries should remove the queried peers from the routing table
	_, _ = d1.FindPeer(ctx, "test")

	// d1 should update its routing table to remove d2 because of the failure
	_, err = rn1.ExpectRoutingRemoved(ctx, kadt.PeerID(d2.host.ID()))
	require.NoError(t, err)
}
