package dht

import (
	"context"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-kad-dht/v2/internal/coord"
	"github.com/libp2p/go-libp2p-kad-dht/v2/kadt"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
)

// A Topology is an arrangement of DHTs intended to simulate a network
type Topology struct {
	clk  clock.Clock
	tb   testing.TB
	dhts map[string]*DHT
	rns  map[string]*coord.BufferedRoutingNotifier
}

func NewTopology(tb testing.TB) *Topology {
	return &Topology{
		clk:  clock.New(),
		tb:   tb,
		dhts: make(map[string]*DHT),
		rns:  make(map[string]*coord.BufferedRoutingNotifier),
	}
}

func (t *Topology) SetClock(clk clock.Clock) {
	t.clk = clk
}

// AddServer adds a DHT configured as a server to the topology.
// If cfg is nil the default DHT config is used with Mode set to ModeOptServer
func (t *Topology) AddServer(cfg *Config) *DHT {
	t.tb.Helper()

	listenAddr := libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0")

	h, err := libp2p.New(listenAddr)
	require.NoError(t.tb, err)

	t.tb.Cleanup(func() {
		if err = h.Close(); err != nil {
			t.tb.Logf("unexpected error when closing host: %s", err)
		}
	})

	if cfg == nil {
		cfg = DefaultConfig()
	}
	cfg.Mode = ModeOptServer

	d, err := New(h, cfg)
	require.NoError(t.tb, err)

	rn := coord.NewBufferedRoutingNotifier()
	d.kad.SetRoutingNotifier(rn)

	// add at least 1 entry in the routing table so the server will pass connectivity checks
	fillRoutingTable(t.tb, d, 1)
	require.NotEmpty(t.tb, d.rt.NearestNodes(kadt.PeerID(d.host.ID()).Key(), 1))

	t.tb.Cleanup(func() {
		if err = d.Close(); err != nil {
			t.tb.Logf("unexpected error when closing dht: %s", err)
		}
	})

	did := t.makeid(d)
	t.dhts[did] = d
	t.rns[did] = rn

	return d
}

// AddServer adds a DHT configured as a client to the topology.
// If cfg is nil the default DHT config is used with Mode set to ModeOptClient
func (t *Topology) AddClient(cfg *Config) *DHT {
	t.tb.Helper()

	h, err := libp2p.New(libp2p.NoListenAddrs)
	require.NoError(t.tb, err)

	t.tb.Cleanup(func() {
		if err = h.Close(); err != nil {
			t.tb.Logf("unexpected error when closing host: %s", err)
		}
	})

	if cfg == nil {
		cfg = DefaultConfig()
	}
	cfg.Mode = ModeOptClient

	d, err := New(h, cfg)
	require.NoError(t.tb, err)

	rn := coord.NewBufferedRoutingNotifier()
	d.kad.SetRoutingNotifier(rn)

	t.tb.Cleanup(func() {
		if err = d.Close(); err != nil {
			t.tb.Logf("unexpected error when closing dht: %s", err)
		}
	})

	did := t.makeid(d)
	t.dhts[did] = d
	t.rns[did] = rn

	return d
}

func (t *Topology) makeid(d *DHT) string {
	return kadt.PeerID(d.host.ID()).String()
}

// Connect ensures that a has b in its routing table and vice versa.
func (t *Topology) Connect(ctx context.Context, a *DHT, b *DHT) {
	t.tb.Helper()

	aid := t.makeid(a)
	arn, ok := t.rns[aid]
	require.True(t.tb, ok, "expected routing notifier for supplied DHT")

	aAddr := peer.AddrInfo{
		ID:    a.host.ID(),
		Addrs: a.host.Addrs(),
	}

	bid := t.makeid(b)
	brn, ok := t.rns[bid]
	require.True(t.tb, ok, "expected routing notifier for supplied DHT")

	bAddr := peer.AddrInfo{
		ID:    b.host.ID(),
		Addrs: b.host.Addrs(),
	}

	// Add b's addresses to a
	err := a.AddAddresses(ctx, []peer.AddrInfo{bAddr}, time.Hour)
	require.NoError(t.tb, err)

	// Add a's addresses to b
	err = b.AddAddresses(ctx, []peer.AddrInfo{aAddr}, time.Hour)
	require.NoError(t.tb, err)

	// include state machine runs in the background for a and eventually should add the node to routing table
	_, err = arn.ExpectRoutingUpdated(ctx, kadt.PeerID(b.host.ID()))
	require.NoError(t.tb, err)

	// the routing table should now contain the node
	_, err = a.kad.GetNode(ctx, kadt.PeerID(b.host.ID()))
	require.NoError(t.tb, err)

	// include state machine runs in the background for b and eventually should add the node to routing table
	_, err = brn.ExpectRoutingUpdated(ctx, kadt.PeerID(a.host.ID()))
	require.NoError(t.tb, err)

	// the routing table should now contain the node
	_, err = b.kad.GetNode(ctx, kadt.PeerID(a.host.ID()))
	require.NoError(t.tb, err)
}

// ConnectChain connects the DHTs in a linear chain.
// The DHTs are configured with routing tables that contain immediate neighbours,
// such that DHT[x] has DHT[x-1] and DHT[x+1] in its routing table.
// The connections do not form a ring: DHT[0] only has DHT[1] in its table and DHT[n-1] only has DHT[n-2] in its table.
// If n > 2 then the first and last DHTs are guaranteed not have one another in their routing tables.
func (t *Topology) ConnectChain(ctx context.Context, ds ...*DHT) {
	for i := 1; i < len(ds); i++ {
		t.Connect(ctx, ds[i-1], ds[i])
	}
}

// ExpectRoutingUpdated blocks until an [EventRoutingUpdated] event is emitted by the supplied [DHT] the specified peer id.
func (t *Topology) ExpectRoutingUpdated(ctx context.Context, d *DHT, id peer.ID) (*coord.EventRoutingUpdated, error) {
	did := t.makeid(d)
	rn, ok := t.rns[did]
	require.True(t.tb, ok, "expected routing notifier for supplied DHT")

	return rn.ExpectRoutingUpdated(ctx, kadt.PeerID(id))
}

// ExpectRoutingRemoved blocks until an [EventRoutingRemoved] event is emitted by the supplied [DHT] the specified peer id.
func (t *Topology) ExpectRoutingRemoved(ctx context.Context, d *DHT, id peer.ID) (*coord.EventRoutingRemoved, error) {
	did := t.makeid(d)
	rn, ok := t.rns[did]
	require.True(t.tb, ok, "expected routing notifier for supplied DHT")

	return rn.ExpectRoutingRemoved(ctx, kadt.PeerID(id))
}
