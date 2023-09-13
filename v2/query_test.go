package dht

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"

	"github.com/libp2p/go-libp2p-kad-dht/v2/coord"
	"github.com/libp2p/go-libp2p-kad-dht/v2/internal/kadtest"
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

// expectRoutingUpdated selects on the event channel until an EventRoutingUpdated event is seen for the specified peer id
func expectRoutingUpdated(t *testing.T, ctx context.Context, events <-chan coord.RoutingNotification, id peer.ID) (*coord.EventRoutingUpdated, error) {
	t.Helper()
	for {
		select {
		case ev := <-events:
			if tev, ok := ev.(*coord.EventRoutingUpdated); ok {
				if tev.NodeInfo.ID == id {
					return tev, nil
				}
				t.Logf("saw routing update for %s", tev.NodeInfo.ID)
			}
		case <-ctx.Done():
			return nil, fmt.Errorf("test deadline exceeded while waiting for routing update event")
		}
	}
}

// expectRoutingUpdated selects on the event channel until an EventRoutingUpdated event is seen for the specified peer id
func expectRoutingRemoved(t *testing.T, ctx context.Context, events <-chan coord.RoutingNotification, id peer.ID) (*coord.EventRoutingRemoved, error) {
	t.Helper()
	for {
		select {
		case ev := <-events:
			if tev, ok := ev.(*coord.EventRoutingRemoved); ok {
				if tev.NodeID == id {
					return tev, nil
				}
				t.Logf("saw routing removed for %s", tev.NodeID)
			}
		case <-ctx.Done():
			return nil, fmt.Errorf("test deadline exceeded while waiting for routing removed event")
		}
	}
}

func connect(t *testing.T, ctx context.Context, a, b *DHT) {
	t.Helper()

	remoteAddrInfo := peer.AddrInfo{
		ID:    b.host.ID(),
		Addrs: b.host.Addrs(),
	}

	// Add b's addresss to a
	err := a.AddAddresses(ctx, []peer.AddrInfo{remoteAddrInfo}, time.Minute)
	require.NoError(t, err)

	// the include state machine runs in the background for a and eventually should add the node to routing table
	_, err = expectRoutingUpdated(t, ctx, a.kad.RoutingNotifications(), b.host.ID())
	require.NoError(t, err)

	// the routing table should now contain the node
	_, err = a.kad.GetNode(ctx, b.host.ID())
	require.NoError(t, err)
}

// connectLinearChain connects the dhts together in a linear chain.
// The dhts are configured with routing tables that contain immediate neighbours.
func connectLinearChain(t *testing.T, ctx context.Context, dhts ...*DHT) {
	for i := 1; i < len(dhts); i++ {
		connect(t, ctx, dhts[i-1], dhts[i])
		connect(t, ctx, dhts[i], dhts[i-1])
	}
}

func TestRTAdditionOnSuccessfulQuery(t *testing.T) {
	ctx := kadtest.CtxShort(t)

	cfg := DefaultConfig()
	d1 := newServerDht(t, cfg)
	d2 := newServerDht(t, cfg)
	d3 := newServerDht(t, cfg)

	connectLinearChain(t, ctx, d1, d2, d3)

	// d3 does not know about d1
	_, err := d3.kad.GetNode(ctx, d1.host.ID())
	require.ErrorIs(t, err, coord.ErrNodeNotFound)

	// d1 does not know about d3
	_, err = d1.kad.GetNode(ctx, d3.host.ID())
	require.ErrorIs(t, err, coord.ErrNodeNotFound)

	// // but when d3 queries d2, d1 and d3 discover each other
	_, _ = d3.FindPeer(ctx, "something")
	// ignore the error

	// d3 should update its routing table to include d1 during the query
	_, err = expectRoutingUpdated(t, ctx, d3.kad.RoutingNotifications(), d1.host.ID())
	require.NoError(t, err)

	// d3 now has d1 in its routing table
	_, err = d3.kad.GetNode(ctx, d1.host.ID())
	require.NoError(t, err)

	// d1 should update its routing table to include d3 during the query
	_, err = expectRoutingUpdated(t, ctx, d1.kad.RoutingNotifications(), d3.host.ID())
	require.NoError(t, err)

	// d1 now has d3 in its routing table
	_, err = d1.kad.GetNode(ctx, d3.host.ID())
	require.NoError(t, err)
}

func TestRTEvictionOnFailedQuery(t *testing.T) {
	ctx := kadtest.CtxShort(t)

	cfg := DefaultConfig()

	d1 := newServerDht(t, cfg)
	d2 := newServerDht(t, cfg)
	connect(t, ctx, d1, d2)
	connect(t, ctx, d2, d1)

	// close both hosts so query fails
	require.NoError(t, d1.host.Close())
	require.NoError(t, d2.host.Close())

	// peers will still be in the RT because time is paused and
	// no scheduled probes will have taken place

	// d1 still has d2 in the routing table
	_, err := d1.kad.GetNode(ctx, d2.host.ID())
	require.NoError(t, err)

	// d2 still has d1 in the routing table
	_, err = d2.kad.GetNode(ctx, d1.host.ID())
	require.NoError(t, err)

	// failed queries should remove the queried peers from the routing table
	_, _ = d1.FindPeer(ctx, "test")

	// d1 should update its routing table to remove d2 because of the failure
	_, err = expectRoutingRemoved(t, ctx, d1.kad.RoutingNotifications(), d2.host.ID())
	require.NoError(t, err)
}
