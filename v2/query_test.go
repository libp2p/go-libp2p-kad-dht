package dht

import (
	"testing"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/stretchr/testify/require"
)

// import (
// 	"context"
// 	"fmt"
// 	"testing"
// 	"time"

// 	"github.com/libp2p/go-libp2p"
// 	tu "github.com/libp2p/go-libp2p-testing/etc"
// 	"github.com/libp2p/go-libp2p/core/host"
// 	"github.com/libp2p/go-libp2p/core/peer"

// 	"github.com/stretchr/testify/require"
// )

// TODO Debug test failures due to timing issue on windows
// Tests  are timing dependent as can be seen in the 2 seconds timed context that we use in "tu.WaitFor".
// While the tests work fine on OSX and complete in under a second,
// they repeatedly fail to complete in the stipulated time on Windows.
// However, increasing the timeout makes them pass on Windows.

// func TestRTEvictionOnFailedQuery(t *testing.T) {
// 	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
// 	defer cancel()

// 	d1 := setupDHT(ctx, t, false)
// 	d2 := setupDHT(ctx, t, false)

// 	for i := 0; i < 10; i++ {
// 		connect(t, ctx, d1, d2)
// 		for _, conn := range d1.host.Network().ConnsToPeer(d2.self) {
// 			conn.Close()
// 		}
// 	}

// 	// peers should be in the RT because of fixLowPeers
// 	require.NoError(t, tu.WaitFor(ctx, func() error {
// 		if !checkRoutingTable(d1, d2) {
// 			return fmt.Errorf("should have routes")
// 		}
// 		return nil
// 	}))

// 	// close both hosts so query fails
// 	require.NoError(t, d1.host.Close())
// 	require.NoError(t, d2.host.Close())
// 	// peers will still be in the RT because we have decoupled membership from connectivity
// 	require.NoError(t, tu.WaitFor(ctx, func() error {
// 		if !checkRoutingTable(d1, d2) {
// 			return fmt.Errorf("should have routes")
// 		}
// 		return nil
// 	}))

// 	// failed queries should remove the peers from the RT
// 	_, err := d1.GetClosestPeers(ctx, "test")
// 	require.NoError(t, err)

// 	_, err = d2.GetClosestPeers(ctx, "test")
// 	require.NoError(t, err)

// 	require.NoError(t, tu.WaitFor(ctx, func() error {
// 		if checkRoutingTable(d1, d2) {
// 			return fmt.Errorf("should not have routes")
// 		}
// 		return nil
// 	}))
// }

// func newPeerPair(t testing.TB) (host.Host, *DHT) {
// 	listenAddr := libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0")

// 	client, err := libp2p.New(listenAddr)
// 	require.NoError(t, err)

// 	server, err := libp2p.New(listenAddr)
// 	require.NoError(t, err)

// 	cfg := DefaultConfig()
// 	cfg.Mode = ModeOptServer
// 	serverDHT, err := New(server, cfg)

// 	fillRoutingTable(t, serverDHT)

// 	t.Cleanup(func() {
// 		if err = serverDHT.Close(); err != nil {
// 			t.Logf("failed closing DHT: %s", err)
// 		}

// 		if err = client.Close(); err != nil {
// 			t.Logf("failed closing client host: %s", err)
// 		}

// 		if err = server.Close(); err != nil {
// 			t.Logf("failed closing client host: %s", err)
// 		}
// 	})

// 	ctx := context.Background()
// 	err = client.Connect(ctx, peer.AddrInfo{
// 		ID:    server.ID(),
// 		Addrs: server.Addrs(),
// 	})
// 	require.NoError(t, err)

// 	return client, serverDHT
// }

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

	d, err := New(h, cfg)
	require.NoError(t, err)

	t.Cleanup(func() {
		if err = d.Close(); err != nil {
			t.Logf("unexpected error when closing dht: %s", err)
		}
	})
	return d
}

func newClientDht(t testing.TB, cfg *Config) *DHT {
	h := newClientHost(t)

	d, err := New(h, cfg)
	require.NoError(t, err)

	t.Cleanup(func() {
		if err = d.Close(); err != nil {
			t.Logf("unexpected error when closing dht: %s", err)
		}
	})
	return d
}

// func newTestDHTForHostWithConfig(t testing.TB, h host.Host, cfg *Config) *DHT {
// 	t.Helper()

// 	h, err := libp2p.New(libp2p.NoListenAddrs)
// 	require.NoError(t, err)

// 	d, err := New(h, cfg)
// 	require.NoError(t, err)

// 	t.Cleanup(func() {
// 		if err = d.Close(); err != nil {
// 			t.Logf("closing dht: %s", err)
// 		}

// 		if err = h.Close(); err != nil {
// 			t.Logf("closing host: %s", err)
// 		}
// 	})

// 	return d
// }

// func connect(t *testing.T, ctx context.Context, a, b *DHT) {
// 	t.Helper()
// 	connectNoSync(t, ctx, a, b)
// 	wait(t, ctx, a, b)
// 	wait(t, ctx, b, a)
// }

// func wait(t *testing.T, ctx context.Context, a, b *DHT) {
// 	t.Helper()

// 	// loop until connection notification has been received.
// 	// under high load, this may not happen as immediately as we would like.
// 	for a.routingTable.Find(b.self) == "" {
// 		select {
// 		case <-ctx.Done():
// 			t.Fatal(ctx.Err())
// 		case <-time.After(time.Millisecond * 5):
// 		}
// 	}
// }

// func connectNoSync(t *testing.T, ctx context.Context, a, b *DHT) {
// 	t.Helper()

// 	idB := b.self
// 	addrB := b.peerstore.Addrs(idB)
// 	if len(addrB) == 0 {
// 		t.Fatal("peers setup incorrectly: no local address")
// 	}

// 	if err := a.host.Connect(ctx, peer.AddrInfo{ID: idB, Addrs: addrB}); err != nil {
// 		t.Fatal(err)
// 	}
// }

// func TestRTAdditionOnSuccessfulQuery(t *testing.T) {
// 	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
// 	defer cancel()

// 	// d1 := setupDHT(ctx, t, false)
// 	// d2 := setupDHT(ctx, t, false)
// 	// d3 := setupDHT(ctx, t, false)

// 	d1 := newTestDHT(t)
// 	defer d1.Close()
// 	d2 := newTestDHT(t)
// 	defer d2.Close()
// 	d3 := newTestDHT(t)
// 	defer d3.Close()

// 	connect(t, ctx, d1, d2)
// 	connect(t, ctx, d2, d3)
// 	// validate RT states

// 	// d1 has d2
// 	require.NoError(t, tu.WaitFor(ctx, func() error {
// 		if !checkRoutingTable(d1, d2) {
// 			return fmt.Errorf("should  have routes")
// 		}
// 		return nil
// 	}))
// 	// d2 has d3
// 	require.NoError(t, tu.WaitFor(ctx, func() error {
// 		if !checkRoutingTable(d2, d3) {
// 			return fmt.Errorf("should  have routes")
// 		}
// 		return nil
// 	}))

// 	// however, d1 does not know about d3
// 	require.NoError(t, tu.WaitFor(ctx, func() error {
// 		if checkRoutingTable(d1, d3) {
// 			return fmt.Errorf("should not have routes")
// 		}
// 		return nil
// 	}))

// 	// but when d3 queries d2, d1 and d3 discover each other
// 	_, err := d3.GetClosestPeers(ctx, "something")
// 	require.NoError(t, err)
// 	require.NoError(t, tu.WaitFor(ctx, func() error {
// 		if !checkRoutingTable(d1, d3) {
// 			return fmt.Errorf("should have routes")
// 		}
// 		return nil
// 	}))
// }

// func checkRoutingTable(a, b *IpfsDHT) bool {
// 	// loop until connection notification has been received.
// 	// under high load, this may not happen as immediately as we would like.
// 	return a.routingTable.Find(b.self) != "" && b.routingTable.Find(a.self) != ""
// }
