package dht

import (
	"context"
	"fmt"
	"testing"
	"time"

	tu "github.com/libp2p/go-libp2p-testing/etc"

	"github.com/stretchr/testify/require"
)

// TODO Debug test failures due to timing issue on windows
// Both tests are timing dependent as can be seen in the 2 seconds timed context that we use in "tu.WaitFor".
// While both tests work fine on OSX and complete in under a second,
// they repeatedly fail to complete in the stipulated time on Windows.
// However, increasing the timeout makes them pass on Windows.

func TestNotifieeMultipleConn(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	d1 := setupDHT(ctx, t, false, RoutingTableCheckInterval(50*time.Millisecond))
	d2 := setupDHT(ctx, t, false, RoutingTableCheckInterval(50*time.Millisecond))

	nn1, err := newSubscriberNotifiee(d1)
	if err != nil {
		t.Fatal(err)
	}
	nn2, err := newSubscriberNotifiee(d2)
	if err != nil {
		t.Fatal(err)
	}

	connect(t, ctx, d1, d2)
	c12 := d1.host.Network().ConnsToPeer(d2.self)[0]
	c21 := d2.host.Network().ConnsToPeer(d1.self)[0]

	// Pretend to reestablish/re-kill connection
	nn1.Connected(d1.host.Network(), c12)
	nn2.Connected(d2.host.Network(), c21)

	if !checkRoutingTable(d1, d2) {
		t.Fatal("no routes")
	}

	// we are still connected, so the disconnect notification should be a No-op
	nn1.Disconnected(d1.host.Network(), c12)
	nn2.Disconnected(d2.host.Network(), c21)

	if !checkRoutingTable(d1, d2) {
		t.Fatal("no routes")
	}

	// the connection close should now mark the peer as missing in the RT for both peers
	// because of the disconnect notification
	for _, conn := range d1.host.Network().ConnsToPeer(d2.self) {
		conn.Close()
	}
	for _, conn := range d2.host.Network().ConnsToPeer(d1.self) {
		conn.Close()
	}

	// close both the hosts so all connection attempts to them by RT Peer validation fail
	d1.host.Close()
	d2.host.Close()

	// wait context will ensure that the RT cleanup completes
	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	require.NoError(t, tu.WaitFor(waitCtx, func() error {
		if checkRoutingTable(d1, d2) {
			return fmt.Errorf("should not have routes")
		}
		return nil
	}))
}

func TestNotifieeFuzz(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	d1 := setupDHT(ctx, t, false, RoutingTableCheckInterval(50*time.Millisecond))
	d2 := setupDHT(ctx, t, false, RoutingTableCheckInterval(50*time.Millisecond))

	for i := 0; i < 10; i++ {
		connectNoSync(t, ctx, d1, d2)
		for _, conn := range d1.host.Network().ConnsToPeer(d2.self) {
			conn.Close()
		}
	}

	// close both hosts so peer validation reconnect fails
	d1.host.Close()
	d2.host.Close()
	require.NoError(t, tu.WaitFor(ctx, func() error {
		if checkRoutingTable(d1, d2) {
			return fmt.Errorf("should not have routes")
		}
		return nil
	}))
}

func checkRoutingTable(a, b *IpfsDHT) bool {
	// loop until connection notification has been received.
	// under high load, this may not happen as immediately as we would like.
	return a.routingTable.Find(b.self) != "" && b.routingTable.Find(a.self) != ""
}
