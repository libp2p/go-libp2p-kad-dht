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
			return fmt.Errorf("should  have routes")
		}
		return nil
	}))

	// close both hosts so query fails
	require.NoError(t, d1.host.Close())
	require.NoError(t, d2.host.Close())
	// peers will still be in the RT because we have decoupled membership from connectivity
	require.NoError(t, tu.WaitFor(ctx, func() error {
		if !checkRoutingTable(d1, d2) {
			return fmt.Errorf("should  have routes")
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
			return fmt.Errorf("should not have routes")
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
			return fmt.Errorf("should  have routes")
		}
		return nil
	}))
	// d2 has d3
	require.NoError(t, tu.WaitFor(ctx, func() error {
		if !checkRoutingTable(d2, d3) {
			return fmt.Errorf("should  have routes")
		}
		return nil
	}))

	// however, d1 does not know about d3
	require.NoError(t, tu.WaitFor(ctx, func() error {
		if checkRoutingTable(d1, d3) {
			return fmt.Errorf("should not have routes")
		}
		return nil
	}))

	// but when d3 queries d2, d1 and d3 discover each other
	_, err := d3.GetClosestPeers(ctx, "something")
	require.NoError(t, err)
	require.NoError(t, tu.WaitFor(ctx, func() error {
		if !checkRoutingTable(d1, d3) {
			return fmt.Errorf("should have routes")
		}
		return nil
	}))
}

func checkRoutingTable(a, b *IpfsDHT) bool {
	// loop until connection notification has been received.
	// under high load, this may not happen as immediately as we would like.
	return a.routingTable.Find(b.self) != "" && b.routingTable.Find(a.self) != ""
}
