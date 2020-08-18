package dht

import (
	"context"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p-core/event"
	"github.com/libp2p/go-libp2p-core/peer"
	kb "github.com/libp2p/go-libp2p-kbucket"

	"github.com/stretchr/testify/require"
)

func TestSelfWalkOnAddressChange(t *testing.T) {
	ctx := context.Background()
	// create three DHT instances with auto refresh disabled
	d1 := setupDHT(ctx, t, false, DisableAutoRefresh(), forceAddressUpdateProcessing(t))
	d2 := setupDHT(ctx, t, false, DisableAutoRefresh())
	d3 := setupDHT(ctx, t, false, DisableAutoRefresh())

	var connectedTo *IpfsDHT
	// connect d1 to whoever is "further"
	if kb.CommonPrefixLen(kb.ConvertPeerID(d1.self), kb.ConvertPeerID(d2.self)) <=
		kb.CommonPrefixLen(kb.ConvertPeerID(d1.self), kb.ConvertPeerID(d3.self)) {
		connect(t, ctx, d1, d3)
		connectedTo = d3
	} else {
		connect(t, ctx, d1, d2)
		connectedTo = d2
	}

	// then connect d2 AND d3
	connect(t, ctx, d2, d3)

	// d1 should have ONLY 1 peer in it's RT
	waitForWellFormedTables(t, []*IpfsDHT{d1}, 1, 1, 2*time.Second)
	require.Equal(t, connectedTo.self, d1.routingTable.ListPeers()[0])

	// now emit the address change event
	em, err := d1.host.EventBus().Emitter(&event.EvtLocalAddressesUpdated{})
	require.NoError(t, err)
	require.NoError(t, em.Emit(event.EvtLocalAddressesUpdated{}))
	waitForWellFormedTables(t, []*IpfsDHT{d1}, 2, 2, 2*time.Second)
	// it should now have both peers in the RT
	ps := d1.routingTable.ListPeers()
	require.Contains(t, ps, d2.self)
	require.Contains(t, ps, d3.self)
}

func TestDefaultBootstrappers(t *testing.T) {
	ds := GetDefaultBootstrapPeerAddrInfos()
	require.NotEmpty(t, ds)
	require.Len(t, ds, len(DefaultBootstrapPeers))

	dfmap := make(map[peer.ID]peer.AddrInfo)
	for _, p := range DefaultBootstrapPeers {
		info, err := peer.AddrInfoFromP2pAddr(p)
		require.NoError(t, err)
		dfmap[info.ID] = *info
	}

	for _, p := range ds {
		inf, ok := dfmap[p.ID]
		require.True(t, ok)
		require.ElementsMatch(t, p.Addrs, inf.Addrs)
		delete(dfmap, p.ID)
	}
	require.Empty(t, dfmap)
}

func TestBootstrappersReplacable(t *testing.T) {
	old := rtFreezeTimeout
	rtFreezeTimeout = 100 * time.Millisecond
	defer func() {
		rtFreezeTimeout = old
	}()
	ctx := context.Background()
	d := setupDHT(ctx, t, false, disableFixLowPeersRoutine(t), BucketSize(2))
	defer d.host.Close()
	defer d.Close()

	var d1 *IpfsDHT
	var d2 *IpfsDHT

	// d1 & d2 have a cpl of 0
	for {
		d1 = setupDHT(ctx, t, false, disableFixLowPeersRoutine(t))
		if kb.CommonPrefixLen(d.selfKey, d1.selfKey) == 0 {
			break
		}
	}

	for {
		d2 = setupDHT(ctx, t, false, disableFixLowPeersRoutine(t))
		if kb.CommonPrefixLen(d.selfKey, d2.selfKey) == 0 {
			break
		}
	}
	defer d1.host.Close()
	defer d1.Close()

	defer d2.host.Close()
	defer d2.Close()

	connect(t, ctx, d, d1)
	connect(t, ctx, d, d2)
	require.Len(t, d.routingTable.ListPeers(), 2)

	// d3 & d4 with cpl=0 will go in as d1 & d2 are replacable.
	var d3 *IpfsDHT
	var d4 *IpfsDHT

	for {
		d3 = setupDHT(ctx, t, false, disableFixLowPeersRoutine(t))
		if kb.CommonPrefixLen(d.selfKey, d3.selfKey) == 0 {
			break
		}
	}

	for {
		d4 = setupDHT(ctx, t, false, disableFixLowPeersRoutine(t))
		if kb.CommonPrefixLen(d.selfKey, d4.selfKey) == 0 {
			break
		}
	}

	defer d3.host.Close()
	defer d3.Close()
	defer d4.host.Close()
	defer d4.Close()

	connect(t, ctx, d, d3)
	connect(t, ctx, d, d4)
	require.Len(t, d.routingTable.ListPeers(), 2)
	require.Contains(t, d.routingTable.ListPeers(), d3.self)
	require.Contains(t, d.routingTable.ListPeers(), d4.self)

	// do couple of refreshes and wait for the Routing Table to be "frozen".
	<-d.RefreshRoutingTable()
	<-d.RefreshRoutingTable()
	time.Sleep(1 * time.Second)

	// adding d5 fails because RT is frozen
	var d5 *IpfsDHT
	for {
		d5 = setupDHT(ctx, t, false, disableFixLowPeersRoutine(t))
		if kb.CommonPrefixLen(d.selfKey, d5.selfKey) == 0 {
			break
		}
	}
	defer d5.host.Close()
	defer d5.Close()

	connectNoSync(t, ctx, d, d5)
	time.Sleep(500 * time.Millisecond)
	require.Len(t, d.routingTable.ListPeers(), 2)
	require.Contains(t, d.routingTable.ListPeers(), d3.self)
	require.Contains(t, d.routingTable.ListPeers(), d4.self)

	// Let's empty the routing table
	for _, p := range d.routingTable.ListPeers() {
		d.routingTable.RemovePeer(p)
	}
	require.Len(t, d.routingTable.ListPeers(), 0)

	// adding d1 & d2 works now because there is space in the Routing Table
	require.NoError(t, d.host.Network().ClosePeer(d1.self))
	require.NoError(t, d.host.Network().ClosePeer(d2.self))
	connect(t, ctx, d, d1)
	connect(t, ctx, d, d2)
	require.Len(t, d.routingTable.ListPeers(), 2)
	require.Contains(t, d.routingTable.ListPeers(), d1.self)
	require.Contains(t, d.routingTable.ListPeers(), d2.self)

	// adding d3 & d4 also works because the RT is not frozen.
	require.NoError(t, d.host.Network().ClosePeer(d3.self))
	require.NoError(t, d.host.Network().ClosePeer(d4.self))
	connect(t, ctx, d, d3)
	connect(t, ctx, d, d4)
	require.Len(t, d.routingTable.ListPeers(), 2)
	require.Contains(t, d.routingTable.ListPeers(), d3.self)
	require.Contains(t, d.routingTable.ListPeers(), d4.self)

	// run refreshes and freeze the RT
	<-d.RefreshRoutingTable()
	<-d.RefreshRoutingTable()
	time.Sleep(1 * time.Second)
	// cant add d1 & d5 because RT is frozen.
	require.NoError(t, d.host.Network().ClosePeer(d1.self))
	require.NoError(t, d.host.Network().ClosePeer(d5.self))
	connectNoSync(t, ctx, d, d1)
	connectNoSync(t, ctx, d, d5)
	d.peerFound(ctx, d5.self, true)
	d.peerFound(ctx, d1.self, true)
	time.Sleep(1 * time.Second)

	require.Len(t, d.routingTable.ListPeers(), 2)
	require.Contains(t, d.routingTable.ListPeers(), d3.self)
	require.Contains(t, d.routingTable.ListPeers(), d4.self)
}
