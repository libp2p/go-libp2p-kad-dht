package dht

import (
	"context"
	ma "github.com/multiformats/go-multiaddr"
	"testing"
	"time"

	kb "github.com/libp2p/go-libp2p-kbucket"
	"github.com/stretchr/testify/require"
)

func TestSelfWalkOnAddressChange(t *testing.T) {
	ctx := context.Background()
	// create three DHT instances with auto refresh disabled
	d1 := setupDHT(ctx, t, false, DisableAutoRefresh())
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

	// now change the listen address so and event is emitted and we do a self walk
	require.NoError(t, d1.host.Network().Listen(ma.StringCast("/ip4/0.0.0.0/tcp/1234")))
	require.True(t, waitForWellFormedTables(t, []*IpfsDHT{d1}, 2, 2, 20*time.Second))
	// it should now have both peers in the RT
	ps := d1.routingTable.ListPeers()
	require.Contains(t, ps, d2.self)
	require.Contains(t, ps, d3.self)
}
