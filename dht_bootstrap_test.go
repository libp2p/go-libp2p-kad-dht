package dht

import (
	"context"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p-core/event"

	kb "github.com/libp2p/go-libp2p-kbucket"

	"github.com/stretchr/testify/require"
)

func TestSelfWalkOnAddressChange(t *testing.T) {
	ctx := context.Background()
	// create three DHT instances with auto refresh disabled
	d1 := setupDHT(ctx, t, false, DisableAutoRefresh())
	d2 := setupDHT(ctx, t, false, DisableAutoRefresh())
	d3 := setupDHT(ctx, t, false, DisableAutoRefresh())

	var connectedTo *KadDHT
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
	waitForWellFormedTables(t, []*KadDHT{d1}, 1, 1, 2*time.Second)
	require.Equal(t, connectedTo.self, d1.routingTable.ListPeers()[0])

	// now emit the address change event
	em, err := d1.host.EventBus().Emitter(&event.EvtLocalAddressesUpdated{})
	require.NoError(t, err)
	require.NoError(t, em.Emit(event.EvtLocalAddressesUpdated{}))
	waitForWellFormedTables(t, []*KadDHT{d1}, 2, 2, 2*time.Second)
	// it should now have both peers in the RT
	ps := d1.routingTable.ListPeers()
	require.Contains(t, ps, d2.self)
	require.Contains(t, ps, d3.self)
}
