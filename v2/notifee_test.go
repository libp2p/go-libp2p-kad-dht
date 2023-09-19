package dht

import (
	"testing"
	"time"

	"github.com/libp2p/go-libp2p-kad-dht/v2/internal/kadtest"
	"github.com/libp2p/go-libp2p/core/network"

	"github.com/libp2p/go-libp2p/core/event"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDHT_consumeNetworkEvents_onEvtLocalReachabilityChanged(t *testing.T) {
	newModeDHT := func(m ModeOpt) *DHT {
		cfg := DefaultConfig()
		cfg.Mode = m

		return newTestDHTWithConfig(t, cfg)
	}

	t.Run("set server", func(t *testing.T) {
		d := newModeDHT(ModeOptAutoClient)
		d.onEvtLocalReachabilityChanged(event.EvtLocalReachabilityChanged{
			Reachability: network.ReachabilityPublic,
		})
		assert.Equal(t, modeServer, d.mode)
	})

	t.Run("set client", func(t *testing.T) {
		d := newModeDHT(ModeOptAutoClient)

		d.onEvtLocalReachabilityChanged(event.EvtLocalReachabilityChanged{
			Reachability: network.ReachabilityPrivate,
		})

		assert.Equal(t, modeClient, d.mode)
	})

	t.Run("on unknown set client when auto client", func(t *testing.T) {
		d := newModeDHT(ModeOptAutoClient)

		d.onEvtLocalReachabilityChanged(event.EvtLocalReachabilityChanged{
			Reachability: network.ReachabilityUnknown,
		})

		assert.Equal(t, modeClient, d.mode)
	})

	t.Run("on unknown set server when auto server", func(t *testing.T) {
		d := newModeDHT(ModeOptAutoServer)

		d.onEvtLocalReachabilityChanged(event.EvtLocalReachabilityChanged{
			Reachability: network.ReachabilityUnknown,
		})

		assert.Equal(t, modeServer, d.mode)
	})

	t.Run("handles unknown event gracefully", func(t *testing.T) {
		d := newModeDHT(ModeOptAutoServer)

		d.onEvtLocalReachabilityChanged(event.EvtLocalReachabilityChanged{
			Reachability: network.Reachability(99),
		})

		assert.Equal(t, modeServer, d.mode)
	})
}

func TestDHT_consumeNetworkEvents_onEvtPeerIdentificationCompleted(t *testing.T) {
	ctx := kadtest.CtxShort(t)

	top := NewTopology(t)
	d1 := top.AddServer(nil)
	d2 := top.AddServer(nil)

	// make sure d1 has the address of d2 in its peerstore
	d1.host.Peerstore().AddAddrs(d2.host.ID(), d2.host.Addrs(), time.Minute)

	// send the event
	d1.onEvtPeerIdentificationCompleted(event.EvtPeerIdentificationCompleted{
		Peer: d2.host.ID(),
	})

	_, err := top.ExpectRoutingUpdated(ctx, d1, d2.host.ID())
	require.NoError(t, err)
}
