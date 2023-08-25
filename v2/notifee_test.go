package dht

import (
	"testing"

	"github.com/libp2p/go-libp2p/core/network"

	"github.com/libp2p/go-libp2p/core/event"
	"github.com/stretchr/testify/assert"
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
