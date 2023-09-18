package dht

import (
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/stretchr/testify/assert"
)

func TestConfig_Validate(t *testing.T) {
	t.Run("happy path", func(t *testing.T) {
		cfg := DefaultConfig()
		assert.NoError(t, cfg.Validate())
	})

	t.Run("invalid mode", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.Mode = "invalid"
		assert.Error(t, cfg.Validate())
	})

	t.Run("nil Kademlia configuration", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.Kademlia = nil
		assert.Error(t, cfg.Validate())
	})

	t.Run("invalid Kademlia configuration", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.Kademlia.Clock = nil
		assert.Error(t, cfg.Validate())
	})

	t.Run("empty protocol", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.ProtocolID = ""
		assert.Error(t, cfg.Validate())
	})

	t.Run("nil logger", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.Logger = nil
		assert.Error(t, cfg.Validate())
	})

	t.Run("0 stream idle timeout", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.TimeoutStreamIdle = time.Duration(0)
		assert.Error(t, cfg.Validate())
	})

	t.Run("negative stream idle timeout", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.TimeoutStreamIdle = time.Duration(-1)
		assert.Error(t, cfg.Validate())
	})

	t.Run("incompatible backends with ipfs protocol", func(t *testing.T) {
		// When we're using the IPFS protocol, we always require support
		// for ipns, pk, and provider records.
		// If the Backends map is empty and the IPFS protocol is configured,
		// we automatically populate the DHT backends for these record
		// types.
		cfg := DefaultConfig()
		cfg.ProtocolID = ProtocolIPFS
		cfg.Backends["another"] = &RecordBackend{}
		assert.Error(t, cfg.Validate())
	})

	t.Run("additional backends for ipfs protocol", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.ProtocolID = ProtocolIPFS
		cfg.Backends[namespaceProviders] = &RecordBackend{}
		cfg.Backends[namespaceIPNS] = &RecordBackend{}
		cfg.Backends[namespacePublicKey] = &RecordBackend{}
		cfg.Backends["another"] = &RecordBackend{}
		assert.Error(t, cfg.Validate())
	})

	t.Run("nil address filter", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.AddressFilter = nil
		assert.Error(t, cfg.Validate())
	})

	t.Run("nil meter provider", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.MeterProvider = nil
		assert.Error(t, cfg.Validate())
	})

	t.Run("nil tracer provider", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.TracerProvider = nil
		assert.Error(t, cfg.Validate())
	})

	t.Run("nil clock", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.Clock = nil
		assert.Error(t, cfg.Validate())
	})

	t.Run("zero bucket size", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.BucketSize = 0
		assert.Error(t, cfg.Validate())
	})

	t.Run("empty bootstrap peers", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.BootstrapPeers = []peer.AddrInfo{}
		assert.Error(t, cfg.Validate())
	})
}
