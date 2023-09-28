package dht

import (
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

	t.Run("nil Query configuration", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.Query = nil
		assert.Error(t, cfg.Validate())
	})

	t.Run("invalid Query configuration", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.Query.Concurrency = -1
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

	t.Run("backends for ipfs protocol (public key missing)", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.ProtocolID = ProtocolIPFS
		cfg.Backends[namespaceProviders] = &RecordBackend{}
		cfg.Backends[namespaceIPNS] = &RecordBackend{}
		cfg.Backends["another"] = &RecordBackend{}
		assert.Error(t, cfg.Validate())
	})

	t.Run("backends for ipfs protocol (ipns missing)", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.ProtocolID = ProtocolIPFS
		cfg.Backends[namespaceProviders] = &RecordBackend{}
		cfg.Backends["another"] = &RecordBackend{}
		cfg.Backends[namespacePublicKey] = &RecordBackend{}
		assert.Error(t, cfg.Validate())
	})

	t.Run("backends for ipfs protocol (providers missing)", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.ProtocolID = ProtocolIPFS
		cfg.Backends["another"] = &RecordBackend{}
		cfg.Backends[namespaceIPNS] = &RecordBackend{}
		cfg.Backends[namespacePublicKey] = &RecordBackend{}
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

	t.Run("bootstrap peers no addresses", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.BootstrapPeers = []peer.AddrInfo{
			{ID: newPeerID(t), Addrs: []ma.Multiaddr{}},
		}
		assert.Error(t, cfg.Validate())
	})

	t.Run("bootstrap peers mixed no addresses", func(t *testing.T) {
		cfg := DefaultConfig()
		maddr, err := ma.NewMultiaddr("/ip4/127.0.0.1/tcp/1234")
		require.NoError(t, err)
		cfg.BootstrapPeers = []peer.AddrInfo{
			{ID: newPeerID(t), Addrs: []ma.Multiaddr{}},
			{ID: newPeerID(t), Addrs: []ma.Multiaddr{maddr}},
		}
		assert.Error(t, cfg.Validate()) // still an error
	})
}

func TestQueryConfig_Validate(t *testing.T) {
	t.Run("default is valid", func(t *testing.T) {
		cfg := DefaultQueryConfig()
		assert.NoError(t, cfg.Validate())
	})

	t.Run("concurrency positive", func(t *testing.T) {
		cfg := DefaultQueryConfig()

		cfg.Concurrency = 0
		assert.Error(t, cfg.Validate())
		cfg.Concurrency = -1
		assert.Error(t, cfg.Validate())
	})

	t.Run("timeout positive", func(t *testing.T) {
		cfg := DefaultQueryConfig()

		cfg.Timeout = 0
		assert.Error(t, cfg.Validate())
		cfg.Timeout = -1
		assert.Error(t, cfg.Validate())
	})

	t.Run("request concurrency positive", func(t *testing.T) {
		cfg := DefaultQueryConfig()

		cfg.RequestConcurrency = 0
		assert.Error(t, cfg.Validate())
		cfg.RequestConcurrency = -1
		assert.Error(t, cfg.Validate())
	})

	t.Run("request timeout positive", func(t *testing.T) {
		cfg := DefaultQueryConfig()

		cfg.RequestTimeout = 0
		assert.Error(t, cfg.Validate())
		cfg.RequestTimeout = -1
		assert.Error(t, cfg.Validate())
	})

	t.Run("negative default quorum", func(t *testing.T) {
		cfg := DefaultQueryConfig()

		cfg.DefaultQuorum = 0
		assert.NoError(t, cfg.Validate())
		cfg.DefaultQuorum = -1
		assert.Error(t, cfg.Validate())
	})
}
