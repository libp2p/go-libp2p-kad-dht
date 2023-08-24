package dht

import (
	"testing"

	"github.com/benbjohnson/clock"
	"github.com/stretchr/testify/require"

	"github.com/libp2p/go-libp2p"
)

func TestProvidersBackend_GarbageCollectionLifecycle(t *testing.T) {
	h, err := libp2p.New(libp2p.NoListenAddrs)
	require.NoError(t, err)

	dstore, err := InMemoryDatastore()
	require.NoError(t, err)

	t.Cleanup(func() {
		if err = dstore.Close(); err != nil {
			t.Logf("closing datastore: %s", err)
		}

		if err = h.Close(); err != nil {
			t.Logf("closing host: %s", err)
		}
	})

	mockClock := clock.NewMock()

	cfg := DefaultProviderBackendConfig()
	cfg.clk = mockClock

	b, err := NewBackendProvider(h.Peerstore(), dstore, nil)
	require.NoError(t, err)

	b.StartGarbageCollection()
	b.StopGarbageCollection()
	b.StartGarbageCollection()
	b.StopGarbageCollection()
	b.StartGarbageCollection()
	b.StopGarbageCollection()
}
