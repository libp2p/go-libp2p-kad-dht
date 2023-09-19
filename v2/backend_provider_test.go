package dht

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	ds "github.com/ipfs/go-datastore"
	"github.com/libp2p/go-libp2p"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slog"
)

var devnull = slog.New(slog.NewTextHandler(io.Discard, nil))

func newBackendProvider(t testing.TB, cfg *ProvidersBackendConfig) *ProvidersBackend {
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

	b, err := NewBackendProvider(h.Peerstore(), dstore, cfg)
	require.NoError(t, err)

	return b
}

func TestProvidersBackend_GarbageCollection(t *testing.T) {
	clk := clock.NewMock()

	cfg, err := DefaultProviderBackendConfig()
	require.NoError(t, err)

	cfg.clk = clk
	cfg.Logger = devnull

	b := newBackendProvider(t, cfg)

	// start the garbage collection process
	b.StartGarbageCollection()
	t.Cleanup(func() { b.StopGarbageCollection() })

	// write random record to datastore and peerstore
	ctx := context.Background()
	p := newAddrInfo(t)

	// write to datastore
	dsKey := newDatastoreKey(namespaceProviders, "random-key", string(p.ID))
	rec := expiryRecord{expiry: clk.Now()}
	err = b.datastore.Put(ctx, dsKey, rec.MarshalBinary())
	require.NoError(t, err)

	// write to peerstore
	b.addrBook.AddAddrs(p.ID, p.Addrs, time.Hour)

	// advance clock half the validity time and check if record is still there
	clk.Add(cfg.ProvideValidity / 2)

	// we expect the record to still be there after half the ProvideValidity
	_, err = b.datastore.Get(ctx, dsKey)
	require.NoError(t, err)

	// advance clock another time and check if the record was GC'd now
	clk.Add(cfg.ProvideValidity + cfg.GCInterval)

	// we expect the record to be GC'd now
	val, err := b.datastore.Get(ctx, dsKey)
	assert.ErrorIs(t, err, ds.ErrNotFound)
	assert.Nil(t, val)
}

func TestProvidersBackend_GarbageCollection_lifecycle_thread_safe(t *testing.T) {
	cfg, err := DefaultProviderBackendConfig()
	require.NoError(t, err)

	cfg.Logger = devnull

	b := newBackendProvider(t, cfg)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		for i := 0; i < 100; i++ {
			b.StartGarbageCollection()
		}
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		for i := 0; i < 100; i++ {
			b.StopGarbageCollection()
		}
		wg.Done()
	}()
	wg.Wait()

	b.StopGarbageCollection()

	assert.Nil(t, b.gcCancel)
	assert.Nil(t, b.gcDone)
}
