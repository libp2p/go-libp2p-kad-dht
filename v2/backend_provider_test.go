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

func TestProvidersBackend_GarbageCollection(t *testing.T) {
	// construct host, datastore, mock clock and provider backend
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

	// configure mock clock
	mockClock := clock.NewMock()
	cfg := DefaultProviderBackendConfig()
	cfg.clk = mockClock
	cfg.Logger = devnull

	// init backend
	b, err := NewBackendProvider(h.Peerstore(), dstore, cfg)
	require.NoError(t, err)

	// start the garbage collection process
	b.StartGarbageCollection()

	// write random record to datastore and peerstore
	ctx := context.Background()
	p := newAddrInfo(t)

	// write to datastore
	dsKey := newDatastoreKey(namespaceProviders, "random-key", string(p.ID))
	rec := expiryRecord{expiry: mockClock.Now()}
	err = b.datastore.Put(ctx, dsKey, rec.MarshalBinary())
	require.NoError(t, err)

	// write to peerstore
	h.Peerstore().AddAddrs(p.ID, p.Addrs, time.Hour)

	// advance clock half the gc time and check if record is still there
	mockClock.Add(cfg.ProvideValidity / 2)

	// sync autobatching datastore to have all put/deletes visible
	err = b.datastore.Sync(ctx, ds.NewKey(namespaceProviders))
	require.NoError(t, err)

	// we expect the record to still be there after half the ProvideValidity
	_, err = b.datastore.Get(ctx, dsKey)
	require.NoError(t, err)

	// advance clock another gc time and check if record was GC'd now
	mockClock.Add(cfg.ProvideValidity + cfg.GCInterval)

	// sync autobatching datastore to have all put/deletes visible
	err = b.datastore.Sync(ctx, ds.NewKey(namespaceProviders))
	require.NoError(t, err)

	// we expect the record to be GC'd now
	_, err = b.datastore.Get(ctx, dsKey)
	require.ErrorIs(t, err, ds.ErrNotFound)

	b.StopGarbageCollection()
}

func TestProvidersBackend_GarbageCollection_lifecycle_thread_safe(t *testing.T) {
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

	cfg := DefaultProviderBackendConfig()
	cfg.Logger = devnull

	b, err := NewBackendProvider(h.Peerstore(), dstore, cfg)
	require.NoError(t, err)

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
