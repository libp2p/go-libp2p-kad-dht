package dht

import (
	"context"
	"io"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	ds "github.com/ipfs/go-datastore"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-kad-dht/v2/internal/kadtest"
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

type SlogHandler struct {
	clck    clock.Clock
	handler slog.Handler
}

func (s SlogHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return s.handler.Enabled(ctx, level)
}

func (s SlogHandler) Handle(ctx context.Context, record slog.Record) error {
	record.Time = s.clck.Now()
	return s.handler.Handle(ctx, record)
}

func (s SlogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return s.handler.WithAttrs(attrs)
}

func (s SlogHandler) WithGroup(name string) slog.Handler {
	return s.handler.WithGroup(name)
}

var _ slog.Handler = (*SlogHandler)(nil)

func TestProvidersBackend_GarbageCollection(t *testing.T) {
	clk := clock.NewMock()

	cfg, err := DefaultProviderBackendConfig()
	require.NoError(t, err)

	handler := &SlogHandler{
		clck:    clk,
		handler: slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}),
	}

	cfg.clk = clk
	cfg.Logger = slog.New(handler)

	b := newBackendProvider(t, cfg)

	// wrap datastore into hooked datastore. This allows us to hook into calls
	// to dstore.Delete and thus synchronize on that operation. See below.
	dstore := kadtest.NewHookedDatastore(b.datastore)
	b.datastore = dstore

	// write random record to datastore and peerstore
	ctx := context.Background()
	p := newAddrInfo(t)

	// write to datastore
	dsKey := newDatastoreKey(namespaceProviders, "random-key", string(p.ID))
	rec := expiryRecord{expiry: clk.Now()}
	err = b.datastore.Put(ctx, dsKey, rec.MarshalBinary())
	require.NoError(t, err)

	deleted := make(chan struct{})
	dstore.DeleteAfter = func(ctx context.Context, key ds.Key, err error) {
		if key == dsKey {
			close(deleted)
		}
	}

	// start the garbage collection process
	b.StartGarbageCollection()
	t.Cleanup(func() { b.StopGarbageCollection() })

	// write to peerstore
	b.addrBook.AddAddrs(p.ID, p.Addrs, time.Hour)

	// advance clock half the validity time and check if record is still there
	clk.Add(cfg.ProvideValidity / 2)

	// we expect the record to still be there after half the ProvideValidity
	cfg.Logger.Debug("get record", "key", dsKey)
	_, err = b.datastore.Get(ctx, dsKey)
	require.NoError(t, err)

	// advance clock another time and check if the record was GC'd now
	cfg.Logger.Debug("advance time")
	clk.Add(cfg.ProvideValidity + cfg.GCInterval)

	// advancing the clock's time does not guarantee that all other goroutines
	// have been run before this test's go routine continues. This means that
	// the dstore.Delete call from the garbage collection may not have been
	// performed by the time we assert the non-existence of the record below.
	// Therefore, we block here until the `deleted` channel was closed. This
	// happens after the record was deleted. That way, the below dstore.Get call
	// will return a ds.ErrNotFound. This synchronization issue is especially
	// problematic on Windows and didn't surface on MacOS.
	cfg.Logger.Debug("block until deleted")
	select {
	case <-kadtest.CtxShort(t).Done():
		t.Fatal("garbage collection timeout")
	case <-deleted:
		// deleted!
	}

	// we expect the record to be GC'd now
	cfg.Logger.Debug("get record", "key", dsKey)
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
