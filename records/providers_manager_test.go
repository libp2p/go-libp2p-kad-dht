package records

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/libp2p/go-libp2p-kad-dht/amino"
	"github.com/libp2p/go-libp2p-kad-dht/internal"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/host/peerstore/pstoremem"
	"github.com/stretchr/testify/require"

	"github.com/multiformats/go-base32"
	mh "github.com/multiformats/go-multihash"

	ds "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
	dssync "github.com/ipfs/go-datastore/sync"
	//
	// used by TestLargeProvidersSet: do not remove
	// lds "github.com/ipfs/go-ds-leveldb"
)

// testPeerID returns a valid libp2p peer ID wrapping name as an identity
// multihash: no real keypair, but a real, validly-structured peer ID, so
// provider records built from it survive decodeProvKeyPeer's multihash
// validation the way a real peer ID would. Same name always yields the same
// ID; distinct names always yield distinct IDs.
func testPeerID(t testing.TB, name string) peer.ID {
	t.Helper()
	digest, err := mh.Sum([]byte(name), mh.IDENTITY, -1)
	require.NoError(t, err)
	id, err := peer.IDFromBytes(digest)
	require.NoError(t, err)
	return id
}

func TestProviderManager(t *testing.T) {
	ctx := t.Context()

	mid := testPeerID(t, "testing")
	ps, err := pstoremem.NewPeerstore()
	if err != nil {
		t.Fatal(err)
	}
	p, err := NewProviderManager(mid, ps, dssync.MutexWrap(ds.NewMapDatastore()))
	if err != nil {
		t.Fatal(err)
	}
	a := internal.Hash([]byte("test"))
	require.NoError(t, p.AddProvider(ctx, a, peer.AddrInfo{ID: testPeerID(t, "testingprovider")}))

	// Not cached
	// TODO verify that cache is empty
	resp, err := p.GetProviders(ctx, a)
	require.NoError(t, err)
	if len(resp) != 1 {
		t.Fatal("Could not retrieve provider.")
	}

	// Cached
	// TODO verify that cache is populated
	resp, err = p.GetProviders(ctx, a)
	require.NoError(t, err)
	if len(resp) != 1 {
		t.Fatal("Could not retrieve provider.")
	}

	require.NoError(t, p.AddProvider(ctx, a, peer.AddrInfo{ID: testPeerID(t, "testingprovider2")}))
	require.NoError(t, p.AddProvider(ctx, a, peer.AddrInfo{ID: testPeerID(t, "testingprovider3")}))
	// TODO verify that cache is already up to date
	resp, err = p.GetProviders(ctx, a)
	require.NoError(t, err)
	if len(resp) != 3 {
		t.Fatalf("Should have got 3 providers, got %d", len(resp))
	}

	require.NoError(t, p.Close())
}

func TestProviderManagerClosed(t *testing.T) {
	ctx := t.Context()

	mid := testPeerID(t, "testing")
	ps, err := pstoremem.NewPeerstore()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, ps.Close()) })

	p, err := NewProviderManager(mid, ps, dssync.MutexWrap(ds.NewMapDatastore()))
	require.NoError(t, err)
	require.NoError(t, p.Close())

	// calls after Close must fail fast instead of blocking forever
	err = p.AddProvider(ctx, internal.Hash([]byte("test")), peer.AddrInfo{ID: mid})
	require.ErrorIs(t, err, ErrClosed)

	_, err = p.GetProviders(ctx, internal.Hash([]byte("test")))
	require.ErrorIs(t, err, ErrClosed)
}

func TestProvidersDatastore(t *testing.T) {
	old := lruCacheSize
	lruCacheSize = 10
	defer func() { lruCacheSize = old }()

	ctx := t.Context()

	mid := testPeerID(t, "testing")
	ps, err := pstoremem.NewPeerstore()
	if err != nil {
		t.Fatal(err)
	}

	p, err := NewProviderManager(mid, ps, dssync.MutexWrap(ds.NewMapDatastore()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { require.NoError(t, p.Close()) }()

	friend := testPeerID(t, "friend")
	var mhs []mh.Multihash
	for i := range 100 {
		h := internal.Hash(fmt.Append(nil, i))
		mhs = append(mhs, h)
		require.NoError(t, p.AddProvider(ctx, h, peer.AddrInfo{ID: friend}))
	}

	for _, c := range mhs {
		resp, err := p.GetProviders(ctx, c)
		require.NoError(t, err)
		if len(resp) != 1 {
			t.Fatal("Could not retrieve provider.")
		}
		if resp[0].ID != friend {
			t.Fatal("expected provider to be 'friend'")
		}
	}
}

func TestProvidersSerialization(t *testing.T) {
	dstore := dssync.MutexWrap(ds.NewMapDatastore())

	k := internal.Hash(([]byte("my key!")))
	p1 := testPeerID(t, "peer one")
	p2 := testPeerID(t, "peer two")
	pt1 := time.Now()
	pt2 := pt1.Add(time.Hour)

	err := writeProviderEntry(context.Background(), dstore, k, p1, pt1)
	if err != nil {
		t.Fatal(err)
	}

	err = writeProviderEntry(context.Background(), dstore, k, p2, pt2)
	if err != nil {
		t.Fatal(err)
	}

	pset, err := loadProviderSet(context.Background(), dstore, amino.DefaultProvideValidity, k)
	if err != nil {
		t.Fatal(err)
	}

	lt1, ok := pset.set[p1]
	if !ok {
		t.Fatal("failed to load set correctly")
	}

	if !pt1.Equal(lt1) {
		t.Fatalf("time wasnt serialized correctly, %v != %v", pt1, lt1)
	}

	lt2, ok := pset.set[p2]
	if !ok {
		t.Fatal("failed to load set correctly")
	}

	if !pt2.Equal(lt2) {
		t.Fatalf("time wasnt serialized correctly, %v != %v", pt1, lt1)
	}
}

func TestProvidesExpire(t *testing.T) {
	provideValidity := time.Second / 2
	cleanupInterval := time.Second / 10

	// Flush every write straight to disk, as the sustained ADD_PROVIDER rate
	// this buffer is sized for would in practice: Close no longer prunes
	// pending before flushing, so a write left buffered past provideValidity
	// (as this test's handful of writes would be, against the default
	// batchBufferSize) lands on disk stale and waits for the next GC pass
	// instead of being caught at Close.
	oldBatch := batchBufferSize
	batchBufferSize = 1
	t.Cleanup(func() { batchBufferSize = oldBatch })

	ctx := t.Context()

	ds := dssync.MutexWrap(ds.NewMapDatastore())
	mid := testPeerID(t, "testing")
	ps, err := pstoremem.NewPeerstore()
	if err != nil {
		t.Fatal(err)
	}
	p, err := NewProviderManager(mid, ps, ds, ProvideValidity(provideValidity), CleanupInterval(cleanupInterval))
	if err != nil {
		t.Fatal(err)
	}

	peers := []peer.ID{testPeerID(t, "a"), testPeerID(t, "b")}
	var mhs []mh.Multihash
	for i := range 10 {
		h := internal.Hash(fmt.Append(nil, i))
		mhs = append(mhs, h)
	}

	for _, h := range mhs[:5] {
		require.NoError(t, p.AddProvider(ctx, h, peer.AddrInfo{ID: peers[0]}))
		require.NoError(t, p.AddProvider(ctx, h, peer.AddrInfo{ID: peers[1]}))
	}

	time.Sleep(provideValidity / 2)

	for _, h := range mhs[5:] {
		require.NoError(t, p.AddProvider(ctx, h, peer.AddrInfo{ID: peers[0]}))
		require.NoError(t, p.AddProvider(ctx, h, peer.AddrInfo{ID: peers[1]}))
	}

	for _, h := range mhs {
		out, err := p.GetProviders(ctx, h)
		require.NoError(t, err)
		if len(out) != 2 {
			t.Fatal("expected providers to still be there")
		}
	}

	// Sleep long enough for first batch to expire, but not the second batch
	// First batch: added at t=0, expires at t=500ms (ProvideValidity)
	// Second batch: added at t=250ms, expires at t=750ms
	// We sleep 350ms (total 600ms) to provide 150ms margin before second batch expires
	time.Sleep(provideValidity/2 + cleanupInterval)

	for _, h := range mhs[:5] {
		out, err := p.GetProviders(ctx, h)
		require.NoError(t, err)
		if len(out) > 0 {
			t.Fatal("expected providers to be cleaned up, got: ", out)
		}
	}

	for _, h := range mhs[5:] {
		out, err := p.GetProviders(ctx, h)
		require.NoError(t, err)
		if len(out) != 2 {
			t.Fatal("expected providers to still be there")
		}
	}

	time.Sleep(provideValidity)

	// Stop to prevent data races
	require.NoError(t, p.Close())

	// GC no longer purges the cache; expired entries are filtered out on read
	// and age out of the LRU, so cache.Len() may still be non-zero here. The
	// meaningful post-condition is that GC reclaimed the expired records from the
	// datastore, and that serving them already returned empty above.
	res, err := ds.Query(context.Background(), dsq.Query{Prefix: ProvidersKeyPrefix})
	if err != nil {
		t.Fatal(err)
	}
	rest, err := res.Rest()
	if err != nil {
		t.Fatal(err)
	}
	if len(rest) > 0 {
		t.Fatal("expected everything to be cleaned out of the datastore")
	}
}

func TestProvidesCacheExpire(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := t.Context()
		provideValidity := 24 * time.Hour     // 1 day
		cleanupInterval := 5 * 24 * time.Hour // 5 days

		dstore := dssync.MutexWrap(ds.NewMapDatastore())
		mid := testPeerID(t, "testing")
		ps, err := pstoremem.NewPeerstore()
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, ps.Close()) })
		p, err := NewProviderManager(mid, ps, dstore, ProvideValidity(provideValidity), CleanupInterval(cleanupInterval))
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, p.Close()) })

		mhs := make([]mh.Multihash, 2)
		for i := range mhs {
			mhs[i] = internal.Hash(fmt.Append(nil, i))
		}

		peers := []peer.ID{testPeerID(t, "a"), testPeerID(t, "b")}
		for i, h := range mhs {
			require.NoError(t, p.AddProvider(ctx, h, peer.AddrInfo{ID: peers[0]}))
			_, err := p.GetProviders(ctx, h)
			require.NoError(t, err)
			require.Len(t, p.cache.Keys(), i+1)
		}

		time.Sleep(provideValidity / 2)

		require.NoError(t, p.AddProvider(ctx, mhs[0], peer.AddrInfo{ID: peers[1]}))
		// AddProvider updates the cached providerSet synchronously; wait for the
		// background GC goroutine to settle before reading the cache directly.
		synctest.Wait()
		cached, _ := p.cache.Get(string(mhs[0]))
		require.Len(t, cached.(*providerSet).providers, 2)

		// Sleep slightly past provideValidity so the expiry check
		// time.Since(v)>provideValidity triggers for the first batch.
		time.Sleep(provideValidity/2 + time.Millisecond)

		out, err := p.GetProviders(ctx, mhs[0])
		require.NoError(t, err)
		require.Len(t, out, 1, "expected one provider to have expired")
		cached, _ = p.cache.Get(string(mhs[0]))
		require.Len(t, cached.(*providerSet).providers, 1)

		out, err = p.GetProviders(ctx, mhs[1])
		require.NoError(t, err)
		require.Empty(t, out, "expected all providers to have expired")
		cached, _ = p.cache.Get(string(mhs[1]))
		require.Empty(t, cached.(*providerSet).providers)
	})
}

var (
	_ = io.NopCloser
	_ = os.DevNull
)

// TestLargeProvidersSet can be used for profiling.
// The datastore can be switched to levelDB by uncommenting the section below and the import above
func TestLargeProvidersSet(t *testing.T) {
	t.Skip("This can be used for profiling. Skipping it for now to avoid incurring extra CI time")
	old := lruCacheSize
	lruCacheSize = 10
	defer func() { lruCacheSize = old }()

	dstore := ds.NewMapDatastore()

	//dirn, err := os.MkdirTemp("", "provtest")
	//	t.Fatal(err)
	// }
	//
	// opts := &lds.Options{
	//	NoSync:      true,
	//	Compression: 1,
	// }
	// lds, err := lds.NewDatastore(dirn, opts)
	// if err != nil {
	//	t.Fatal(err)
	// }
	// dstore = lds
	//
	// defer func() {
	//	os.RemoveAll(dirn)
	// }()

	ctx := context.Background()
	var peers []peer.ID
	for i := range 3000 {
		peers = append(peers, testPeerID(t, fmt.Sprint(i)))
	}

	mid := testPeerID(t, "myself")
	ps, err := pstoremem.NewPeerstore()
	if err != nil {
		t.Fatal(err)
	}

	p, err := NewProviderManager(mid, ps, dstore)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { require.NoError(t, p.Close()) }()

	var mhs []mh.Multihash
	for i := range 1000 {
		h := internal.Hash(fmt.Append(nil, i))
		mhs = append(mhs, h)
		for _, pid := range peers {
			require.NoError(t, p.AddProvider(ctx, h, peer.AddrInfo{ID: pid}))
		}
	}

	for range 5 {
		start := time.Now()
		for _, h := range mhs {
			_, err := p.GetProviders(ctx, h)
			require.NoError(t, err)
		}
		elapsed := time.Since(start)
		fmt.Printf("query %f ms\n", elapsed.Seconds()*1000)
	}
}

// TestUponCacheMissProvidersAreReadFromDatastore checks that a read missing the
// cache merges the providers on disk with those still buffered in pending.
//
// Both halves are seeded so that neither source can cover for the other: p1 is
// written straight to the datastore and never offered to AddProvider, so it can
// only arrive via loadProviderSet, while p2 stays below the flush threshold, so
// it can only arrive via applyPending. Adding both through AddProvider instead
// would leave the datastore empty and let applyPending satisfy the assertion on
// its own, which passes even if loadProviderSet returns nothing.
func TestUponCacheMissProvidersAreReadFromDatastore(t *testing.T) {
	old := lruCacheSize
	lruCacheSize = 1
	defer func() { lruCacheSize = old }()
	ctx := t.Context()

	p1, p2 := testPeerID(t, "a"), testPeerID(t, "b")
	h1 := internal.Hash([]byte("1"))
	h2 := internal.Hash([]byte("2"))
	ps, err := pstoremem.NewPeerstore()
	require.NoError(t, err)

	store := dssync.MutexWrap(ds.NewMapDatastore())
	pm, err := NewProviderManager(p1, ps, store)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, pm.Close()) })

	now := time.Now()
	require.NoError(t, writeProviderEntry(ctx, store, h1, p1, now))
	require.NoError(t, writeProviderEntry(ctx, store, h2, p1, now))

	// Only a read populates the cache, so read h1 to cache it and h2 to evict it
	// again from the size-1 cache.
	_, err = pm.GetProviders(ctx, h1)
	require.NoError(t, err)
	_, err = pm.GetProviders(ctx, h2)
	require.NoError(t, err)

	require.NoError(t, pm.AddProvider(ctx, h1, peer.AddrInfo{ID: p2}))
	require.NotContains(t, rawKeys(t, ctx, store), mkProvKeyFor(h1, p2),
		"the second provider must still be unflushed for this test to cover the merge")

	h1Provs, err := pm.GetProviders(ctx, h1)
	require.NoError(t, err)
	got := make([]peer.ID, len(h1Provs))
	for i, ai := range h1Provs {
		got[i] = ai.ID
	}
	require.ElementsMatch(t, []peer.ID{p1, p2}, got,
		"h1 must be provided by the datastore copy and the pending write")
}

func TestWriteUpdatesCache(t *testing.T) {
	ctx := t.Context()

	p1, p2 := testPeerID(t, "a"), testPeerID(t, "b")
	h1 := internal.Hash([]byte("1"))
	ps, err := pstoremem.NewPeerstore()
	if err != nil {
		t.Fatal(err)
	}

	pm, err := NewProviderManager(p1, ps, dssync.MutexWrap(ds.NewMapDatastore()))
	if err != nil {
		t.Fatal(err)
	}

	// add provider
	require.NoError(t, pm.AddProvider(ctx, h1, peer.AddrInfo{ID: p1}))
	// force into the cache
	_, err = pm.GetProviders(ctx, h1)
	require.NoError(t, err)
	// add a second provider
	require.NoError(t, pm.AddProvider(ctx, h1, peer.AddrInfo{ID: p2}))

	c1Provs, err := pm.GetProviders(ctx, h1)
	require.NoError(t, err)
	if len(c1Provs) != 2 {
		t.Fatalf("expected h1 to be provided by 2 peers, is by %d", len(c1Provs))
	}
}

// rawKeys returns every physical key stored in dstore.
func rawKeys(t *testing.T, ctx context.Context, dstore ds.Datastore) []string {
	t.Helper()
	res, err := dstore.Query(ctx, dsq.Query{KeysOnly: true})
	require.NoError(t, err)
	defer func() { require.NoError(t, res.Close()) }()

	var keys []string
	for e := range res.Next() {
		require.NoError(t, e.Error)
		keys = append(keys, e.Key)
	}
	return keys
}

// writeProviderEntry writes a provider record straight into dstore, bypassing
// ProviderManager's pending buffer entirely. Tests use this to seed on-disk
// state (for example an already-stale record) that a manager under test
// should never have produced itself.
func writeProviderEntry(ctx context.Context, dstore ds.Datastore, k []byte, p peer.ID, t time.Time) error {
	return dstore.Put(ctx, ds.NewKey(mkProvKeyFor(k, p)), encodeProviderTime(t))
}

// TestProviderKeyScheme verifies provider records are stored under the
// "/providers/" namespace prefix, one datastore key per (key, provider) pair.
func TestProviderKeyScheme(t *testing.T) {
	ctx := t.Context()
	store := dssync.MutexWrap(ds.NewMapDatastore())
	ps, err := pstoremem.NewPeerstore()
	require.NoError(t, err)
	pm, err := NewProviderManager(testPeerID(t, "self"), ps, store)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, pm.Close()) })

	key := internal.Hash([]byte("cid"))
	prov := testPeerID(t, "prov")
	require.NoError(t, pm.AddProvider(ctx, key, peer.AddrInfo{ID: prov}))
	got, err := pm.GetProviders(ctx, key)
	require.NoError(t, err)
	require.Len(t, got, 1)

	require.NoError(t, pm.Close())
	require.Equal(t, "/providers/", ProvidersKeyPrefix)
	require.Equal(t, []string{mkProvKeyFor(key, prov)}, rawKeys(t, ctx, store))
}

// TestProviderManagerConcurrentAccess hammers AddProvider and GetProviders from
// many goroutines against overlapping keys, with GC firing continuously, to
// shake out data races now that the serialising event loop is gone. It is only
// meaningful under -race. Every worker adds itself as a provider for every key,
// so each key must end with the full worker set (fresh records are never GC'd).
func TestProviderManagerConcurrentAccess(t *testing.T) {
	ctx := t.Context()
	ps, err := pstoremem.NewPeerstore()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, ps.Close()) })

	pm, err := NewProviderManager(testPeerID(t, "self"), ps,
		dssync.MutexWrap(ds.NewMapDatastore()),
		// GC aggressively so its cache purge and datastore sweep overlap the
		// readers and writers.
		CleanupInterval(time.Millisecond))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, pm.Close()) })

	const (
		workers = 16
		nkeys   = 8
		rounds  = 30
	)
	mhs := make([]mh.Multihash, nkeys)
	for i := range mhs {
		mhs[i] = internal.Hash(fmt.Append(nil, i))
	}

	var wg sync.WaitGroup
	for w := range workers {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			prov := testPeerID(t, fmt.Sprintf("prov-%d", w))
			for range rounds {
				for _, key := range mhs {
					if err := pm.AddProvider(ctx, key, peer.AddrInfo{ID: prov}); err != nil {
						t.Errorf("worker %d AddProvider: %v", w, err)
						return
					}
					if _, err := pm.GetProviders(ctx, key); err != nil {
						t.Errorf("worker %d GetProviders: %v", w, err)
						return
					}
				}
			}
		}(w)
	}
	wg.Wait()

	for _, key := range mhs {
		got, err := pm.GetProviders(ctx, key)
		require.NoError(t, err)
		require.Lenf(t, got, workers, "key %x should have every worker as a provider", key)
	}
}

// TestProviderGCUnderConcurrentWrites verifies the parallel GC deletes expired
// records while a concurrent writer keeps refreshing a separate live set, which
// must survive untouched.
func TestProviderGCUnderConcurrentWrites(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const (
			provideValidity = 300 * time.Millisecond
			cleanupInterval = 50 * time.Millisecond
		)
		ctx := t.Context()

		store := dssync.MutexWrap(ds.NewMapDatastore())
		ps, err := pstoremem.NewPeerstore()
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, ps.Close()) })
		oldBatch := batchBufferSize
		batchBufferSize = 1
		t.Cleanup(func() { batchBufferSize = oldBatch })
		pm, err := NewProviderManager(testPeerID(t, "self"), ps, store,
			ProvideValidity(provideValidity), CleanupInterval(cleanupInterval))
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, pm.Close()) })

		prov := testPeerID(t, "prov")
		// expiring records are added once and never refreshed; GC must reclaim them.
		expiring := make([]mh.Multihash, 5)
		for i := range expiring {
			expiring[i] = internal.Hash(fmt.Append(nil, 1000+i))
			require.NoError(t, pm.AddProvider(ctx, expiring[i], peer.AddrInfo{ID: prov}))
		}
		// live records are refreshed by a concurrent writer; GC must never drop them.
		live := make([]mh.Multihash, 5)
		for i := range live {
			live[i] = internal.Hash(fmt.Append(nil, 2000+i))
		}

		writerCtx, stopWriter := context.WithCancel(ctx)
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for writerCtx.Err() == nil {
				for _, key := range live {
					if err := pm.AddProvider(writerCtx, key, peer.AddrInfo{ID: prov}); err != nil {
						t.Errorf("writer AddProvider: %v", err)
						return
					}
				}
				time.Sleep(cleanupInterval / 2)
			}
		}()

		// Let the expiring records age out and GC run several times while the writer
		// keeps the live records fresh. Synthetic time makes this instant, and the
		// writer refreshes every cleanupInterval/2, so every live record is at most
		// that old at each GC sweep and none is ever starved past provideValidity.
		time.Sleep(provideValidity + 4*cleanupInterval)
		stopWriter()
		wg.Wait()

		// Close flushes live pending writes and stops GC so the physical state is stable.
		require.NoError(t, pm.Close())

		countKeys := func(key mh.Multihash) int {
			res, err := store.Query(ctx, dsq.Query{Prefix: mkProvKey(key)})
			require.NoError(t, err)
			rest, err := res.Rest()
			require.NoError(t, err)
			return len(rest)
		}

		for _, key := range live {
			require.Equalf(t, 1, countKeys(key), "live record %x should survive GC", key)
		}
		for _, key := range expiring {
			require.Zerof(t, countKeys(key), "expiring record %x should be GC'd", key)
		}
	})
}

// TestCloseFencesDatastoreAccess pins the Close contract: once Close returns,
// AddProvider and GetProviders no longer touch the datastore and report
// ErrClosed, so the backing datastore can be closed right after the manager.
func TestCloseFencesDatastoreAccess(t *testing.T) {
	ctx := t.Context()
	ps, err := pstoremem.NewPeerstore()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, ps.Close()) })
	pm, err := NewProviderManager(testPeerID(t, "self"), ps, dssync.MutexWrap(ds.NewMapDatastore()))
	require.NoError(t, err)

	key := internal.Hash([]byte("cid"))
	require.NoError(t, pm.AddProvider(ctx, key, peer.AddrInfo{ID: testPeerID(t, "prov")}))
	require.NoError(t, pm.Close())

	require.ErrorIs(t, pm.AddProvider(ctx, key, peer.AddrInfo{ID: testPeerID(t, "prov2")}), ErrClosed)
	_, err = pm.GetProviders(ctx, key)
	require.ErrorIs(t, err, ErrClosed)

	require.NoError(t, pm.Close(), "Close must stay idempotent with the fence in place")
}

// GetProviders must honour context cancellation: a cancelled ctx yields the
// context error rather than a silent empty result, so callers' error checks are
// live rather than dead code.
func TestGetProvidersRespectsContextCancellation(t *testing.T) {
	ctx := t.Context()
	ps, err := pstoremem.NewPeerstore()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, ps.Close()) })
	pm, err := NewProviderManager(testPeerID(t, "self"), ps, dssync.MutexWrap(ds.NewMapDatastore()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, pm.Close()) })

	key := internal.Hash([]byte("cid"))
	require.NoError(t, pm.AddProvider(ctx, key, peer.AddrInfo{ID: testPeerID(t, "prov")}))

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	_, err = pm.GetProviders(canceled, key)
	require.ErrorIsf(t, err, context.Canceled, "a cancelled context must surface its error")
}

// sortedQueryDS returns query results ordered lexicographically by key, the way
// a real on-disk datastore (leveldb, badger) does. MapDatastore leaves results
// in random map-iteration order, which would mask whether GetProviders reorders
// them, so tests that assert on ordering wrap it in this.
type sortedQueryDS struct {
	ds.Batching
}

func (s sortedQueryDS) Query(ctx context.Context, q dsq.Query) (dsq.Results, error) {
	res, err := s.Batching.Query(ctx, q)
	if err != nil {
		return nil, err
	}
	entries, err := res.Rest()
	if err != nil {
		return nil, err
	}
	slices.SortFunc(entries, func(a, b dsq.Entry) int { return strings.Compare(a.Key, b.Key) })
	return dsq.ResultsWithEntries(q, entries), nil
}

// TestGetProvidersInvokesShuffle pins the wiring: every read — cache miss and
// cache hit alike — runs the injected shuffle exactly once over the full
// provider set, so ordering can never be served straight from the datastore.
func TestGetProvidersInvokesShuffle(t *testing.T) {
	ctx := t.Context()
	ps, err := pstoremem.NewPeerstore()
	require.NoError(t, err)
	pm, err := NewProviderManager(testPeerID(t, "self"), ps, dssync.MutexWrap(ds.NewMapDatastore()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, pm.Close()) })

	var seenN []int
	pm.shuffle = func(n int, _ func(i, j int)) { seenN = append(seenN, n) }

	key := internal.Hash([]byte("cid"))
	const n = 5
	for i := range n {
		require.NoError(t, pm.AddProvider(ctx, key, peer.AddrInfo{ID: testPeerID(t, fmt.Sprintf("prov-%d", i))}))
	}

	got, err := pm.GetProviders(ctx, key) // cache miss: loads from datastore
	require.NoError(t, err)
	require.Len(t, got, n)

	_, err = pm.GetProviders(ctx, key) // cache hit
	require.NoError(t, err)

	require.Equal(t, []int{n, n}, seenN, "both cache-miss and cache-hit reads must shuffle the full set")
}

// TestGetProvidersShufflesDatastoreOrder verifies that the lexicographic
// peer-ID order a real datastore returns is not passed straight through to
// callers, and that the returned order genuinely depends on the rand source —
// so client load is spread across a key's providers instead of always
// preferring the same few.
func TestGetProvidersShufflesDatastoreOrder(t *testing.T) {
	ctx := t.Context()
	key := internal.Hash([]byte("cid"))

	provs := make([]peer.ID, 12)
	for i := range provs {
		provs[i] = testPeerID(t, fmt.Sprintf("prov-%02d", i))
	}
	// Provider keys are base32(peerID) under a common prefix, and the datastore
	// returns them sorted by that key. base32's alphabet (A-Z2-7) does not sort
	// like the raw peer-ID bytes, so the pass-through order to beat is the
	// base32-key order, not a raw peer-ID sort.
	dsOrder := slices.Clone(provs)
	slices.SortFunc(dsOrder, func(a, b peer.ID) int {
		return strings.Compare(
			base32.RawStdEncoding.EncodeToString([]byte(a)),
			base32.RawStdEncoding.EncodeToString([]byte(b)),
		)
	})

	old := batchBufferSize
	batchBufferSize = 1
	t.Cleanup(func() { batchBufferSize = old })

	get := func(seed uint64) []peer.ID {
		ps, err := pstoremem.NewPeerstore()
		require.NoError(t, err)
		pm, err := NewProviderManager(testPeerID(t, "self"), ps,
			sortedQueryDS{dssync.MutexWrap(ds.NewMapDatastore())})
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, pm.Close()) })
		pm.shuffle = rand.New(rand.NewPCG(seed, seed)).Shuffle

		for _, p := range provs {
			require.NoError(t, pm.AddProvider(ctx, key, peer.AddrInfo{ID: p}))
		}
		got, err := pm.GetProviders(ctx, key)
		require.NoError(t, err)
		ids := make([]peer.ID, len(got))
		for i, ai := range got {
			ids[i] = ai.ID
		}
		return ids
	}

	orderA := get(1)
	orderB := get(2)

	require.ElementsMatch(t, provs, orderA, "shuffle must preserve the provider set")
	require.ElementsMatch(t, provs, orderB, "shuffle must preserve the provider set")
	require.NotEqual(t, dsOrder, orderA, "datastore order must not pass through")
	require.NotEqual(t, orderA, orderB, "different rand sources must yield different order")
}

// TestAddProviderBuffersUntilFlush checks writes stay off disk until Close,
// and that GetProviders serves the pending record without flushing it.
func TestAddProviderBuffersUntilFlush(t *testing.T) {
	ctx := t.Context()
	store := dssync.MutexWrap(ds.NewMapDatastore())
	ps, err := pstoremem.NewPeerstore()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, ps.Close()) })
	pm, err := NewProviderManager(testPeerID(t, "self"), ps, store)
	require.NoError(t, err)

	key := internal.Hash([]byte("cid"))
	prov := testPeerID(t, "prov")
	require.NoError(t, pm.AddProvider(ctx, key, peer.AddrInfo{ID: prov}))
	require.Empty(t, rawKeys(t, ctx, store), "unflushed write must not hit the datastore")

	got, err := pm.GetProviders(ctx, key)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, prov, got[0].ID)
	require.Empty(t, rawKeys(t, ctx, store), "lookup must not flush pending writes")

	require.NoError(t, pm.Close())
	require.Equal(t, []string{mkProvKeyFor(key, prov)}, rawKeys(t, ctx, store))
}

// TestExpiredPendingWriteIsNotServed pins the expiry filter in applyPending.
// Nothing flushes on a timer, so a node that never fills the buffer can hold a
// write well past provideValidity; the overlay must drop it from reads while
// leaving it in pending for a later flush to persist and GC to reclaim.
func TestExpiredPendingWriteIsNotServed(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const provideValidity = time.Hour

		ctx := t.Context()
		store := dssync.MutexWrap(ds.NewMapDatastore())
		ps, err := pstoremem.NewPeerstore()
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, ps.Close()) })

		// GC off, so nothing but the overlay can hide the record.
		pm, err := NewProviderManager(testPeerID(t, "self"), ps, store,
			ProvideValidity(provideValidity), CleanupInterval(0))
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, pm.Close()) })

		key := internal.Hash([]byte("cid"))
		require.NoError(t, pm.AddProvider(ctx, key, peer.AddrInfo{ID: testPeerID(t, "prov")}))
		require.Empty(t, rawKeys(t, ctx, store), "the write must stay buffered")

		time.Sleep(provideValidity + time.Minute)
		synctest.Wait()

		// A cache miss, so the read is served by loadProviderSet (empty) plus
		// applyPending: only applyPending's expiry check can drop the provider.
		provs, err := pm.GetProviders(ctx, key)
		require.NoError(t, err)
		require.Empty(t, provs, "an expired pending write must not be served")
		require.Equal(t, 1, pendingLen(pm), "but it stays buffered for a later flush")
	})
}

// TestAddProviderFlushesAtBatchSize checks the 256-record (here, shrunk)
// threshold commits pending writes as one batch.
func TestAddProviderFlushesAtBatchSize(t *testing.T) {
	old := batchBufferSize
	batchBufferSize = 8
	t.Cleanup(func() { batchBufferSize = old })

	ctx := t.Context()
	store := dssync.MutexWrap(ds.NewMapDatastore())
	ps, err := pstoremem.NewPeerstore()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, ps.Close()) })
	pm, err := NewProviderManager(testPeerID(t, "self"), ps, store)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, pm.Close()) })

	prov := testPeerID(t, "prov")
	for i := range batchBufferSize - 1 {
		require.NoError(t, pm.AddProvider(ctx, internal.Hash(fmt.Append(nil, i)), peer.AddrInfo{ID: prov}))
	}
	require.Empty(t, rawKeys(t, ctx, store), "writes below the flush threshold must stay pending")

	require.NoError(t, pm.AddProvider(ctx, internal.Hash(fmt.Append(nil, batchBufferSize-1)), peer.AddrInfo{ID: prov}))
	require.Len(t, rawKeys(t, ctx, store), batchBufferSize)
}

// errCommit is the failure returned by a flakyBatchDS commit.
var errCommit = errors.New("commit failed")

// flakyBatchDS is a Batching datastore whose first `failures` commits fail,
// standing in for a full disk or a wedged store. A negative count fails every
// commit.
type flakyBatchDS struct {
	ds.Batching
	failures int
}

type flakyBatch struct {
	ds.Batch
	parent *flakyBatchDS
}

func (f *flakyBatchDS) Batch(ctx context.Context) (ds.Batch, error) {
	b, err := f.Batching.Batch(ctx)
	if err != nil {
		return nil, err
	}
	return &flakyBatch{Batch: b, parent: f}, nil
}

func (b *flakyBatch) Commit(ctx context.Context) error {
	if b.parent.failures == 0 {
		return b.Batch.Commit(ctx)
	}
	b.parent.failures--
	return errCommit
}

// pendingLen reports the size of pm's unflushed write buffer.
func pendingLen(pm *ProviderManager) int {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	return len(pm.pending)
}

// TestFlushRetriesFailedCommitUntilCapped checks a datastore that cannot commit
// leaves its writes buffered so later writes retry them, and that the retrying
// is bounded: pending never grows past maxPendingWrites, at which point the
// buffer is dropped and starts refilling.
func TestFlushRetriesFailedCommitUntilCapped(t *testing.T) {
	const hardCap = 12 // two retries past the flush threshold
	oldBatch, oldMax := batchBufferSize, maxPendingWrites
	batchBufferSize, maxPendingWrites = 10, hardCap
	t.Cleanup(func() { batchBufferSize, maxPendingWrites = oldBatch, oldMax })

	ctx := t.Context()
	store := &flakyBatchDS{Batching: dssync.MutexWrap(ds.NewMapDatastore()), failures: -1}
	ps, err := pstoremem.NewPeerstore()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, ps.Close()) })
	pm, err := NewProviderManager(testPeerID(t, "self"), ps, store)
	require.NoError(t, err)

	prov := testPeerID(t, "prov")
	for i := range 3 * hardCap {
		err := pm.AddProvider(ctx, internal.Hash(fmt.Append(nil, i)), peer.AddrInfo{ID: prov})
		require.NoErrorf(t, err, "write %d must not inherit another write's flush failure", i)
		require.LessOrEqualf(t, pendingLen(pm), hardCap, "pending must stay capped after write %d", i)
	}

	// The last write above filled pending to the cap and dropped it, so the
	// cycle restarts: writes buffer again until the threshold retries the flush.
	require.Zero(t, pendingLen(pm), "reaching the cap must drop the buffer")
	require.NoError(t, pm.AddProvider(ctx, internal.Hash([]byte("next")), peer.AddrInfo{ID: prov}))
	require.Equal(t, 1, pendingLen(pm), "writes must buffer again after a drop")

	// Close is the one caller that asked for a flush, so it does get the error.
	require.ErrorIs(t, pm.Close(), errCommit, "Close must report the failed final flush")
	require.Empty(t, rawKeys(t, ctx, store.Batching), "no write may reach a store that cannot commit")
}

// TestFlushRetryRecoversTransientFailure checks the retry is worth keeping: a
// single failed commit loses nothing, because the next write re-commits the
// whole buffer once the datastore recovers.
func TestFlushRetryRecoversTransientFailure(t *testing.T) {
	oldBatch, oldMax := batchBufferSize, maxPendingWrites
	batchBufferSize, maxPendingWrites = 10, 12
	t.Cleanup(func() { batchBufferSize, maxPendingWrites = oldBatch, oldMax })

	ctx := t.Context()
	store := &flakyBatchDS{Batching: dssync.MutexWrap(ds.NewMapDatastore()), failures: 1}
	ps, err := pstoremem.NewPeerstore()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, ps.Close()) })
	pm, err := NewProviderManager(testPeerID(t, "self"), ps, store)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, pm.Close()) })

	prov := testPeerID(t, "prov")
	for i := range batchBufferSize - 1 {
		require.NoError(t, pm.AddProvider(ctx, internal.Hash(fmt.Append(nil, i)), peer.AddrInfo{ID: prov}))
	}

	// The threshold write trips the one failure the store has in it.
	require.NoError(t, pm.AddProvider(ctx, internal.Hash(fmt.Append(nil, batchBufferSize-1)), peer.AddrInfo{ID: prov}))
	require.Equal(t, batchBufferSize, pendingLen(pm), "a failed flush must keep its writes for the retry")
	require.Empty(t, rawKeys(t, ctx, store.Batching))

	// The next write retries, and the recovered store takes the whole buffer.
	require.NoError(t, pm.AddProvider(ctx, internal.Hash([]byte("recovered")), peer.AddrInfo{ID: prov}))
	require.Zero(t, pendingLen(pm))
	require.Lenf(t, rawKeys(t, ctx, store.Batching), batchBufferSize+1,
		"the retry must persist every record the failed flush held")
}

// stalledBatchDS is a Batching whose commits never complete on their own,
// standing in for a datastore that has stopped making progress but still
// honours ctx.
type stalledBatchDS struct {
	ds.Batching
}

type stalledBatch struct {
	ds.Batch
}

func (d *stalledBatchDS) Batch(ctx context.Context) (ds.Batch, error) {
	b, err := d.Batching.Batch(ctx)
	if err != nil {
		return nil, err
	}
	return &stalledBatch{Batch: b}, nil
}

func (b *stalledBatch) Commit(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}

// TestCloseGivesUpOnStalledDatastore checks the flush is bounded by
// flushTimeout rather than blocking shutdown forever, so IpfsDHT.Close cannot
// hang on a datastore that has stopped completing commits. AddProvider flushes
// through the same bound.
func TestCloseGivesUpOnStalledDatastore(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := t.Context()
		store := &stalledBatchDS{Batching: dssync.MutexWrap(ds.NewMapDatastore())}
		ps, err := pstoremem.NewPeerstore()
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, ps.Close()) })

		pm, err := NewProviderManager(testPeerID(t, "self"), ps, store)
		require.NoError(t, err)

		// One write, far below batchBufferSize, so only Close flushes it.
		require.NoError(t, pm.AddProvider(ctx, internal.Hash([]byte("cid")),
			peer.AddrInfo{ID: testPeerID(t, "prov")}))

		start := time.Now()
		require.ErrorIs(t, pm.Close(), context.DeadlineExceeded)
		require.Equal(t, flushTimeout, time.Since(start),
			"Close must give up after exactly one flushTimeout")
	})
}

// countingBatchDS counts Put, Delete, and Commit against a Batching datastore
// so tests can assert that GC and flushes batch instead of writing one-by-one.
type countingBatchDS struct {
	ds.Batching
	puts    int
	deletes int
	commits int
}

type countingBatch struct {
	ds.Batch
	parent *countingBatchDS
}

func (c *countingBatchDS) Put(ctx context.Context, key ds.Key, value []byte) error {
	c.puts++
	return c.Batching.Put(ctx, key, value)
}

func (c *countingBatchDS) Delete(ctx context.Context, key ds.Key) error {
	c.deletes++
	return c.Batching.Delete(ctx, key)
}

func (c *countingBatchDS) Batch(ctx context.Context) (ds.Batch, error) {
	b, err := c.Batching.Batch(ctx)
	if err != nil {
		return nil, err
	}
	return &countingBatch{Batch: b, parent: c}, nil
}

func (b *countingBatch) Put(ctx context.Context, key ds.Key, value []byte) error {
	b.parent.puts++
	return b.Batch.Put(ctx, key, value)
}

func (b *countingBatch) Delete(ctx context.Context, key ds.Key) error {
	b.parent.deletes++
	return b.Batch.Delete(ctx, key)
}

func (b *countingBatch) Commit(ctx context.Context) error {
	b.parent.commits++
	return b.Batch.Commit(ctx)
}

// TestProviderGCDeletesInBatches checks a leftover sweep commits once per
// batchBufferSize deletes, not once per record.
func TestProviderGCDeletesInBatches(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		old := batchBufferSize
		batchBufferSize = 10
		t.Cleanup(func() { batchBufferSize = old })

		const n = 25
		ctx := t.Context()
		store := &countingBatchDS{Batching: dssync.MutexWrap(ds.NewMapDatastore())}
		past := time.Now().Add(-time.Hour)
		prov := testPeerID(t, "prov")
		for i := range n {
			require.NoError(t, writeProviderEntry(ctx, store, internal.Hash(fmt.Append(nil, i)), prov, past))
		}
		require.Len(t, rawKeys(t, ctx, store), n)
		store.puts, store.deletes, store.commits = 0, 0, 0

		ps, err := pstoremem.NewPeerstore()
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, ps.Close()) })
		pm, err := NewProviderManager(testPeerID(t, "self"), ps, store,
			ProvideValidity(time.Minute), CleanupInterval(time.Hour))
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, pm.Close()) })

		time.Sleep(time.Hour)
		synctest.Wait()

		require.Empty(t, rawKeys(t, ctx, store), "expired records must be gone")
		require.Equal(t, n, store.deletes)
		require.Equal(t, (n+batchBufferSize-1)/batchBufferSize, store.commits)
		require.Zero(t, store.puts, "GC must not write records")
	})
}

// TestProviderReadsSurviveGCDeletingStalePendingKey checks that GC deleting a
// stale on-disk record does not lose a fresher write for the same key that is
// still unflushed in pending: GetProviders must keep serving it via the
// pending overlay regardless of what GC just did to the datastore, and Close
// must still land it on disk afterward. batchBufferSize is kept large so the
// fresh write never auto-flushes during the test.
func TestProviderReadsSurviveGCDeletingStalePendingKey(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const (
			provideValidity = time.Minute
			cleanupInterval = 10 * time.Second
		)
		old := batchBufferSize
		batchBufferSize = 256
		t.Cleanup(func() { batchBufferSize = old })

		ctx := t.Context()
		store := dssync.MutexWrap(ds.NewMapDatastore())
		key := internal.Hash([]byte("cid"))
		prov := testPeerID(t, "prov")

		// Seed disk with a record that is already well past provideValidity.
		staleTime := time.Now().Add(-2 * provideValidity)
		require.NoError(t, writeProviderEntry(ctx, store, key, prov, staleTime))

		ps, err := pstoremem.NewPeerstore()
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, ps.Close()) })
		pm, err := NewProviderManager(testPeerID(t, "self"), ps, store,
			ProvideValidity(provideValidity), CleanupInterval(cleanupInterval))
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, pm.Close()) })

		// A fresh reprovide lands in pending only; batchBufferSize is far from
		// reached so it must not touch disk yet.
		freshTime := time.Now()
		require.NoError(t, pm.AddProvider(ctx, key, peer.AddrInfo{ID: prov}))
		require.Equal(t, []string{mkProvKeyFor(key, prov)}, rawKeys(t, ctx, store),
			"unflushed write must not overwrite disk yet")

		// Let GC run once. The on-disk record is old enough to be swept, and
		// nothing stops it: GC deletes it even though the key is still pending.
		time.Sleep(cleanupInterval)
		synctest.Wait()
		require.Empty(t, rawKeys(t, ctx, store), "GC sweeps the stale on-disk record")

		// The pending overlay must still serve the fresh write, disk state
		// notwithstanding.
		got, err := pm.GetProviders(ctx, key)
		require.NoError(t, err)
		require.Len(t, got, 1, "the pending write must still be visible after GC deletes the stale disk copy")

		require.NoError(t, pm.Close())
		entries, err := store.Query(ctx, dsq.Query{Prefix: mkProvKey(key)})
		require.NoError(t, err)
		rest, err := entries.Rest()
		require.NoError(t, err)
		require.Len(t, rest, 1)
		ts, err := readTimeValue(rest[0].Value)
		require.NoError(t, err)
		require.True(t, ts.Equal(freshTime), "Close must flush the pending write back to disk")
	})
}
