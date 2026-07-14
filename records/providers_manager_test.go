package records

import (
	"context"
	"fmt"
	"io"
	"math/rand"
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

	mh "github.com/multiformats/go-multihash"

	ds "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
	dssync "github.com/ipfs/go-datastore/sync"
	//
	// used by TestLargeProvidersSet: do not remove
	// lds "github.com/ipfs/go-ds-leveldb"
)

func TestProviderManager(t *testing.T) {
	ctx := t.Context()

	mid := peer.ID("testing")
	ps, err := pstoremem.NewPeerstore()
	if err != nil {
		t.Fatal(err)
	}
	p, err := NewProviderManager(ctx, mid, ps, dssync.MutexWrap(ds.NewMapDatastore()))
	if err != nil {
		t.Fatal(err)
	}
	a := internal.Hash([]byte("test"))
	p.AddProvider(ctx, a, peer.AddrInfo{ID: peer.ID("testingprovider")})

	// Not cached
	// TODO verify that cache is empty
	resp, _ := p.GetProviders(ctx, a)
	if len(resp) != 1 {
		t.Fatal("Could not retrieve provider.")
	}

	// Cached
	// TODO verify that cache is populated
	resp, _ = p.GetProviders(ctx, a)
	if len(resp) != 1 {
		t.Fatal("Could not retrieve provider.")
	}

	p.AddProvider(ctx, a, peer.AddrInfo{ID: peer.ID("testingprovider2")})
	p.AddProvider(ctx, a, peer.AddrInfo{ID: peer.ID("testingprovider3")})
	// TODO verify that cache is already up to date
	resp, _ = p.GetProviders(ctx, a)
	if len(resp) != 3 {
		t.Fatalf("Should have got 3 providers, got %d", len(resp))
	}

	p.Close()
}

func TestProvidersDatastore(t *testing.T) {
	old := lruCacheSize
	lruCacheSize = 10
	defer func() { lruCacheSize = old }()

	ctx := t.Context()

	mid := peer.ID("testing")
	ps, err := pstoremem.NewPeerstore()
	if err != nil {
		t.Fatal(err)
	}

	p, err := NewProviderManager(ctx, mid, ps, dssync.MutexWrap(ds.NewMapDatastore()))
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	friend := peer.ID("friend")
	var mhs []mh.Multihash
	for i := range 100 {
		h := internal.Hash(fmt.Append(nil, i))
		mhs = append(mhs, h)
		p.AddProvider(ctx, h, peer.AddrInfo{ID: friend})
	}

	for _, c := range mhs {
		resp, _ := p.GetProviders(ctx, c)
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
	p1 := peer.ID("peer one")
	p2 := peer.ID("peer two")
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

	ctx := t.Context()

	ds := dssync.MutexWrap(ds.NewMapDatastore())
	mid := peer.ID("testing")
	ps, err := pstoremem.NewPeerstore()
	if err != nil {
		t.Fatal(err)
	}
	p, err := NewProviderManager(ctx, mid, ps, ds, ProvideValidity(provideValidity), CleanupInterval(cleanupInterval))
	if err != nil {
		t.Fatal(err)
	}

	peers := []peer.ID{"a", "b"}
	var mhs []mh.Multihash
	for i := range 10 {
		h := internal.Hash(fmt.Append(nil, i))
		mhs = append(mhs, h)
	}

	for _, h := range mhs[:5] {
		p.AddProvider(ctx, h, peer.AddrInfo{ID: peers[0]})
		p.AddProvider(ctx, h, peer.AddrInfo{ID: peers[1]})
	}

	time.Sleep(provideValidity / 2)

	for _, h := range mhs[5:] {
		p.AddProvider(ctx, h, peer.AddrInfo{ID: peers[0]})
		p.AddProvider(ctx, h, peer.AddrInfo{ID: peers[1]})
	}

	for _, h := range mhs {
		out, _ := p.GetProviders(ctx, h)
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
		out, _ := p.GetProviders(ctx, h)
		if len(out) > 0 {
			t.Fatal("expected providers to be cleaned up, got: ", out)
		}
	}

	for _, h := range mhs[5:] {
		out, _ := p.GetProviders(ctx, h)
		if len(out) != 2 {
			t.Fatal("expected providers to still be there")
		}
	}

	time.Sleep(provideValidity)

	// Stop to prevent data races
	p.Close()

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
		mid := peer.ID("testing")
		ps, err := pstoremem.NewPeerstore()
		require.NoError(t, err)
		t.Cleanup(func() { ps.Close() })
		p, err := NewProviderManager(ctx, mid, ps, dstore, ProvideValidity(provideValidity), CleanupInterval(cleanupInterval))
		require.NoError(t, err)
		t.Cleanup(func() { p.Close() })

		mhs := make([]mh.Multihash, 2)
		for i := range mhs {
			mhs[i] = internal.Hash(fmt.Append(nil, i))
		}

		peers := []peer.ID{"a", "b"}
		for i, h := range mhs {
			p.AddProvider(ctx, h, peer.AddrInfo{ID: peers[0]})
			p.GetProviders(ctx, h)
			require.Len(t, p.cache.Keys(), i+1)
		}

		time.Sleep(provideValidity / 2)

		p.AddProvider(ctx, mhs[0], peer.AddrInfo{ID: peers[1]})
		// AddProvider updates the cached providerSet synchronously; wait for the
		// background GC goroutine to settle before reading the cache directly.
		synctest.Wait()
		cached, _ := p.cache.Get(string(mhs[0]))
		require.Len(t, cached.(*providerSet).providers, 2)

		// Sleep slightly past provideValidity so the expiry check
		// time.Since(v)>provideValidity triggers for the first batch.
		time.Sleep(provideValidity/2 + time.Millisecond)

		out, _ := p.GetProviders(ctx, mhs[0])
		require.Len(t, out, 1, "expected one provider to have expired")
		cached, _ = p.cache.Get(string(mhs[0]))
		require.Len(t, cached.(*providerSet).providers, 1)

		out, _ = p.GetProviders(ctx, mhs[1])
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
		peers = append(peers, peer.ID(fmt.Sprint(i)))
	}

	mid := peer.ID("myself")
	ps, err := pstoremem.NewPeerstore()
	if err != nil {
		t.Fatal(err)
	}

	p, err := NewProviderManager(ctx, mid, ps, dstore)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	var mhs []mh.Multihash
	for i := range 1000 {
		h := internal.Hash(fmt.Append(nil, i))
		mhs = append(mhs, h)
		for _, pid := range peers {
			p.AddProvider(ctx, h, peer.AddrInfo{ID: pid})
		}
	}

	for range 5 {
		start := time.Now()
		for _, h := range mhs {
			_, _ = p.GetProviders(ctx, h)
		}
		elapsed := time.Since(start)
		fmt.Printf("query %f ms\n", elapsed.Seconds()*1000)
	}
}

func TestUponCacheMissProvidersAreReadFromDatastore(t *testing.T) {
	old := lruCacheSize
	lruCacheSize = 1
	defer func() { lruCacheSize = old }()
	ctx := t.Context()

	p1, p2 := peer.ID("a"), peer.ID("b")
	h1 := internal.Hash([]byte("1"))
	h2 := internal.Hash([]byte("2"))
	ps, err := pstoremem.NewPeerstore()
	if err != nil {
		t.Fatal(err)
	}

	pm, err := NewProviderManager(ctx, p1, ps, dssync.MutexWrap(ds.NewMapDatastore()))
	if err != nil {
		t.Fatal(err)
	}

	// add provider
	pm.AddProvider(ctx, h1, peer.AddrInfo{ID: p1})
	// make the cached provider for h1 go to datastore
	pm.AddProvider(ctx, h2, peer.AddrInfo{ID: p1})
	// now just offloaded record should be brought back and joined with p2
	pm.AddProvider(ctx, h1, peer.AddrInfo{ID: p2})

	h1Provs, _ := pm.GetProviders(ctx, h1)
	if len(h1Provs) != 2 {
		t.Fatalf("expected h1 to be provided by 2 peers, is by %d", len(h1Provs))
	}
}

func TestWriteUpdatesCache(t *testing.T) {
	ctx := t.Context()

	p1, p2 := peer.ID("a"), peer.ID("b")
	h1 := internal.Hash([]byte("1"))
	ps, err := pstoremem.NewPeerstore()
	if err != nil {
		t.Fatal(err)
	}

	pm, err := NewProviderManager(ctx, p1, ps, dssync.MutexWrap(ds.NewMapDatastore()))
	if err != nil {
		t.Fatal(err)
	}

	// add provider
	pm.AddProvider(ctx, h1, peer.AddrInfo{ID: p1})
	// force into the cache
	pm.GetProviders(ctx, h1)
	// add a second provider
	pm.AddProvider(ctx, h1, peer.AddrInfo{ID: p2})

	c1Provs, _ := pm.GetProviders(ctx, h1)
	if len(c1Provs) != 2 {
		t.Fatalf("expected h1 to be provided by 2 peers, is by %d", len(c1Provs))
	}
}

// rawKeys returns every physical key stored in dstore.
func rawKeys(t *testing.T, ctx context.Context, dstore ds.Datastore) []string {
	t.Helper()
	res, err := dstore.Query(ctx, dsq.Query{KeysOnly: true})
	require.NoError(t, err)
	defer res.Close()

	var keys []string
	for e := range res.Next() {
		require.NoError(t, e.Error)
		keys = append(keys, e.Key)
	}
	return keys
}

// TestProviderKeyScheme verifies provider records are stored under the
// "/providers/" namespace prefix, one datastore key per (key, provider) pair.
func TestProviderKeyScheme(t *testing.T) {
	ctx := t.Context()
	store := dssync.MutexWrap(ds.NewMapDatastore())
	ps, err := pstoremem.NewPeerstore()
	require.NoError(t, err)
	pm, err := NewProviderManager(ctx, peer.ID("self"), ps, store)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, pm.Close()) })

	key := internal.Hash([]byte("cid"))
	prov := peer.ID("prov")
	require.NoError(t, pm.AddProvider(ctx, key, peer.AddrInfo{ID: prov}))
	// AddProvider writes straight through to the datastore.
	got, err := pm.GetProviders(ctx, key)
	require.NoError(t, err)
	require.Len(t, got, 1)

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
	t.Cleanup(func() { ps.Close() })

	pm, err := NewProviderManager(ctx, peer.ID("self"), ps,
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
			prov := peer.ID(fmt.Sprintf("prov-%d", w))
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
	provideValidity := 300 * time.Millisecond
	cleanupInterval := 50 * time.Millisecond
	ctx := t.Context()

	store := dssync.MutexWrap(ds.NewMapDatastore())
	ps, err := pstoremem.NewPeerstore()
	require.NoError(t, err)
	t.Cleanup(func() { ps.Close() })
	pm, err := NewProviderManager(ctx, peer.ID("self"), ps, store,
		ProvideValidity(provideValidity), CleanupInterval(cleanupInterval))
	require.NoError(t, err)

	prov := peer.ID("prov")
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
				pm.AddProvider(writerCtx, key, peer.AddrInfo{ID: prov})
			}
			time.Sleep(cleanupInterval / 2)
		}
	}()

	// Let the expiring records age out and GC run several times while the writer
	// keeps the live records fresh.
	time.Sleep(provideValidity + 4*cleanupInterval)
	stopWriter()
	wg.Wait()

	// Stop GC before inspecting the datastore so the physical state is stable.
	require.NoError(t, pm.Close())

	// GC deletes without the write lock, so a live key whose writer was starved
	// past provideValidity mid-run could have been reclaimed — the sanctioned
	// GC-vs-write race that self-heals on the next provide. Re-provide each live
	// key now that GC is stopped so the assertion tests that self-heal guarantee
	// deterministically rather than the exact-survival negation of the race.
	for _, key := range live {
		require.NoError(t, pm.AddProvider(ctx, key, peer.AddrInfo{ID: prov}))
	}

	countKeys := func(key mh.Multihash) int {
		res, err := store.Query(ctx, dsq.Query{Prefix: mkProvKey(key)})
		require.NoError(t, err)
		rest, err := res.Rest()
		require.NoError(t, err)
		return len(rest)
	}

	for _, key := range live {
		require.Equalf(t, 1, countKeys(key), "live record %x should survive/self-heal", key)
	}
	for _, key := range expiring {
		require.Zerof(t, countKeys(key), "expiring record %x should be GC'd", key)
	}
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
	pm, err := NewProviderManager(ctx, peer.ID("self"), ps, dssync.MutexWrap(ds.NewMapDatastore()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, pm.Close()) })

	var seenN []int
	pm.shuffle = func(n int, _ func(i, j int)) { seenN = append(seenN, n) }

	key := internal.Hash([]byte("cid"))
	const n = 5
	for i := range n {
		require.NoError(t, pm.AddProvider(ctx, key, peer.AddrInfo{ID: peer.ID(fmt.Sprintf("prov-%d", i))}))
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
		provs[i] = peer.ID(fmt.Sprintf("prov-%02d", i))
	}
	// Provider keys are base32(peerID) under a common prefix, and base32 is
	// order-preserving, so the datastore hands them back sorted by peer ID.
	lexOrder := slices.Clone(provs)
	slices.SortFunc(lexOrder, func(a, b peer.ID) int { return strings.Compare(string(a), string(b)) })

	get := func(seed int64) []peer.ID {
		ps, err := pstoremem.NewPeerstore()
		require.NoError(t, err)
		pm, err := NewProviderManager(ctx, peer.ID("self"), ps,
			sortedQueryDS{dssync.MutexWrap(ds.NewMapDatastore())})
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, pm.Close()) })
		pm.shuffle = rand.New(rand.NewSource(seed)).Shuffle

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
	require.NotEqual(t, lexOrder, orderA, "datastore lex order must not pass through")
	require.NotEqual(t, orderA, orderB, "different rand sources must yield different order")
}
