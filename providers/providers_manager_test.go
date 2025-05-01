package providers

import (
	"context"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p-kad-dht/internal"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/host/peerstore/pstoremem"

	mh "github.com/multiformats/go-multihash"

	ds "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
	dssync "github.com/ipfs/go-datastore/sync"
	//
	// used by TestLargeProvidersSet: do not remove
	// lds "github.com/ipfs/go-ds-leveldb"
)

func TestProviderManager(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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
	for i := 0; i < 100; i++ {
		h := internal.Hash([]byte(fmt.Sprint(i)))
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

	pset, err := loadProviderSet(context.Background(), dstore, k)
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
	pval := ProvideValidity
	cleanup := defaultCleanupInterval
	ProvideValidity = time.Second / 2
	defaultCleanupInterval = time.Second / 10
	defer func() {
		ProvideValidity = pval
		defaultCleanupInterval = cleanup
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ds := dssync.MutexWrap(ds.NewMapDatastore())
	mid := peer.ID("testing")
	ps, err := pstoremem.NewPeerstore()
	if err != nil {
		t.Fatal(err)
	}
	p, err := NewProviderManager(ctx, mid, ps, ds)
	if err != nil {
		t.Fatal(err)
	}

	peers := []peer.ID{"a", "b"}
	var mhs []mh.Multihash
	for i := 0; i < 10; i++ {
		h := internal.Hash([]byte(fmt.Sprint(i)))
		mhs = append(mhs, h)
	}

	for _, h := range mhs[:5] {
		p.AddProvider(ctx, h, peer.AddrInfo{ID: peers[0]})
		p.AddProvider(ctx, h, peer.AddrInfo{ID: peers[1]})
	}

	time.Sleep(ProvideValidity / 2)

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

	time.Sleep(ProvideValidity/2 + 2*defaultCleanupInterval)

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

	time.Sleep(ProvideValidity)

	// Stop to prevent data races
	p.Close()

	if p.cache.Len() != 0 {
		t.Fatal("providers map not cleaned up")
	}

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
	for i := 0; i < 3000; i++ {
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
	for i := 0; i < 1000; i++ {
		h := internal.Hash([]byte(fmt.Sprint(i)))
		mhs = append(mhs, h)
		for _, pid := range peers {
			p.AddProvider(ctx, h, peer.AddrInfo{ID: pid})
		}
	}

	for i := 0; i < 5; i++ {
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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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
