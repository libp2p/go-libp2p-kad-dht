package keystore

import (
	"context"
	"crypto/rand"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	"github.com/libp2p/go-libp2p-kad-dht/provider/internal/keyspace"
	mh "github.com/multiformats/go-multihash"

	"github.com/ipfs/go-libdht/kad/key"
	"github.com/ipfs/go-libdht/kad/key/bit256"
	"github.com/ipfs/go-libdht/kad/key/bitstr"

	"github.com/ipfs/go-test/random"
	"github.com/stretchr/testify/require"
)

func TestKeystorePutAndGet(t *testing.T) {
	t.Run("Keystore", func(t *testing.T) {
		ds := ds.NewMapDatastore()
		defer ds.Close()
		store, err := NewKeystore(ds)
		require.NoError(t, err)
		defer store.Close()

		testKeyStorePutAndGetImpl(t, store)
	})

	t.Run("ResettableKeystore", func(t *testing.T) {
		ds := ds.NewMapDatastore()
		defer ds.Close()
		store, err := NewResettableKeystore(ds)
		require.NoError(t, err)
		defer store.Close()

		testKeyStorePutAndGetImpl(t, store)
	})
}

func testKeyStorePutAndGetImpl(t *testing.T, store Keystore) {
	mhs := make([]mh.Multihash, 6)
	for i := range mhs {
		h, err := mh.Sum([]byte{byte(i)}, mh.SHA2_256, -1)
		require.NoError(t, err)
		mhs[i] = h
	}

	added, err := store.Put(context.Background(), mhs...)
	require.NoError(t, err)
	require.Len(t, added, len(mhs))

	added, err = store.Put(context.Background(), mhs...)
	require.NoError(t, err)
	require.Empty(t, added)

	for _, h := range mhs {
		prefix := bitstr.Key(key.BitString(keyspace.MhToBit256(h))[:6])
		got, err := store.Get(context.Background(), prefix)
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, m := range got {
			if string(m) == string(h) {
				found = true
				break
			}
		}
		require.True(t, found, "expected to find multihash %v for prefix %s", h, prefix)
	}

	p := bitstr.Key(key.BitString(keyspace.MhToBit256(mhs[0]))[:3])
	res, err := store.Get(context.Background(), p)
	require.NoError(t, err)
	require.NotEmpty(t, res, "expected results for prefix %s", p)

	longPrefix := bitstr.Key(key.BitString(keyspace.MhToBit256(mhs[0]))[:15])
	res, err = store.Get(context.Background(), longPrefix)
	if err != nil {
		t.Fatal(err)
	}
	for _, h := range res {
		bs := bitstr.Key(key.BitString(keyspace.MhToBit256(h)))
		require.True(t, keyspace.IsPrefix(longPrefix, bs), "returned hash does not match long prefix")
	}
}

func genMultihashesMatchingPrefix(prefix bitstr.Key, n int) []mh.Multihash {
	mhs := make([]mh.Multihash, 0, n)
	for i := 0; len(mhs) < n; i++ {
		h := random.Multihashes(1)[0]
		k := keyspace.MhToBit256(h)
		if keyspace.IsPrefix(prefix, k) {
			mhs = append(mhs, h)
		}
	}
	return mhs
}

// mapDatastoreFactory returns an in-memory WithDatastoreFactory pair, mirroring
// how Kubo runs the ResettableKeystore in factory mode (separate per-namespace
// datastores) but without touching the filesystem.
func mapDatastoreFactory() (func(string) (ds.Batching, error), func(string) error) {
	stores := map[string]ds.Batching{}
	create := func(suffix string) (ds.Batching, error) {
		d := ds.NewMapDatastore()
		stores[suffix] = d
		return d, nil
	}
	destroy := func(suffix string) error {
		if d, ok := stores[suffix]; ok {
			_ = d.Close()
			delete(stores, suffix)
		}
		return nil
	}
	return create, destroy
}

func TestKeyStoreContainsPrefix(t *testing.T) {
	t.Run("Keystore", func(t *testing.T) {
		ds := ds.NewMapDatastore()
		defer ds.Close()
		store, err := NewKeystore(ds)
		require.NoError(t, err)
		defer store.Close()

		testKeystoreContainsPrefixImpl(t, store)
	})

	t.Run("ResettableKeystore", func(t *testing.T) {
		ds := ds.NewMapDatastore()
		defer ds.Close()
		store, err := NewResettableKeystore(ds)
		require.NoError(t, err)
		defer store.Close()

		testKeystoreContainsPrefixImpl(t, store)
	})
}

func testKeystoreContainsPrefixImpl(t *testing.T, store Keystore) {
	ctx := context.Background()

	ok, err := store.ContainsPrefix(ctx, bitstr.Key("0000"))
	require.NoError(t, err)
	require.False(t, ok)

	generated := genMultihashesMatchingPrefix(bitstr.Key(strings.Repeat("0", 10)), 1)
	require.True(t, keyspace.IsPrefix(bitstr.Key("0000"), keyspace.MhToBit256(generated[0])))
	store.Put(ctx, generated...)

	ok, err = store.ContainsPrefix(ctx, bitstr.Key("0"))
	require.NoError(t, err)
	require.True(t, ok)

	ok, err = store.ContainsPrefix(ctx, bitstr.Key("0000"))
	require.NoError(t, err)
	require.True(t, ok)

	ok, err = store.ContainsPrefix(ctx, bitstr.Key(strings.Repeat("0", 6)))
	require.NoError(t, err)
	require.True(t, ok)

	ok, err = store.ContainsPrefix(ctx, bitstr.Key(strings.Repeat("0", 10)))
	require.NoError(t, err)
	require.True(t, ok)

	ok, err = store.ContainsPrefix(ctx, bitstr.Key("1"))
	require.NoError(t, err)
	require.False(t, ok)

	ok, err = store.ContainsPrefix(ctx, bitstr.Key("0001"))
	require.NoError(t, err)
	require.False(t, ok)

	ok, err = store.ContainsPrefix(ctx, bitstr.Key(strings.Repeat("0", 8)+"1"))
	require.NoError(t, err)
	require.False(t, ok)
}

func TestKeystoreCountKeysUpTo(t *testing.T) {
	// A small prefixBits keeps the "long" prefixes that exercise countUpTo's
	// per-key decodeKey/IsPrefix filter (those exceeding prefixBits) only a few
	// bits long, so the rejection sampling below stays cheap.
	const prefixBits = 8
	bucket := bitstr.Key(strings.Repeat("0", prefixBits))      // "00000000"
	otherBucket := bitstr.Key(strings.Repeat("1", prefixBits)) // "11111111"
	// Generate the keys once and reuse them across both store implementations.
	bucketKeys := genMultihashesMatchingPrefix(bucket, 3)
	otherKeys := genMultihashesMatchingPrefix(otherBucket, 2)

	t.Run("Keystore", func(t *testing.T) {
		ds := ds.NewMapDatastore()
		defer ds.Close()
		store, err := NewKeystore(ds, WithPrefixBits(prefixBits))
		require.NoError(t, err)
		defer store.Close()

		testKeystoreCountKeysUpToImpl(t, store, bucket, bucketKeys, otherKeys)
	})

	// Exercise the ResettableKeystore in both storage modes. They take
	// different datastore paths that countUpTo's long-prefix decodeKey/IsPrefix
	// filter must round-trip correctly: shared mode wraps both reset slots under
	// the non-colliding "/k0" and "/k1" prefixes via namespace.Wrap (see
	// sharedSlotPrefix), while factory mode (how Kubo runs it, for full disk
	// reclamation) keeps each namespace in its own datastore.
	t.Run("ResettableKeystore/shared", func(t *testing.T) {
		store, err := NewResettableKeystore(ds.NewMapDatastore(),
			KeystoreOption(WithPrefixBits(prefixBits)))
		require.NoError(t, err)
		defer store.Close()

		testKeystoreCountKeysUpToImpl(t, store, bucket, bucketKeys, otherKeys)
	})

	t.Run("ResettableKeystore/factory", func(t *testing.T) {
		create, destroy := mapDatastoreFactory()
		store, err := NewResettableKeystore(ds.NewMapDatastore(),
			WithDatastoreFactory(create, destroy),
			KeystoreOption(WithPrefixBits(prefixBits)))
		require.NoError(t, err)
		defer store.Close()

		testKeystoreCountKeysUpToImpl(t, store, bucket, bucketKeys, otherKeys)
	})
}

func testKeystoreCountKeysUpToImpl(t *testing.T, store Keystore, bucket bitstr.Key, bucketKeys, otherKeys []mh.Multihash) {
	ctx := context.Background()

	// Empty keystore counts zero (limit 0 means no cap).
	n, err := store.CountKeysUpTo(ctx, bitstr.Key("1"), 0)
	require.NoError(t, err)
	require.Zero(t, n)

	// bucketKeys share the bucket prefix (prefixBits), so they land in one
	// datastore bucket. A prefix longer than prefixBits then exercises countUpTo's
	// per-key filter (decodeKey and IsPrefix), the path that differs from a
	// plain datastore prefix scan. otherKeys live in a different bucket so counts
	// must filter rather than just total the keystore.
	_, err = store.Put(ctx, bucketKeys...)
	require.NoError(t, err)
	_, err = store.Put(ctx, otherKeys...)
	require.NoError(t, err)

	// A prefix 4 bits longer than the bucket (so > prefixBits), derived from a
	// real key so it matches that key while the filter discards bucket peers
	// that diverge in the extra bits.
	longPrefix := bitstr.Key(key.BitString(keyspace.MhToBit256(bucketKeys[0]))[:len(bucket)+4])

	// With no cap (limit 0), CountKeysUpTo must agree with len(Get) for every
	// prefix, across the short (<=prefixBits) and long (>prefixBits) branches.
	for _, prefix := range []bitstr.Key{"0", "1", bucket, longPrefix} {
		got, err := store.Get(ctx, prefix)
		require.NoError(t, err)
		n, err := store.CountKeysUpTo(ctx, prefix, 0)
		require.NoError(t, err)
		require.Equalf(t, len(got), n, "CountKeysUpTo disagrees with Get for prefix %q", prefix)
	}

	// Exact uncapped counts: the full bucket, empty prefix and a prefix matching
	// neither bucket in a non-empty keystore.
	n, err = store.CountKeysUpTo(ctx, bucket, 0)
	require.NoError(t, err)
	require.Equal(t, len(bucketKeys), n)

	n, err = store.CountKeysUpTo(ctx, "", 0)
	require.NoError(t, err)
	require.Equal(t, len(bucketKeys)+len(otherKeys), n)

	n, err = store.CountKeysUpTo(ctx, bitstr.Key("01"), 0)
	require.NoError(t, err)
	require.Zero(t, n)

	// A negative limit means no cap, just like 0.
	n, err = store.CountKeysUpTo(ctx, "", -1)
	require.NoError(t, err)
	require.Equal(t, len(bucketKeys)+len(otherKeys), n)

	// A positive limit caps the result at min(total, limit). Sweep limits below,
	// at and above the total for the short branch (bucket, ""), the long branch
	// (longPrefix), and a non-matching prefix, so the early-exit and cap hold on
	// every path.
	for _, prefix := range []bitstr.Key{bucket, "", longPrefix, bitstr.Key("01")} {
		total, err := store.CountKeysUpTo(ctx, prefix, 0)
		require.NoError(t, err)
		for limit := 1; limit <= total+2; limit++ {
			n, err := store.CountKeysUpTo(ctx, prefix, limit)
			require.NoError(t, err)
			require.Equalf(t, min(total, limit), n, "CountKeysUpTo(%q, %d)", prefix, limit)
		}
	}
}

// countingDatastore wraps a datastore and counts how many query entries are
// pulled through it, so a test can assert that CountKeysUpTo stops scanning once
// it has enough matches instead of reading the whole match set.
type countingDatastore struct {
	ds.Batching
	reads atomic.Int64
}

func (c *countingDatastore) Query(ctx context.Context, q query.Query) (query.Results, error) {
	res, err := c.Batching.Query(ctx, q)
	if err != nil {
		return nil, err
	}
	// Re-serve the underlying entries through an iterator that counts each one
	// the consumer pulls. The inner datastore still applies q (prefix, limit,
	// keys-only), so a short-prefix count delegating its cap to query.Limit reads
	// only that many entries here.
	entries, err := res.Rest()
	if err != nil {
		return nil, err
	}
	i := 0
	return query.ResultsFromIterator(q, query.Iterator{
		Next: func() (query.Result, bool) {
			if i >= len(entries) {
				return query.Result{}, false
			}
			e := entries[i]
			i++
			c.reads.Add(1)
			return query.Result{Entry: e}, true
		},
	}), nil
}

func TestKeystoreCountKeysUpToStopsEarly(t *testing.T) {
	// prefixBits 0 keeps every key under a single datastore path, so a non-empty
	// prefix always takes countUpTo's per-key filter branch (BitLen > prefixBits)
	// while the empty prefix takes the query.Limit-delegation branch. Every key
	// matches the 1-bit prefix "0", so they are cheap to generate yet numerous
	// enough that a full scan is plainly distinguishable from an early exit.
	const total = 512
	keys := genMultihashesMatchingPrefix("0", total)

	newStore := func(t *testing.T) (Keystore, *countingDatastore) {
		c := &countingDatastore{Batching: ds.NewMapDatastore()}
		store, err := NewKeystore(c, WithPrefixBits(0))
		require.NoError(t, err)
		_, err = store.Put(context.Background(), keys...)
		require.NoError(t, err)
		c.reads.Store(0) // discard reads from setup
		t.Cleanup(func() { _ = store.Close() })
		return store, c
	}

	t.Run("filter branch breaks at the cap", func(t *testing.T) {
		// "0" matches every key but is longer than prefixBits, so countUpTo filters
		// per key and relies on its own break to stop. A small cap returns the cap
		// and reads far fewer than total entries (the cap plus the datastore's
		// keys-only read-ahead buffer, not the whole bucket).
		const limit = 3
		store, c := newStore(t)
		n, err := store.CountKeysUpTo(context.Background(), "0", limit)
		require.NoError(t, err)
		require.Equal(t, limit, n)
		require.Lessf(t, c.reads.Load(), int64(total),
			"capped count read %d of %d entries instead of stopping early", c.reads.Load(), total)
	})

	t.Run("filter branch scans everything when uncapped", func(t *testing.T) {
		store, c := newStore(t)
		n, err := store.CountKeysUpTo(context.Background(), "0", 0)
		require.NoError(t, err)
		require.Equal(t, total, n)
		require.Equalf(t, int64(total), c.reads.Load(),
			"uncapped count read %d entries, expected every one of %d", c.reads.Load(), total)
	})

	t.Run("short prefix delegates the cap to the query", func(t *testing.T) {
		// The empty prefix has BitLen 0 (<= prefixBits), so countUpTo sets
		// query.Limit and the datastore hands back exactly the cap entries.
		for _, limit := range []int{1, 3, 50} {
			store, c := newStore(t)
			n, err := store.CountKeysUpTo(context.Background(), "", limit)
			require.NoError(t, err)
			require.Equal(t, limit, n)
			require.Equalf(t, int64(limit), c.reads.Load(),
				"short-prefix count read %d entries for cap %d", c.reads.Load(), limit)
		}
	})
}

// failingQueryDatastore makes every datastore query fail once armed. It lets a
// test drive countUpTo's query down its error path without disturbing the size
// scan that the keystore runs at startup.
type failingQueryDatastore struct {
	ds.Batching
	fail atomic.Bool
	err  error
}

func (d *failingQueryDatastore) Query(ctx context.Context, q query.Query) (query.Results, error) {
	if d.fail.Load() {
		return nil, d.err
	}
	return d.Batching.Query(ctx, q)
}

// TestKeystoreCountKeysUpToPropagatesQueryError checks that CountKeysUpTo
// surfaces a datastore query failure to its caller. The provider reschedules a
// region when this count fails, so a swallowed error would silently skip that
// reschedule.
func TestKeystoreCountKeysUpToPropagatesQueryError(t *testing.T) {
	ctx := context.Background()
	wantErr := errors.New("datastore query failed")
	d := &failingQueryDatastore{Batching: ds.NewMapDatastore(), err: wantErr}

	store, err := NewKeystore(d)
	require.NoError(t, err)
	defer store.Close()

	// Size completes the startup size scan, so arming the failure now affects
	// only the count query that follows.
	_, err = store.Size(ctx)
	require.NoError(t, err)
	d.fail.Store(true)

	n, err := store.CountKeysUpTo(ctx, bitstr.Key("1"), 0)
	require.ErrorIs(t, err, wantErr)
	require.Zero(t, n)
}

// TestKeystoreCountKeysUpToPropagatesDecodeError checks that CountKeysUpTo
// surfaces a key it cannot decode. A prefix longer than prefixBits makes
// countUpTo decode every matching datastore key to filter it, so a stored key
// with a corrupt suffix must turn into an error rather than a wrong count.
func TestKeystoreCountKeysUpToPropagatesDecodeError(t *testing.T) {
	ctx := context.Background()
	const prefixBits = 8
	d := ds.NewMapDatastore()

	// A key in the all-zero bucket whose final component is not valid base64url.
	// decodeKey base64-decodes that suffix, so it fails on this key.
	corrupt := ds.NewKey("/0/0/0/0/0/0/0/0/!not-base64")
	require.NoError(t, d.Put(ctx, corrupt, []byte("v")))

	store, err := NewKeystore(d, WithPrefixBits(prefixBits))
	require.NoError(t, err)
	defer store.Close()

	// A 9-bit prefix exceeds prefixBits, so countUpTo decodes each matching key.
	n, err := store.CountKeysUpTo(ctx, bitstr.Key(strings.Repeat("0", prefixBits+1)), 0)
	require.Error(t, err)
	require.Zero(t, n)
}

func TestKeystoreDelete(t *testing.T) {
	t.Run("Keystore", func(t *testing.T) {
		ds := ds.NewMapDatastore()
		defer ds.Close()
		store, err := NewKeystore(ds)
		require.NoError(t, err)
		defer store.Close()

		testKeystoreDeleteImpl(t, store)
	})

	t.Run("ResettableKeystore", func(t *testing.T) {
		ds := ds.NewMapDatastore()
		defer ds.Close()
		store, err := NewResettableKeystore(ds)
		require.NoError(t, err)
		defer store.Close()

		testKeystoreDeleteImpl(t, store)
	})
}

func testKeystoreDeleteImpl(t *testing.T, store Keystore) {
	mhs := random.Multihashes(3)
	for i := range mhs {
		h, err := mh.Sum([]byte{byte(i)}, mh.SHA2_256, -1)
		require.NoError(t, err)
		mhs[i] = h
	}
	_, err := store.Put(context.Background(), mhs...)
	require.NoError(t, err)

	delPrefix := bitstr.Key(key.BitString(keyspace.MhToBit256(mhs[0]))[:6])
	err = store.Delete(context.Background(), mhs[0])
	require.NoError(t, err)

	res, err := store.Get(context.Background(), delPrefix)
	require.NoError(t, err)
	for _, h := range res {
		require.NotEqual(t, string(h), string(mhs[0]), "expected deleted hash to be gone")
	}

	// other hashes should still be retrievable
	otherPrefix := bitstr.Key(key.BitString(keyspace.MhToBit256(mhs[1]))[:6])
	res, err = store.Get(context.Background(), otherPrefix)
	require.NoError(t, err)
	require.NotEmpty(t, res, "expected remaining hashes for other prefix")
}

func TestKeystoreSize(t *testing.T) {
	t.Run("Keystore", func(t *testing.T) {
		ds := ds.NewMapDatastore()
		defer ds.Close()
		store, err := NewKeystore(ds)
		require.NoError(t, err)
		defer store.Close()

		testKeystoreSizeImpl(t, store)
	})

	t.Run("ResettableKeystore", func(t *testing.T) {
		ds := ds.NewMapDatastore()
		defer ds.Close()
		store, err := NewResettableKeystore(ds)
		require.NoError(t, err)
		defer store.Close()

		testKeystoreSizeImpl(t, store)
	})
}

func testKeystoreSizeImpl(t *testing.T, store Keystore) {
	ctx := context.Background()

	mhs0 := random.Multihashes(128)
	_, err := store.Put(ctx, mhs0...)
	require.NoError(t, err)

	size, err := store.Size(ctx)
	require.NoError(t, err)
	require.Equal(t, len(mhs0), size)

	nKeys := 1 << 12
	batches := 1 << 6
	for range batches {
		mhs1 := random.Multihashes(nKeys / batches)
		_, err = store.Put(ctx, mhs1...)
		require.NoError(t, err)
	}

	size, err = store.Size(ctx)
	require.NoError(t, err)
	require.Equal(t, len(mhs0)+nKeys, size)
}

func TestKeystoreSizePersistence(t *testing.T) {
	t.Run("Keystore", func(t *testing.T) {
		d := ds.NewMapDatastore()
		defer d.Close()

		testKeystoreSizePersistenceImpl(t, d, func(datastore ds.Batching) (Keystore, error) {
			return NewKeystore(datastore)
		})
	})

	t.Run("ResettableKeystore", func(t *testing.T) {
		d := ds.NewMapDatastore()
		defer d.Close()

		testKeystoreSizePersistenceImpl(t, d, func(datastore ds.Batching) (Keystore, error) {
			return NewResettableKeystore(datastore)
		})
	})
}

func testKeystoreSizePersistenceImpl(t *testing.T, datastore ds.Batching, newStore func(ds.Batching) (Keystore, error)) {
	ctx := context.Background()

	// Create keystore, add keys, and close it
	store, err := newStore(datastore)
	require.NoError(t, err)

	const initialKeys = 100
	mhs := random.Multihashes(initialKeys)
	_, err = store.Put(ctx, mhs...)
	require.NoError(t, err)

	size, err := store.Size(ctx)
	require.NoError(t, err)
	require.Equal(t, initialKeys, size)

	// Close should persist size
	err = store.Close()
	require.NoError(t, err)

	// Reopen keystore - should load persisted size without scanning
	store2, err := newStore(datastore)
	require.NoError(t, err)
	defer store2.Close()

	// Size should be correct (loaded from persisted value)
	size2, err := store2.Size(ctx)
	require.NoError(t, err)
	require.Equal(t, initialKeys, size2, "loaded size should match persisted size")

	// Add more keys to verify it continues to work
	const additionalKeys = 50
	moreMhs := random.Multihashes(additionalKeys)
	_, err = store2.Put(ctx, moreMhs...)
	require.NoError(t, err)

	size3, err := store2.Size(ctx)
	require.NoError(t, err)
	require.Equal(t, initialKeys+additionalKeys, size3, "size should update correctly after loading")
}

func TestKeystoreSizeFallbackOnCorruption(t *testing.T) {
	keystoreTypes := []struct {
		name    string
		newFunc func(ds.Batching) (Keystore, error)
	}{
		{"Keystore", func(d ds.Batching) (Keystore, error) { return NewKeystore(d) }},
		{"ResettableKeystore", func(d ds.Batching) (Keystore, error) { return NewResettableKeystore(d) }},
	}

	subtests := []struct {
		name string
		impl func(*testing.T, ds.Batching, func(ds.Batching) (Keystore, error))
	}{
		{"Missing size key (simulates crash)", testKeystoreSizeMissingKeyImpl},
		{"Restart without crash", testKeystoreSizeCleanRestartImpl},
	}

	for _, kt := range keystoreTypes {
		t.Run(kt.name, func(t *testing.T) {
			for _, st := range subtests {
				t.Run(st.name, func(t *testing.T) {
					d := ds.NewMapDatastore()
					defer d.Close()

					st.impl(t, d, kt.newFunc)
				})
			}
		})
	}
}

func testKeystoreSizeMissingKeyImpl(t *testing.T, datastore ds.Batching, newStore func(ds.Batching) (Keystore, error)) {
	ctx := context.Background()

	// Create keystore and add keys, but don't close it (simulates crash)
	store, err := newStore(datastore)
	require.NoError(t, err)

	const numKeys = 50
	mhs := random.Multihashes(numKeys)
	_, err = store.Put(ctx, mhs...)
	require.NoError(t, err)

	// Don't call store.Close() - simulates crash
	// Note: In real crash scenario, the worker just stops without persisting

	// Open new keystore - should fall back to refreshSize since no size was persisted
	store2, err := newStore(datastore)
	require.NoError(t, err)
	defer store2.Close()

	size, err := store2.Size(ctx)
	require.NoError(t, err)
	require.Equal(t, numKeys, size, "should count existing keys when no persisted size exists")
}

func testKeystoreSizeCleanRestartImpl(t *testing.T, datastore ds.Batching, newStore func(ds.Batching) (Keystore, error)) {
	ctx := context.Background()

	// Create keystore, add keys, and properly close
	store, err := newStore(datastore)
	require.NoError(t, err)

	const numKeys = 75
	mhs := random.Multihashes(numKeys)
	_, err = store.Put(ctx, mhs...)
	require.NoError(t, err)

	err = store.Close()
	require.NoError(t, err)

	// Reopen - should use persisted size (fast startup)
	store2, err := newStore(datastore)
	require.NoError(t, err)
	defer store2.Close()

	size, err := store2.Size(ctx)
	require.NoError(t, err)
	require.Equal(t, numKeys, size, "should load persisted size on clean restart")
}

func TestKeystoreSizeWithDeleteAndEmpty(t *testing.T) {
	keystoreTypes := []struct {
		name    string
		newFunc func(ds.Batching) (Keystore, error)
	}{
		{"Keystore", func(d ds.Batching) (Keystore, error) { return NewKeystore(d) }},
		{"ResettableKeystore", func(d ds.Batching) (Keystore, error) { return NewResettableKeystore(d) }},
	}

	subtests := []struct {
		name string
		impl func(*testing.T, Keystore)
	}{
		{"Size after deletes", testKeystoreSizeAfterDeletesImpl},
		{"Size after Empty", testKeystoreSizeAfterEmptyImpl},
		{"Size with duplicate puts", testKeystoreSizeWithDuplicatePutsImpl},
	}

	for _, kt := range keystoreTypes {
		t.Run(kt.name, func(t *testing.T) {
			for _, st := range subtests {
				t.Run(st.name, func(t *testing.T) {
					d := ds.NewMapDatastore()
					defer d.Close()
					store, err := kt.newFunc(d)
					require.NoError(t, err)
					defer store.Close()

					st.impl(t, store)
				})
			}
		})
	}
}

func testKeystoreSizeAfterDeletesImpl(t *testing.T, store Keystore) {
	ctx := context.Background()

	// Add keys
	const totalKeys = 100
	mhs := random.Multihashes(totalKeys)
	_, err := store.Put(ctx, mhs...)
	require.NoError(t, err)

	size, err := store.Size(ctx)
	require.NoError(t, err)
	require.Equal(t, totalKeys, size)

	// Delete some keys
	const keysToDelete = 30
	err = store.Delete(ctx, mhs[:keysToDelete]...)
	require.NoError(t, err)

	const remainingKeys = totalKeys - keysToDelete
	size, err = store.Size(ctx)
	require.NoError(t, err)
	require.Equal(t, remainingKeys, size, "size should decrease after deletes")

	// Delete the same keys again (idempotent)
	err = store.Delete(ctx, mhs[:keysToDelete]...)
	require.NoError(t, err)

	size, err = store.Size(ctx)
	require.NoError(t, err)
	require.Equal(t, remainingKeys, size, "size should not change when deleting non-existent keys")

	// Delete remaining keys
	err = store.Delete(ctx, mhs[keysToDelete:]...)
	require.NoError(t, err)

	size, err = store.Size(ctx)
	require.NoError(t, err)
	require.Equal(t, 0, size, "size should be 0 after deleting all keys")
}

func testKeystoreSizeAfterEmptyImpl(t *testing.T, store Keystore) {
	ctx := context.Background()

	// Add keys
	const initialKeys = 200
	mhs := random.Multihashes(initialKeys)
	_, err := store.Put(ctx, mhs...)
	require.NoError(t, err)

	size, err := store.Size(ctx)
	require.NoError(t, err)
	require.Equal(t, initialKeys, size)

	// Empty the keystore
	err = store.Empty(ctx)
	require.NoError(t, err)

	size, err = store.Size(ctx)
	require.NoError(t, err)
	require.Equal(t, 0, size, "size should be 0 after Empty")

	// Verify we can add keys again
	const keysAfterEmpty = 50
	newMhs := random.Multihashes(keysAfterEmpty)
	_, err = store.Put(ctx, newMhs...)
	require.NoError(t, err)

	size, err = store.Size(ctx)
	require.NoError(t, err)
	require.Equal(t, keysAfterEmpty, size, "size should work correctly after Empty")
}

func testKeystoreSizeWithDuplicatePutsImpl(t *testing.T, store Keystore) {
	ctx := context.Background()

	const initialKeys = 50
	mhs := random.Multihashes(initialKeys)

	// First put
	added, err := store.Put(ctx, mhs...)
	require.NoError(t, err)
	require.Len(t, added, initialKeys)

	size, err := store.Size(ctx)
	require.NoError(t, err)
	require.Equal(t, initialKeys, size)

	// Second put with same keys - should not increase size
	added, err = store.Put(ctx, mhs...)
	require.NoError(t, err)
	require.Len(t, added, 0, "duplicate keys should not be added")

	size, err = store.Size(ctx)
	require.NoError(t, err)
	require.Equal(t, initialKeys, size, "size should not increase for duplicate keys")

	// Mixed put: some new, some existing
	const newKeysInMix = 30
	const existingKeysInMix = 20
	newMhs := random.Multihashes(newKeysInMix)
	mixed := append(mhs[:existingKeysInMix], newMhs...)
	added, err = store.Put(ctx, mixed...)
	require.NoError(t, err)
	require.Len(t, added, newKeysInMix, "only new keys should be added")

	const totalAfterMix = initialKeys + newKeysInMix
	size, err = store.Size(ctx)
	require.NoError(t, err)
	require.Equal(t, totalAfterMix, size, "size should only increase by new keys")
}

func TestDsKey(t *testing.T) {
	s := keystore{
		prefixBits: 8,
	}

	k := bit256.ZeroKey()
	dsk := dsKey(k, s.prefixBits)
	expectedPrefix := "/0/0/0/0/0/0/0/0/"
	require.Equal(t, expectedPrefix, dsk.String()[:len(expectedPrefix)])

	s.prefixBits = 16

	b := [bit256.KeyLen]byte{}
	for range 1024 {
		_, err := rand.Read(b[:])
		require.NoError(t, err)
		k := bit256.NewKeyFromArray(b)

		sdk := dsKey(k, s.prefixBits)
		require.Equal(t, s.prefixBits+1, strings.Count(sdk.String(), "/"))
		decoded, err := s.decodeKey(sdk.String())
		require.NoError(t, err)
		require.Equal(t, k, decoded)
	}
}
