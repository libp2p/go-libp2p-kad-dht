package keystore

import (
	"context"
	"crypto/rand"
	"strings"
	"testing"

	ds "github.com/ipfs/go-datastore"
	"github.com/libp2p/go-libp2p-kad-dht/provider/internal/keyspace"
	mh "github.com/multiformats/go-multihash"

	"github.com/probe-lab/go-libdht/kad/key"
	"github.com/probe-lab/go-libdht/kad/key/bit256"
	"github.com/probe-lab/go-libdht/kad/key/bitstr"

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

	b := [32]byte{}
	for range 1024 {
		_, err := rand.Read(b[:])
		require.NoError(t, err)
		k := bit256.NewKey(b[:])

		sdk := dsKey(k, s.prefixBits)
		require.Equal(t, s.prefixBits+1, strings.Count(sdk.String(), "/"))
		decoded, err := s.decodeKey(sdk.String())
		require.NoError(t, err)
		require.Equal(t, k, decoded)
	}
}
