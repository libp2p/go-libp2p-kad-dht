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
