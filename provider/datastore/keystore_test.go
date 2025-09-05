package datastore

import (
	"context"
	"crypto/rand"
	"strings"
	"testing"

	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-test/random"
	"github.com/libp2p/go-libp2p-kad-dht/provider/internal/keyspace"
	mh "github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"

	"github.com/probe-lab/go-libdht/kad/key"
	"github.com/probe-lab/go-libdht/kad/key/bit256"
	"github.com/probe-lab/go-libdht/kad/key/bitstr"
)

func TestKeyStoreStoreAndGet(t *testing.T) {
	ds := ds.NewMapDatastore()
	defer ds.Close()
	store, err := NewKeyStore(ds)
	if err != nil {
		t.Fatal(err)
	}

	mhs := make([]mh.Multihash, 6)
	for i := range mhs {
		h, err := mh.Sum([]byte{byte(i)}, mh.SHA2_256, -1)
		if err != nil {
			t.Fatal(err)
		}
		mhs[i] = h
	}

	added, err := store.Put(context.Background(), mhs...)
	if err != nil {
		t.Fatal(err)
	}
	if len(added) != len(mhs) {
		t.Fatalf("expected %d new hashes, got %d", len(mhs), len(added))
	}

	added, err = store.Put(context.Background(), mhs...)
	if err != nil {
		t.Fatal(err)
	}
	if len(added) != 0 {
		t.Fatalf("expected no new hashes on second put, got %d", len(added))
	}

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
		if !found {
			t.Fatalf("expected to find multihash %v for prefix %s", h, prefix)
		}
	}

	p := bitstr.Key(key.BitString(keyspace.MhToBit256(mhs[0]))[:3])
	res, err := store.Get(context.Background(), p)
	if err != nil {
		t.Fatal(err)
	}
	if len(res) == 0 {
		t.Fatalf("expected results for prefix %s", p)
	}

	longPrefix := bitstr.Key(key.BitString(keyspace.MhToBit256(mhs[0]))[:15])
	res, err = store.Get(context.Background(), longPrefix)
	if err != nil {
		t.Fatal(err)
	}
	for _, h := range res {
		bs := bitstr.Key(key.BitString(keyspace.MhToBit256(h)))
		if bs[:15] != longPrefix {
			t.Fatalf("returned hash does not match long prefix")
		}
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
	ctx := context.Background()
	ds := ds.NewMapDatastore()
	defer ds.Close()
	store, err := NewKeyStore(ds)
	require.NoError(t, err)

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

func TestKeyStoreReset(t *testing.T) {
	ds := ds.NewMapDatastore()
	defer ds.Close()

	store, err := NewKeyStore(ds)
	if err != nil {
		t.Fatal(err)
	}

	first := make([]mh.Multihash, 2)
	for i := range first {
		h, err := mh.Sum([]byte{byte(i)}, mh.SHA2_256, -1)
		if err != nil {
			t.Fatal(err)
		}
		first[i] = h
	}
	if _, err := store.Put(context.Background(), first...); err != nil {
		t.Fatal(err)
	}

	secondChan := make(chan cid.Cid, 2)
	second := make([]mh.Multihash, 2)
	for i := range 2 {
		h, err := mh.Sum([]byte{byte(i + 10)}, mh.SHA2_256, -1)
		if err != nil {
			t.Fatal(err)
		}
		second[i] = h
		secondChan <- cid.NewCidV1(cid.Raw, h)
	}
	close(secondChan)

	err = store.ResetCids(context.Background(), secondChan)
	if err != nil {
		t.Fatal(err)
	}

	// old hashes should not be present
	for _, h := range first {
		prefix := bitstr.Key(key.BitString(keyspace.MhToBit256(h))[:6])
		got, err := store.Get(context.Background(), prefix)
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range got {
			if string(m) == string(h) {
				t.Fatalf("expected old hash %v to be removed", h)
			}
		}
	}

	// new hashes should be retrievable
	for _, h := range second {
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
		if !found {
			t.Fatalf("expected hash %v after reset", h)
		}
	}
}

func TestKeyStoreDelete(t *testing.T) {
	ds := ds.NewMapDatastore()
	defer ds.Close()

	store, err := NewKeyStore(ds)
	if err != nil {
		t.Fatal(err)
	}

	mhs := random.Multihashes(3)
	for i := range mhs {
		h, err := mh.Sum([]byte{byte(i)}, mh.SHA2_256, -1)
		if err != nil {
			t.Fatal(err)
		}
		mhs[i] = h
	}
	if _, err := store.Put(context.Background(), mhs...); err != nil {
		t.Fatal(err)
	}

	delPrefix := bitstr.Key(key.BitString(keyspace.MhToBit256(mhs[0]))[:6])
	if err := store.Delete(context.Background(), mhs[0]); err != nil {
		t.Fatal(err)
	}

	res, err := store.Get(context.Background(), delPrefix)
	if err != nil {
		t.Fatal(err)
	}
	for _, h := range res {
		if string(h) == string(mhs[0]) {
			t.Fatalf("expected no hashes for prefix after delete")
		}
	}

	// other hashes should still be retrievable
	otherPrefix := bitstr.Key(key.BitString(keyspace.MhToBit256(mhs[1]))[:6])
	res, err = store.Get(context.Background(), otherPrefix)
	if err != nil {
		t.Fatal(err)
	}
	if len(res) == 0 {
		t.Fatalf("expected remaining hashes for other prefix")
	}
}

func TestKeyStoreSize(t *testing.T) {
	ctx := context.Background()
	ds := ds.NewMapDatastore()
	defer ds.Close()
	store, err := NewKeyStore(ds)
	require.NoError(t, err)

	mhs0 := random.Multihashes(128)
	store.Put(ctx, mhs0...)

	size, err := store.Size(ctx)
	require.NoError(t, err)
	require.Equal(t, len(mhs0), size)

	mhs1 := random.Multihashes(102400)
	store.Put(ctx, mhs1...)

	size, err = store.Size(ctx)
	require.NoError(t, err)
	require.Equal(t, len(mhs0)+len(mhs1), size)
}

func TestDsKey(t *testing.T) {
	s := KeyStore{
		base:       ds.NewKey("/base/prefix"),
		prefixBits: 8,
	}

	k := bit256.ZeroKey()
	dsk := dsKey(k, s.prefixBits, s.base)
	expectedPrefix := "/base/prefix/0/0/0/0/0/0/0/0/"
	require.Equal(t, expectedPrefix, dsk.String()[:len(expectedPrefix)])

	s.prefixBits = 16

	b := [32]byte{}
	for range 1024 {
		_, err := rand.Read(b[:])
		require.NoError(t, err)
		k := bit256.NewKey(b[:])

		sdk := dsKey(k, s.prefixBits, s.base)
		require.Equal(t, strings.Count(s.base.String(), "/")+s.prefixBits+1, strings.Count(sdk.String(), "/"))
		decoded, err := s.decodeKey(sdk.String())
		require.NoError(t, err)
		require.Equal(t, k, decoded)
	}
}
