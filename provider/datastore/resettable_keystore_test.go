package datastore

import (
	"context"
	"testing"

	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	"github.com/libp2p/go-libp2p-kad-dht/provider/internal/keyspace"
	mh "github.com/multiformats/go-multihash"

	"github.com/probe-lab/go-libdht/kad/key"
	"github.com/probe-lab/go-libdht/kad/key/bitstr"

	"github.com/stretchr/testify/require"
)

func TestKeyStoreReset(t *testing.T) {
	ds := ds.NewMapDatastore()
	defer ds.Close()

	store, err := NewResettableKeyStore(ds)
	require.NoError(t, err)
	defer store.Close()

	first := make([]mh.Multihash, 2)
	for i := range first {
		h, err := mh.Sum([]byte{byte(i)}, mh.SHA2_256, -1)
		require.NoError(t, err)
		first[i] = h
	}
	_, err = store.Put(context.Background(), first...)
	require.NoError(t, err)

	secondChan := make(chan cid.Cid, 2)
	second := make([]mh.Multihash, 2)
	for i := range 2 {
		h, err := mh.Sum([]byte{byte(i + 10)}, mh.SHA2_256, -1)
		require.NoError(t, err)
		second[i] = h
		secondChan <- cid.NewCidV1(cid.Raw, h)
	}
	close(secondChan)

	err = store.ResetCids(context.Background(), secondChan)
	require.NoError(t, err)

	// old hashes should not be present
	for _, h := range first {
		prefix := bitstr.Key(key.BitString(keyspace.MhToBit256(h))[:6])
		got, err := store.Get(context.Background(), prefix)
		require.NoError(t, err)
		for _, m := range got {
			require.NotEqual(t, string(m), string(h), "expected old hash %v to be removed", h)
		}
	}

	// new hashes should be retrievable
	for _, h := range second {
		prefix := bitstr.Key(key.BitString(keyspace.MhToBit256(h))[:6])
		got, err := store.Get(context.Background(), prefix)
		require.NoError(t, err)
		found := false
		for _, m := range got {
			if string(m) == string(h) {
				found = true
				break
			}
		}
		require.True(t, found, "expected hash %v after reset", h)
	}
}
