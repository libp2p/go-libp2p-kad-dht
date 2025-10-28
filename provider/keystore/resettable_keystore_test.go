package keystore

import (
	"context"
	"testing"

	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	"github.com/libp2p/go-libp2p-kad-dht/provider/internal/keyspace"
	mh "github.com/multiformats/go-multihash"

	"github.com/probe-lab/go-libdht/kad/key"
	"github.com/probe-lab/go-libdht/kad/key/bitstr"

	"github.com/ipfs/go-test/random"
	"github.com/stretchr/testify/require"
)

func TestKeystoreReset(t *testing.T) {
	ds := ds.NewMapDatastore()
	defer ds.Close()

	store, err := NewResettableKeystore(ds)
	require.NoError(t, err)
	defer store.Close()

	const numFirstKeys = 2
	first := make([]mh.Multihash, numFirstKeys)
	for i := range first {
		h, err := mh.Sum([]byte{byte(i)}, mh.SHA2_256, -1)
		require.NoError(t, err)
		first[i] = h
	}
	_, err = store.Put(context.Background(), first...)
	require.NoError(t, err)

	const numSecondKeys = 2
	const secondOffset = 10
	secondChan := make(chan cid.Cid, numSecondKeys)
	second := make([]mh.Multihash, numSecondKeys)
	for i := range numSecondKeys {
		h, err := mh.Sum([]byte{byte(i + secondOffset)}, mh.SHA2_256, -1)
		require.NoError(t, err)
		second[i] = h
		secondChan <- cid.NewCidV1(cid.Raw, h)
	}
	close(secondChan)

	err = store.ResetCids(context.Background(), secondChan)
	require.NoError(t, err)

	// old hashes should not be present
	const prefixBits = 6
	for _, h := range first {
		prefix := bitstr.Key(key.BitString(keyspace.MhToBit256(h))[:prefixBits])
		got, err := store.Get(context.Background(), prefix)
		require.NoError(t, err)
		for _, m := range got {
			require.NotEqual(t, string(m), string(h), "expected old hash %v to be removed", h)
		}
	}

	// new hashes should be retrievable
	for _, h := range second {
		prefix := bitstr.Key(key.BitString(keyspace.MhToBit256(h))[:prefixBits])
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

func TestKeystoreResetSize(t *testing.T) {
	ds := ds.NewMapDatastore()
	defer ds.Close()

	store, err := NewResettableKeystore(ds)
	require.NoError(t, err)
	defer store.Close()

	ctx := context.Background()

	// Add initial keys
	const initialKeys = 100
	initial := random.Multihashes(initialKeys)
	_, err = store.Put(ctx, initial...)
	require.NoError(t, err)

	size, err := store.Size(ctx)
	require.NoError(t, err)
	require.Equal(t, initialKeys, size, "initial size should be %d", initialKeys)

	// Reset with fewer keys
	const firstResetKeys = 50
	resetChan := make(chan cid.Cid, firstResetKeys)
	resetMhs := random.Multihashes(firstResetKeys)
	for _, h := range resetMhs {
		resetChan <- cid.NewCidV1(cid.Raw, h)
	}
	close(resetChan)

	err = store.ResetCids(ctx, resetChan)
	require.NoError(t, err)

	// Size should reflect reset keys only
	size, err = store.Size(ctx)
	require.NoError(t, err)
	require.Equal(t, firstResetKeys, size, "size after reset should be %d", firstResetKeys)

	// Reset with more keys
	const secondResetKeys = 200
	resetChan2 := make(chan cid.Cid, secondResetKeys)
	resetMhs2 := random.Multihashes(secondResetKeys)
	for _, h := range resetMhs2 {
		resetChan2 <- cid.NewCidV1(cid.Raw, h)
	}
	close(resetChan2)

	err = store.ResetCids(ctx, resetChan2)
	require.NoError(t, err)

	size, err = store.Size(ctx)
	require.NoError(t, err)
	require.Equal(t, secondResetKeys, size, "size after second reset should be %d", secondResetKeys)

	// Reset to empty
	const emptyResetKeys = 0
	resetChan3 := make(chan cid.Cid)
	close(resetChan3)

	err = store.ResetCids(ctx, resetChan3)
	require.NoError(t, err)

	size, err = store.Size(ctx)
	require.NoError(t, err)
	require.Equal(t, emptyResetKeys, size, "size after reset to empty should be %d", emptyResetKeys)
}

func TestKeystoreResetSizeAcrossMultipleCycles(t *testing.T) {
	ds := ds.NewMapDatastore()
	defer ds.Close()

	store, err := NewResettableKeystore(ds)
	require.NoError(t, err)
	defer store.Close()

	ctx := context.Background()

	// First reset with 100 keys
	const firstResetKeys = 100
	resetChan := make(chan cid.Cid, firstResetKeys)
	firstMhs := random.Multihashes(firstResetKeys)
	for _, h := range firstMhs {
		resetChan <- cid.NewCidV1(cid.Raw, h)
	}
	close(resetChan)

	err = store.ResetCids(ctx, resetChan)
	require.NoError(t, err)

	size, err := store.Size(ctx)
	require.NoError(t, err)
	require.Equal(t, firstResetKeys, size, "size after first reset should be %d", firstResetKeys)

	// Second reset with 50 keys, some overlapping with first reset
	const overlappingKeys = 25
	const newKeys = 25
	const secondResetKeys = overlappingKeys + newKeys
	resetChan2 := make(chan cid.Cid, secondResetKeys)
	// Use 25 from first reset + 25 new
	for i := range overlappingKeys {
		resetChan2 <- cid.NewCidV1(cid.Raw, firstMhs[i])
	}
	newMhs := random.Multihashes(newKeys)
	for _, h := range newMhs {
		resetChan2 <- cid.NewCidV1(cid.Raw, h)
	}
	close(resetChan2)

	err = store.ResetCids(ctx, resetChan2)
	require.NoError(t, err)

	// Size should be 50 (total unique keys in second reset)
	size, err = store.Size(ctx)
	require.NoError(t, err)
	require.Equal(t, secondResetKeys, size, "size after second reset should be %d, regardless of overlap", secondResetKeys)
}

func TestKeystoreResetSizeWithConcurrentPuts(t *testing.T) {
	ds := ds.NewMapDatastore()
	defer ds.Close()

	store, err := NewResettableKeystore(ds)
	require.NoError(t, err)
	defer store.Close()

	ctx := context.Background()

	// Add initial keys
	const initialKeys = 50
	initial := random.Multihashes(initialKeys)
	_, err = store.Put(ctx, initial...)
	require.NoError(t, err)

	// Start reset with a slow channel (simulates concurrent operations)
	const resetKeys = 80
	const concurrentKeys = 30
	resetChan := make(chan cid.Cid, resetKeys+concurrentKeys)
	resetMhs := random.Multihashes(resetKeys)

	// Send keys in batches to allow concurrent puts
	const resetBatchPoint = 20
	go func() {
		for i, h := range resetMhs {
			resetChan <- cid.NewCidV1(cid.Raw, h)
			// Allow some concurrent puts to happen
			if i == resetBatchPoint {
				// Add concurrent keys during reset
				concurrent := random.Multihashes(concurrentKeys)
				_, err := store.Put(ctx, concurrent...)
				require.NoError(t, err)
			}
		}
		close(resetChan)
	}()

	err = store.ResetCids(ctx, resetChan)
	require.NoError(t, err)

	// Size should include both reset keys and concurrent puts
	const expectedTotalSize = resetKeys + concurrentKeys
	size, err := store.Size(ctx)
	require.NoError(t, err)
	require.Equal(t, expectedTotalSize, size, "size should include reset keys (%d) + concurrent puts (%d)", resetKeys, concurrentKeys)
}

func TestKeystoreResetSizePersistence(t *testing.T) {
	ds := ds.NewMapDatastore()
	defer ds.Close()

	// Create and populate keystore
	store, err := NewResettableKeystore(ds)
	require.NoError(t, err)

	ctx := context.Background()

	// Add keys WITHOUT reset
	const numKeys = 75
	mhs := random.Multihashes(numKeys)
	_, err = store.Put(ctx, mhs...)
	require.NoError(t, err)

	size, err := store.Size(ctx)
	require.NoError(t, err)
	require.Equal(t, numKeys, size)

	// Close and reopen
	err = store.Close()
	require.NoError(t, err)

	store2, err := NewResettableKeystore(ds)
	require.NoError(t, err)
	defer store2.Close()

	// Size should be persisted correctly
	size2, err := store2.Size(ctx)
	require.NoError(t, err)
	require.Equal(t, numKeys, size2, "size should persist correctly after restart (without reset)")
}
