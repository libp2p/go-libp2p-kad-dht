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
	resetChan := make(chan cid.Cid)
	resetMhs := random.Multihashes(resetKeys)
	newMhs := random.Multihashes(concurrentKeys)

	// Start reset in a goroutine first
	resetDone := make(chan error, 1)
	go func() {
		resetDone <- store.ResetCids(ctx, resetChan)
	}()

	// Send keys and perform concurrent put during reset
	go func() {
		for i, h := range resetMhs {
			resetChan <- cid.NewCidV1(cid.Raw, h)
			// After sending some keys, we know ResetCids is consuming them
			// so it's safe to call Put() - it will happen during the reset
			if i == 20 {
				_, err := store.Put(ctx, newMhs...)
				require.NoError(t, err)
			}
		}
		close(resetChan)
	}()

	// Wait for reset to complete
	err = <-resetDone
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

func TestKeystoreActiveNamespacePersistenceX(t *testing.T) {
	ds := ds.NewMapDatastore()
	defer ds.Close()

	ctx := context.Background()

	// Create initial keystore
	store, err := NewResettableKeystore(ds)
	require.NoError(t, err)

	// Add initial keys
	const initialKeys = 50
	initialMhs := random.Multihashes(initialKeys)
	_, err = store.Put(ctx, initialMhs...)
	require.NoError(t, err)

	// Verify initial size
	size, err := store.Size(ctx)
	require.NoError(t, err)
	require.Equal(t, initialKeys, size, "initial size should be %d", initialKeys)

	// Perform reset with different keys
	const resetKeys = 75
	resetMhs := random.Multihashes(resetKeys)
	resetChan := make(chan cid.Cid, resetKeys)
	for _, h := range resetMhs {
		resetChan <- cid.NewCidV1(cid.Raw, h)
	}
	close(resetChan)

	err = store.ResetCids(ctx, resetChan)
	require.NoError(t, err)

	// Verify size after reset
	size, err = store.Size(ctx)
	require.NoError(t, err)
	require.Equal(t, resetKeys, size, "size after reset should be %d", resetKeys)

	// Close the keystore
	err = store.Close()
	require.NoError(t, err)

	// Reopen keystore - it should restore from the active namespace (post-reset)
	store2, err := NewResettableKeystore(ds)
	require.NoError(t, err)
	defer store2.Close()

	// Verify size is correctly restored from the active datastore
	size2, err := store2.Size(ctx)
	require.NoError(t, err)
	require.Equal(t, resetKeys, size2, "size should persist correctly after restart (with reset)")

	// Verify that the reset keys are still present
	const prefixBits = 6
	for _, h := range resetMhs {
		prefix := bitstr.Key(key.BitString(keyspace.MhToBit256(h))[:prefixBits])
		got, err := store2.Get(ctx, prefix)
		require.NoError(t, err)
		found := false
		for _, m := range got {
			if string(m) == string(h) {
				found = true
				break
			}
		}
		require.True(t, found, "reset key %v should be present after restart", h)
	}

	// Verify that the initial keys (before reset) are NOT present
	for _, h := range initialMhs {
		prefix := bitstr.Key(key.BitString(keyspace.MhToBit256(h))[:prefixBits])
		got, err := store2.Get(ctx, prefix)
		require.NoError(t, err)
		for _, m := range got {
			require.NotEqual(t, string(m), string(h), "initial key %v should not be present after restart", h)
		}
	}
}

func TestKeystoreActiveNamespacePersistenceMultipleResets(t *testing.T) {
	ds := ds.NewMapDatastore()
	defer ds.Close()

	ctx := context.Background()

	// Create initial keystore
	store, err := NewResettableKeystore(ds)
	require.NoError(t, err)

	// Perform first reset
	const firstResetKeys = 100
	firstResetMhs := random.Multihashes(firstResetKeys)
	resetChan1 := make(chan cid.Cid, firstResetKeys)
	for _, h := range firstResetMhs {
		resetChan1 <- cid.NewCidV1(cid.Raw, h)
	}
	close(resetChan1)

	err = store.ResetCids(ctx, resetChan1)
	require.NoError(t, err)

	// Close and reopen
	err = store.Close()
	require.NoError(t, err)

	store2, err := NewResettableKeystore(ds)
	require.NoError(t, err)

	// Verify first reset keys are present
	size, err := store2.Size(ctx)
	require.NoError(t, err)
	require.Equal(t, firstResetKeys, size, "size after first reset and restart should be %d", firstResetKeys)

	// Perform second reset
	const secondResetKeys = 50
	secondResetMhs := random.Multihashes(secondResetKeys)
	resetChan2 := make(chan cid.Cid, secondResetKeys)
	for _, h := range secondResetMhs {
		resetChan2 <- cid.NewCidV1(cid.Raw, h)
	}
	close(resetChan2)

	err = store2.ResetCids(ctx, resetChan2)
	require.NoError(t, err)

	// Close and reopen again
	err = store2.Close()
	require.NoError(t, err)

	store3, err := NewResettableKeystore(ds)
	require.NoError(t, err)
	defer store3.Close()

	// Verify second reset keys are present and first reset keys are gone
	size, err = store3.Size(ctx)
	require.NoError(t, err)
	require.Equal(t, secondResetKeys, size, "size after second reset and restart should be %d", secondResetKeys)

	// Verify that the second reset keys are present
	const prefixBits = 6
	for _, h := range secondResetMhs {
		prefix := bitstr.Key(key.BitString(keyspace.MhToBit256(h))[:prefixBits])
		got, err := store3.Get(ctx, prefix)
		require.NoError(t, err)
		found := false
		for _, m := range got {
			if string(m) == string(h) {
				found = true
				break
			}
		}
		require.True(t, found, "second reset key %v should be present after restart", h)
	}

	// Verify that the first reset keys are NOT present
	for _, h := range firstResetMhs {
		prefix := bitstr.Key(key.BitString(keyspace.MhToBit256(h))[:prefixBits])
		got, err := store3.Get(ctx, prefix)
		require.NoError(t, err)
		for _, m := range got {
			require.NotEqual(t, string(m), string(h), "first reset key %v should not be present after second reset and restart", h)
		}
	}
}

// TestKeystoreCloseDuringReset tests that closing the keystore during a
// ResetCids operation does not cause a panic. This reproduces the race
// condition where the underlying datastore is closed while ResetCids is
// still running.
func TestKeystoreCloseDuringReset(t *testing.T) {
	ds := ds.NewMapDatastore()
	defer ds.Close()

	store, err := NewResettableKeystore(ds)
	require.NoError(t, err)

	ctx := context.Background()

	// Create a slow channel that will feed keys gradually
	const resetKeys = 1000
	resetChan := make(chan cid.Cid)

	// Start reset in background
	resetDone := make(chan error, 1)
	go func() {
		resetDone <- store.ResetCids(ctx, resetChan)
	}()

	// Feed some keys, then close the keystore
	go func() {
		resetMhs := random.Multihashes(resetKeys)
		for i, h := range resetMhs {
			resetChan <- cid.NewCidV1(cid.Raw, h)
			// Close the keystore partway through
			if i == 100 {
				// Give it a moment to process some keys
				go func() {
					err := store.Close()
					// Close might return an error if operations are still running,
					// but it shouldn't panic
					if err != nil {
						t.Logf("Close returned error (expected): %v", err)
					}
				}()
			}
		}
		close(resetChan)
	}()

	// Wait for reset to complete or fail
	err = <-resetDone
	// ResetCids should return ErrClosed when the keystore is closed
	if err != nil && err != ErrClosed {
		t.Logf("ResetCids returned error (may be expected during close): %v", err)
	}
	// The important thing is we didn't panic
}
