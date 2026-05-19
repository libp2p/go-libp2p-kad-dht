package keystore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	dssync "github.com/ipfs/go-datastore/sync"
	pebble "github.com/ipfs/go-ds-pebble"
	"github.com/libp2p/go-libp2p-kad-dht/provider/internal/keyspace"
	mh "github.com/multiformats/go-multihash"

	"github.com/ipfs/go-libdht/kad/key"
	"github.com/ipfs/go-libdht/kad/key/bitstr"

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

func TestKeystoreFactoryMode(t *testing.T) {
	ctx := context.Background()
	baseDir := t.TempDir()

	// Meta datastore for the active-namespace marker, owned by the caller.
	// An in-memory datastore suffices since it only stores the active
	// namespace marker and persisted size.
	metaDs := ds.NewMapDatastore()
	defer metaDs.Close()

	create := func(suffix string) (ds.Batching, error) {
		return pebble.NewDatastore(filepath.Join(baseDir, suffix), nil)
	}
	destroy := func(suffix string) error {
		return os.RemoveAll(filepath.Join(baseDir, suffix))
	}

	// --- Phase 1: create keystore, write keys, verify ---

	store, err := NewResettableKeystore(metaDs,
		WithDatastoreFactory(create, destroy),
	)
	require.NoError(t, err)

	const initialKeys = 50
	initialMhs := random.Multihashes(initialKeys)
	_, err = store.Put(ctx, initialMhs...)
	require.NoError(t, err)

	size, err := store.Size(ctx)
	require.NoError(t, err)
	require.Equal(t, initialKeys, size)

	// --- Phase 2: reset with new keys ---

	const resetKeys = 30
	resetMhs := random.Multihashes(resetKeys)
	resetChan := make(chan cid.Cid, resetKeys)
	for _, h := range resetMhs {
		resetChan <- cid.NewCidV1(cid.Raw, h)
	}
	close(resetChan)

	err = store.ResetCids(ctx, resetChan)
	require.NoError(t, err)

	size, err = store.Size(ctx)
	require.NoError(t, err)
	require.Equal(t, resetKeys, size)

	// The old active namespace directory should have been removed from disk.
	_, err = os.Stat(filepath.Join(baseDir, "0"))
	require.ErrorIs(t, err, os.ErrNotExist, "old namespace dir should be removed after reset")

	// The new active namespace directory should exist.
	_, err = os.Stat(filepath.Join(baseDir, "1"))
	require.NoError(t, err, "new namespace dir should exist")

	// Verify reset keys are accessible
	const prefixBits = 6
	for _, h := range resetMhs {
		prefix := bitstr.Key(key.BitString(keyspace.MhToBit256(h))[:prefixBits])
		got, err := store.Get(ctx, prefix)
		require.NoError(t, err)
		found := false
		for _, m := range got {
			if string(m) == string(h) {
				found = true
				break
			}
		}
		require.True(t, found, "reset key should be present")
	}

	// --- Phase 3: close and reopen — verify persistence ---

	err = store.Close()
	require.NoError(t, err)

	store2, err := NewResettableKeystore(metaDs,
		WithDatastoreFactory(create, destroy),
	)
	require.NoError(t, err)

	require.Equal(t, store2.activeNamespace, byte(1))

	size, err = store2.Size(ctx)
	require.NoError(t, err)
	require.Equal(t, resetKeys, size, "size should persist across restart")

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
		require.True(t, found, "reset key should survive restart")
	}

	// Old keys should not be present after restart
	for _, h := range initialMhs {
		prefix := bitstr.Key(key.BitString(keyspace.MhToBit256(h))[:prefixBits])
		got, err := store2.Get(ctx, prefix)
		require.NoError(t, err)
		for _, m := range got {
			require.NotEqual(t, string(m), string(h), "old key should not be present after restart")
		}
	}

	err = store2.Close()
	require.NoError(t, err)
}

func TestKeystoreFactoryModeCrashRecovery(t *testing.T) {
	ctx := context.Background()
	baseDir := t.TempDir()

	metaDs := ds.NewMapDatastore()
	defer metaDs.Close()

	create := func(suffix string) (ds.Batching, error) {
		return pebble.NewDatastore(filepath.Join(baseDir, suffix), nil)
	}
	destroy := func(suffix string) error {
		return os.RemoveAll(filepath.Join(baseDir, suffix))
	}

	// Create keystore with initial keys in namespace "0".
	store, err := NewResettableKeystore(metaDs,
		WithDatastoreFactory(create, destroy),
	)
	require.NoError(t, err)

	initialMhs := random.Multihashes(20)
	_, err = store.Put(ctx, initialMhs...)
	require.NoError(t, err)

	require.NoError(t, store.Close())

	// Simulate a crash that left a stale alt directory ("1") on disk with
	// garbage keys from an incomplete prior reset.
	staleDs, err := create("1")
	require.NoError(t, err)
	staleMhs := random.Multihashes(10)
	b, err := staleDs.Batch(ctx)
	require.NoError(t, err)
	for _, h := range staleMhs {
		k := keyspace.MhToBit256(h)
		require.NoError(t, b.Put(ctx, dsKey(k, DefaultPrefixBits), h))
	}
	require.NoError(t, b.Commit(ctx))
	require.NoError(t, staleDs.Close())

	// Reopen keystore and perform a reset. The stale directory should be
	// cleaned up before the new alt datastore is created.
	store2, err := NewResettableKeystore(metaDs,
		WithDatastoreFactory(create, destroy),
	)
	require.NoError(t, err)

	const resetKeys = 15
	freshMhs := random.Multihashes(resetKeys)
	ch := make(chan cid.Cid, resetKeys)
	for _, h := range freshMhs {
		ch <- cid.NewCidV1(cid.Raw, h)
	}
	close(ch)

	require.NoError(t, store2.ResetCids(ctx, ch))

	size, err := store2.Size(ctx)
	require.NoError(t, err)
	require.Equal(t, resetKeys, size, "only fresh keys should be present")

	// Verify none of the stale keys leaked through.
	for _, h := range staleMhs {
		prefix := bitstr.Key(key.BitString(keyspace.MhToBit256(h))[:DefaultPrefixBits])
		got, err := store2.Get(ctx, prefix)
		require.NoError(t, err)
		for _, m := range got {
			require.NotEqual(t, string(m), string(h), "stale key should not be present after reset")
		}
	}

	require.NoError(t, store2.Close())
}

// closeErrDs wraps a ds.Batching and makes Close return an error when
// the failClose flag is set.
type closeErrDs struct {
	ds.Batching
	failClose *bool
}

func (d *closeErrDs) Close() error {
	d.Batching.Close()
	if *d.failClose {
		return errors.New("injected close error")
	}
	return nil
}

func TestKeystoreFactoryModeTeardownCloseError(t *testing.T) {
	ctx := context.Background()
	baseDir := t.TempDir()

	metaDs := ds.NewMapDatastore()
	defer metaDs.Close()

	failClose := true
	create := func(suffix string) (ds.Batching, error) {
		d, err := pebble.NewDatastore(filepath.Join(baseDir, suffix), nil)
		if err != nil {
			return nil, err
		}
		return &closeErrDs{Batching: d, failClose: &failClose}, nil
	}
	destroyed := make(map[string]bool)
	destroy := func(suffix string) error {
		destroyed[suffix] = true
		return os.RemoveAll(filepath.Join(baseDir, suffix))
	}

	// Start with failClose disabled so the primary opens normally.
	failClose = false
	store, err := NewResettableKeystore(metaDs,
		WithDatastoreFactory(create, destroy),
	)
	require.NoError(t, err)

	_, err = store.Put(ctx, random.Multihashes(10)...)
	require.NoError(t, err)

	// Enable close errors. The alt datastore created during reset (and the
	// old primary torn down after the swap) will both use this flag.
	// teardownAltDs should still call destroyDs even when Close fails.
	failClose = true

	ch := make(chan cid.Cid, 5)
	for _, h := range random.Multihashes(5) {
		ch <- cid.NewCidV1(cid.Raw, h)
	}
	close(ch)

	// ResetCids does not propagate teardown errors (cleanup is best-effort
	// in the defer), but destroyDs should still have been called.
	err = store.ResetCids(ctx, ch)
	require.NoError(t, err)

	// destroyDs should still have been called for the old namespace.
	require.True(t, destroyed["0"], "destroyDs should be called even when Close fails")
	// The old namespace directory should be gone.
	_, statErr := os.Stat(filepath.Join(baseDir, "0"))
	require.ErrorIs(t, statErr, os.ErrNotExist, "old namespace dir should be removed despite close error")

	failClose = false
	require.NoError(t, store.Close())
}

func TestKeystoreCorruptedMarkerRecovery(t *testing.T) {
	ctx := context.Background()

	t.Run("shared datastore mode", func(t *testing.T) {
		metaDs := ds.NewMapDatastore()
		defer metaDs.Close()

		// Write a corrupted active namespace marker (value 42).
		require.NoError(t, metaDs.Put(ctx, ds.NewKey("active"), []byte{42}))

		store, err := NewResettableKeystore(metaDs)
		require.NoError(t, err)

		require.Equal(t, byte(0), store.activeNamespace, "corrupted marker should reset to 0")

		// Verify the keystore is fully functional after recovery.
		mhs := random.Multihashes(5)
		_, err = store.Put(ctx, mhs...)
		require.NoError(t, err)

		size, err := store.Size(ctx)
		require.NoError(t, err)
		require.Equal(t, 5, size)

		require.NoError(t, store.Close())
	})

	t.Run("factory mode", func(t *testing.T) {
		baseDir := t.TempDir()
		metaDs := ds.NewMapDatastore()
		defer metaDs.Close()

		// Create a stale datastore at "0" with leftover keys.
		staleDs, err := pebble.NewDatastore(filepath.Join(baseDir, "0"), nil)
		require.NoError(t, err)
		staleMhs := random.Multihashes(5)
		b, err := staleDs.Batch(ctx)
		require.NoError(t, err)
		for _, h := range staleMhs {
			k := keyspace.MhToBit256(h)
			require.NoError(t, b.Put(ctx, dsKey(k, DefaultPrefixBits), h))
		}
		require.NoError(t, b.Commit(ctx))
		require.NoError(t, staleDs.Close())

		// Write a corrupted marker with wrong length.
		require.NoError(t, metaDs.Put(ctx, ds.NewKey("active"), []byte{0, 1, 2}))

		create := func(suffix string) (ds.Batching, error) {
			return pebble.NewDatastore(filepath.Join(baseDir, suffix), nil)
		}
		destroy := func(suffix string) error {
			return os.RemoveAll(filepath.Join(baseDir, suffix))
		}

		store, err := NewResettableKeystore(metaDs,
			WithDatastoreFactory(create, destroy),
		)
		require.NoError(t, err)

		require.Equal(t, byte(0), store.activeNamespace, "corrupted marker should reset to 0")

		// Stale directory should have been destroyed and recreated empty.
		size, err := store.Size(ctx)
		require.NoError(t, err)
		require.Equal(t, 0, size, "stale data should be purged on corrupted marker")

		// Verify put + reset cycle works on the recovered keystore.
		freshMhs := random.Multihashes(10)
		_, err = store.Put(ctx, freshMhs...)
		require.NoError(t, err)

		resetMhs := random.Multihashes(5)
		ch := make(chan cid.Cid, len(resetMhs))
		for _, h := range resetMhs {
			ch <- cid.NewCidV1(cid.Raw, h)
		}
		close(ch)
		require.NoError(t, store.ResetCids(ctx, ch))

		size, err = store.Size(ctx)
		require.NoError(t, err)
		require.Equal(t, 5, size)

		require.NoError(t, store.Close())
	})
}

func TestKeystoreOptionAdapter(t *testing.T) {
	ctx := context.Background()
	baseDir := t.TempDir()

	metaDs := ds.NewMapDatastore()
	defer metaDs.Close()

	create := func(suffix string) (ds.Batching, error) {
		return pebble.NewDatastore(filepath.Join(baseDir, suffix), nil)
	}
	destroy := func(suffix string) error {
		return os.RemoveAll(filepath.Join(baseDir, suffix))
	}

	const customPrefixBits = 8
	store, err := NewResettableKeystore(metaDs,
		WithDatastoreFactory(create, destroy),
		KeystoreOption(WithPrefixBits(customPrefixBits)),
	)
	require.NoError(t, err)

	require.Equal(t, customPrefixBits, store.prefixBits, "KeystoreOption should pass through prefixBits")

	// Verify the keystore works with the custom prefix.
	mhs := random.Multihashes(10)
	_, err = store.Put(ctx, mhs...)
	require.NoError(t, err)

	size, err := store.Size(ctx)
	require.NoError(t, err)
	require.Equal(t, 10, size)

	// Verify keys are retrievable with the configured prefix length.
	for _, h := range mhs {
		prefix := bitstr.Key(key.BitString(keyspace.MhToBit256(h))[:customPrefixBits])
		got, err := store.Get(ctx, prefix)
		require.NoError(t, err)
		found := false
		for _, m := range got {
			if string(m) == string(h) {
				found = true
				break
			}
		}
		require.True(t, found, "key should be retrievable with custom prefix bits")
	}

	require.NoError(t, store.Close())
}

// slowSyncDatastore wraps a ds.Batching and blocks Sync() calls whose key
// path starts with slowPrefix until release is closed. The first skipN
// matching calls are allowed through unblocked — this lets the test pass
// through the Sync that prepareAltDs.empty issues during opStart, so the
// wedge lands on the Phase-A altPutBlind Sync instead. If wedged is
// non-nil, it is closed when the first call actually blocks, letting tests
// synchronise around the wedge taking effect.
type slowSyncDatastore struct {
	ds.Batching
	slowPrefix string
	skipN      int
	count      atomic.Int64
	release    <-chan struct{}
	wedged     chan struct{}
	wedgedOnce sync.Once
}

func (s *slowSyncDatastore) Sync(ctx context.Context, k ds.Key) error {
	if !strings.HasPrefix(k.String(), s.slowPrefix) {
		return s.Batching.Sync(ctx, k)
	}
	if s.count.Add(1) <= int64(s.skipN) {
		return s.Batching.Sync(ctx, k)
	}
	s.wedgedOnce.Do(func() {
		if s.wedged != nil {
			close(s.wedged)
		}
	})
	select {
	case <-s.release:
		return s.Batching.Sync(ctx, k)
	case <-ctx.Done():
		return ctx.Err()
	}
}

// TestKeystoreWorkerResponsiveWhileAltDsWedged verifies the core property of
// the altPut-decoupling design: while a reset's altDs write is blocked inside
// a slow underlying datastore, the keystore worker continues to serve Size/Put
// on the primary namespace without delay.
//
// Regression test for the production hang where a single pebble call stayed in
// flight for >1h, freezing every keystore operation. The fix moves all altDs
// I/O off the worker: worker Puts go to an in-memory buffer; ResetCids' own
// goroutine owns altDs writes. So even when altDs wedges, the worker stays
// responsive.
func TestKeystoreWorkerResponsiveWhileAltDsWedged(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		defer func() {
			select {
			case <-release:
			default:
				close(release)
			}
		}()

		base := dssync.MutexWrap(ds.NewMapDatastore())
		// Block Sync on the alternate namespace only, so primary-side worker
		// operations stay fast while ResetCids' altPutBlind wedges in altDs.Sync.
		slow := &slowSyncDatastore{Batching: base, release: release, skipN: 1}

		store, err := NewResettableKeystore(slow)
		require.NoError(t, err)
		defer store.Close()

		// Derive the alt namespace prefix from the freshly-initialised store
		// rather than hard-coding "/1". Safe to set after NewResettableKeystore
		// because the constructor performs no Sync calls itself (the loadSize
		// path only Gets/Deletes the size key).
		slow.slowPrefix = fmt.Sprintf("/%d", 1-store.activeNamespace)

		ctx := t.Context()

		// Pre-populate primary so Size has a non-zero baseline.
		const initial = 10
		_, err = store.Put(ctx, random.Multihashes(initial)...)
		require.NoError(t, err)

		// Start a reset. ResetCids' first altPutBlind reaches altDs.Sync,
		// which blocks until release is closed.
		resetChan := make(chan cid.Cid, 1)
		resetChan <- cid.NewCidV1(cid.Raw, random.Multihashes(1)[0])
		close(resetChan)
		resetDone := make(chan error, 1)
		go func() {
			resetDone <- store.ResetCids(ctx, resetChan)
		}()

		// Wait until ResetCids is parked inside altDs.Sync. After this,
		// the worker must be idle in its select — any subsequent probe
		// that hangs proves the worker was stuck behind altDs.
		synctest.Wait()

		// Probes use a short synthetic deadline so a wedged worker fails
		// cleanly with ctx.Err() rather than deadlocking the bubble. On
		// the happy path the deadline never fires — Size/Put round-trip
		// through the worker in zero synthetic time.
		expected := initial
		probe := func() {
			probeCtx, cancel := context.WithTimeout(ctx, time.Second)
			defer cancel()
			size, err := store.Size(probeCtx)
			require.NoError(t, err, "Size must return while altDs is wedged")
			require.Equal(t, expected, size)

			const probePuts = 3
			_, err = store.Put(probeCtx, random.Multihashes(probePuts)...)
			require.NoError(t, err, "Put must return while altDs is wedged")
			expected += probePuts
		}
		for range 5 {
			probe()
		}

		// Sanity check: ResetCids is still in flight (its altPut is wedged).
		select {
		case err := <-resetDone:
			t.Fatalf("ResetCids returned unexpectedly while altDs wedged: %v", err)
		default:
		}

		// Release the wedge; reset must complete cleanly.
		close(release)
		synctest.Wait()
		require.NoError(t, <-resetDone)
	})
}

// TestKeystoreResetBufferBackpressure verifies that when the worker's reset
// buffer is at capacity, additional Puts block until ResetCids drains the
// buffer — keys are never dropped silently.
func TestKeystoreResetBufferBackpressure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// Shrink the cap so we can hit it without writing millions of keys.
		const bufCap = 10

		release := make(chan struct{})
		defer func() {
			select {
			case <-release:
			default:
				close(release)
			}
		}()

		base := dssync.MutexWrap(ds.NewMapDatastore())
		slow := &slowSyncDatastore{
			Batching: base,
			release:  release,
			skipN:    1,
			wedged:   make(chan struct{}),
		}
		store, err := NewResettableKeystore(slow, WithResetBufferCapacity(bufCap))
		require.NoError(t, err)
		defer store.Close()
		slow.slowPrefix = fmt.Sprintf("/%d", 1-store.activeNamespace)

		ctx := t.Context()

		// Start a reset that wedges in Phase A's altDs.Sync so it can't drain
		// the buffer.
		resetChan := make(chan cid.Cid, 1)
		resetChan <- cid.NewCidV1(cid.Raw, random.Multihashes(1)[0])
		close(resetChan)
		resetDone := make(chan error, 1)
		go func() {
			resetDone <- store.ResetCids(ctx, resetChan)
		}()

		// Wait until ResetCids is wedged in Phase A's Sync. Without this,
		// the test's Puts could race ahead of opStart and bypass the buffer.
		synctest.Wait()
		select {
		case <-slow.wedged:
		default:
			t.Fatal("ResetCids never reached the wedged altDs.Sync")
		}

		// Fill the buffer exactly to capacity.
		_, err = store.Put(ctx, random.Multihashes(bufCap)...)
		require.NoError(t, err, "filling the buffer to capacity should succeed")

		// The next Put must block; it cannot be accepted without dropping
		// keys, which the design forbids. synctest.Wait returns once the
		// goroutine is durably parked in bufferKeys — a stronger guarantee
		// than any wall-clock delay.
		blocked := make(chan error, 1)
		go func() {
			_, err := store.Put(ctx, random.Multihashes(1)...)
			blocked <- err
		}()
		synctest.Wait()
		select {
		case err := <-blocked:
			t.Fatalf("Put should have blocked on full buffer, got err=%v", err)
		default:
		}

		// Release the reset wedge. Phase A's drainBufBlind drains the buffer,
		// the blocked Put wakes up and completes; ResetCids then progresses
		// through Phase B/C and opCleanup.
		close(release)
		synctest.Wait()
		require.NoError(t, <-blocked, "blocked Put must succeed after buffer drains")
		require.NoError(t, <-resetDone)
	})
}

// TestKeystoreResetBufferLargerThanCap verifies that a single Put larger
// than the configured reset buffer capacity completes by buffering in
// chunks across multiple drain cycles. Without the chunking fix in
// bufferKeys, the old condition len(s.buf)+len(keys) > cap stayed
// permanently true once len(keys) > cap and the worker deadlocked.
func TestKeystoreResetBufferLargerThanCap(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const bufCap = 10

		release := make(chan struct{})
		defer func() {
			select {
			case <-release:
			default:
				close(release)
			}
		}()

		base := dssync.MutexWrap(ds.NewMapDatastore())
		slow := &slowSyncDatastore{
			Batching: base,
			release:  release,
			skipN:    1,
			wedged:   make(chan struct{}),
		}
		store, err := NewResettableKeystore(slow, WithResetBufferCapacity(bufCap))
		require.NoError(t, err)
		defer store.Close()
		slow.slowPrefix = fmt.Sprintf("/%d", 1-store.activeNamespace)

		ctx := t.Context()

		resetChan := make(chan cid.Cid, 1)
		resetMh := random.Multihashes(1)[0]
		resetChan <- cid.NewCidV1(cid.Raw, resetMh)
		close(resetChan)
		resetDone := make(chan error, 1)
		go func() {
			resetDone <- store.ResetCids(ctx, resetChan)
		}()

		// Wait until ResetCids is wedged inside altDs.Sync so the buffer
		// cannot drain while we issue the large Put.
		synctest.Wait()
		select {
		case <-slow.wedged:
		default:
			t.Fatal("ResetCids never reached the wedged altDs.Sync")
		}

		// Put 2x bufCap keys in a single call. The first bufCap keys fill
		// the buffer; the remainder cannot be appended until ResetCids
		// drains.
		const factor = 2
		putN := factor * bufCap
		putMhs := random.Multihashes(putN)
		putDone := make(chan error, 1)
		go func() {
			_, err := store.Put(ctx, putMhs...)
			putDone <- err
		}()

		synctest.Wait()
		select {
		case err := <-putDone:
			t.Fatalf("Put should have blocked while buffer was full, got err=%v", err)
		default:
		}

		// Release the wedge. Phase A's final drainBuf takes the first chunk;
		// bufferKeys wakes and appends the remaining chunk.
		close(release)
		synctest.Wait()
		require.NoError(t, <-putDone, "Put must complete after enough drains")
		require.NoError(t, <-resetDone)

		// All keys (1 reset key + putN concurrent Put keys) must be present
		// in the swapped-in primary.
		size, err := store.Size(ctx)
		require.NoError(t, err)
		require.Equalf(t, putN+1, size,
			"size should reflect reset key plus concurrent Put keys (got %d)",
			size)
	})
}

// TestKeystoreResetPhaseATickerDrainsSlowKeysChan verifies that Phase A's
// periodic ticker drains the worker buffer even when keysChan stops
// delivering, so no batch-flush drain ever fires. Without the ticker the
// worker would block in bufferKeys forever on any concurrent Put larger
// than the buffer capacity, because the only remaining drains would be
// Phase A's tail drain (after keysChan closes) and Phase C's drain — at
// most enough for 3x capacity.
func TestKeystoreResetPhaseATickerDrainsSlowKeysChan(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const bufCap = 5

		base := dssync.MutexWrap(ds.NewMapDatastore())
		store, err := NewResettableKeystore(base, WithResetBufferCapacity(bufCap))
		require.NoError(t, err)
		defer func() { require.NoError(t, store.Close()) }()

		ctx := t.Context()

		resetChan := make(chan cid.Cid)
		resetDone := make(chan error, 1)
		go func() {
			resetDone <- store.ResetCids(ctx, resetChan)
		}()

		// Deliver one key so the channel send unblocks once Phase A picks it
		// up — guarantees opStart finished and resetInProgress is true before
		// the concurrent Put fires.
		syncMh := random.Multihashes(1)[0]
		resetChan <- cid.NewCidV1(cid.Raw, syncMh)

		// Concurrent Put of 5x capacity keys, more than the 3x ceiling
		// covered by Phase A's tail drain + Phase C's drain alone. Under
		// synctest the Phase A ticker fires at synthetic 100ms intervals
		// the moment all goroutines park; without the ticker the buffer
		// would never drain and the Put would block until close(resetChan).
		const factor = 5
		putN := factor * bufCap
		putMhs := random.Multihashes(putN)
		putDone := make(chan error, 1)
		go func() {
			_, err := store.Put(ctx, putMhs...)
			putDone <- err
		}()
		require.NoError(t, <-putDone, "Put must complete via ticker-driven drains")

		close(resetChan)
		require.NoError(t, <-resetDone)

		size, err := store.Size(ctx)
		require.NoError(t, err)
		require.Equalf(t, putN+1, size,
			"size should reflect sync key + concurrent Put keys (got %d)", size)
	})
}

// hasCountingDatastore counts the number of Has() calls made against keys
// whose path starts with the configured prefix. Used to assert that the
// new design's Phase A never calls Has on altDs.
type hasCountingDatastore struct {
	ds.Batching
	prefix string
	count  atomic.Int64
}

func (h *hasCountingDatastore) Has(ctx context.Context, k ds.Key) (bool, error) {
	if strings.HasPrefix(k.String(), h.prefix) {
		h.count.Add(1)
	}
	return h.Batching.Has(ctx, k)
}

// TestKeystoreResetNoHasInBulkPhase verifies that a reset without
// concurrent Puts performs zero Has() calls on altDs. This is the key
// property that prevents the pebble.Has wedge from blocking Phase A,
// which is by far the bulk of a reset's altDs work.
func TestKeystoreResetNoHasInBulkPhase(t *testing.T) {
	base := dssync.MutexWrap(ds.NewMapDatastore())
	counter := &hasCountingDatastore{Batching: base}

	store, err := NewResettableKeystore(counter)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	counter.prefix = fmt.Sprintf("/%d", 1-store.activeNamespace)

	ctx := t.Context()

	// Reset with N keys, no concurrent Puts. Phase A blind-puts all of
	// them; Phase B counts via Query (not Has); Phase C / opCleanup find
	// the buffer empty. Total Has calls on altDs should be zero.
	const n = 200
	mhs := random.Multihashes(n)
	ch := make(chan cid.Cid, n)
	for _, h := range mhs {
		ch <- cid.NewCidV1(cid.Raw, h)
	}
	close(ch)

	require.NoError(t, store.ResetCids(ctx, ch))

	require.Zero(t, counter.count.Load(),
		"Phase A bulk reset must not call Has on altDs (got %d calls)",
		counter.count.Load())

	size, err := store.Size(ctx)
	require.NoError(t, err)
	require.Equal(t, n, size, "reset must still produce the correct size")
}

// pickMhsWithBit0 returns n multihashes whose first sha256 bit equals
// targetBit. Used to construct keys whose dsKey path collides with the
// shared-mode namespace prefix (namespace "/1" + dsKey "/1/...").
func pickMhsWithBit0(t *testing.T, targetBit int, n int) []mh.Multihash {
	t.Helper()
	out := make([]mh.Multihash, 0, n)
	for tries := 0; tries < 4096 && len(out) < n; tries++ {
		h := random.Multihashes(1)[0]
		if int(keyspace.MhToBit256(h).Bit(0)) == targetBit {
			out = append(out, h)
		}
	}
	require.Lenf(t, out, n,
		"failed to find %d multihashes whose first sha256 bit is %d",
		n, targetBit)
	return out
}

// TestKeystoreResetEmptiesNamespaceCollidingKeys is a tripwire for the
// emptySharedAltDs fix. If a future caller replaces it with the obvious
// wrapped iterate-then-Delete path, the bug below returns silently and
// this test catches it.
//
// Background: in shared-datastore mode the alt slot is "/0" or "/1",
// and inside the slot each key is filed under "/<bit0>/<bit1>/...". So
// a key whose first hash bit is 1, stored in slot "/1", lives at the
// underlying path "/1/1/...".
//
// Bug: namespace.Wrap.Delete runs the key through ConvertKey, which
// only adds the namespace prefix when it isn't already an ancestor. For
// "/1/..." it isn't added, so Delete targets "/1/..." instead of
// "/1/1/...". The real key stays behind on every reset.
//
// Fix: emptySharedAltDs talks to the unwrapped datastore directly and
// deletes everything under the "/1" prefix as stored, with no rewrite.
func TestKeystoreResetEmptiesNamespaceCollidingKeys(t *testing.T) {
	base := dssync.MutexWrap(ds.NewMapDatastore())
	store, err := NewResettableKeystore(base)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	// Active namespace defaults to 0, so the first reset writes into
	// altDs = "/1". Pick multihashes whose dsKey starts with "/1/...",
	// so the underlying keys collide with the namespace prefix.
	require.Equal(t, byte(0), store.activeNamespace,
		"test assumes initial active namespace is 0")
	const n = 8
	mhs := pickMhsWithBit0(t, 1, n)

	ctx := t.Context()
	ch := make(chan cid.Cid, n)
	for _, h := range mhs {
		ch <- cid.NewCidV1(cid.Raw, h)
	}
	close(ch)
	require.NoError(t, store.ResetCids(ctx, ch))

	// After the first reset, primary is "/1" and holds n colliding keys.
	size, err := store.Size(ctx)
	require.NoError(t, err)
	require.Equal(t, n, size)

	// Second reset with an empty channel swaps "/1" back into altDs and
	// triggers teardownAltDs("/1"), which runs emptySharedAltDs. This is
	// the path under test.
	ch2 := make(chan cid.Cid)
	close(ch2)
	require.NoError(t, store.ResetCids(ctx, ch2))

	// No "/1/..." keys must remain in the underlying datastore. With the
	// old wrapped-delete path, the colliding "/1/1/..." underlying keys
	// would still be present here.
	var leftover []string
	for res, err := range ds.QueryIter(ctx, base, query.Query{Prefix: "/1", KeysOnly: true}) {
		require.NoError(t, err)
		leftover = append(leftover, res.Key)
	}
	require.Emptyf(t, leftover,
		"teardown must remove all alt-namespace keys; %d orphaned: %v",
		len(leftover), leftover)
}
