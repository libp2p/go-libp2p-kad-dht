package queue

import (
	"context"
	"testing"

	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/ipfs/go-test/random"
	mh "github.com/multiformats/go-multihash"

	"github.com/ipfs/go-libdht/kad/key/bitstr"
	"github.com/ipfs/go-libdht/kad/trie"
	"github.com/stretchr/testify/require"

	"github.com/libp2p/go-libp2p-kad-dht/provider/internal/keyspace"
)

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

func TestProvideEnqueueSimple(t *testing.T) {
	nMultihashesPerPrefix := 1 << 4

	q := NewProvideQueue()

	// Enqueue no multihash
	q.Enqueue(bitstr.Key("1010"))
	require.Equal(t, q.Size(), 0)

	prefixes := []bitstr.Key{
		"000",
		"001",
		"010",
		"011",
		"10",
	}
	for _, prefix := range prefixes {
		mhs := genMultihashesMatchingPrefix(prefix, nMultihashesPerPrefix)
		q.Enqueue(prefix, mhs...)
	}

	// Verify prefixes are in the queue
	require.Equal(t, len(prefixes), q.queue.prefixes.Size())
	require.Equal(t, len(prefixes), q.queue.queue.Len())
	for _, prefix := range prefixes {
		require.GreaterOrEqual(t, q.queue.queue.Index(func(k bitstr.Key) bool { return k == prefix }), 0)
		ok, _ := trie.Find(q.queue.prefixes, prefix)
		require.True(t, ok)
	}
	// Verify the count of multihashes matches
	require.Equal(t, len(prefixes)*nMultihashesPerPrefix, q.Size())
	// Verify count of regions in the queue
	require.Equal(t, len(prefixes), q.NumRegions())
}

func TestProvideEnqueueOverlapping(t *testing.T) {
	nMultihashesPerPrefix := 1 << 4

	q := NewProvideQueue()

	prefixes := []bitstr.Key{
		"000",
		"0000",
	}
	for _, prefix := range prefixes {
		mhs := genMultihashesMatchingPrefix(prefix, nMultihashesPerPrefix)
		q.Enqueue(prefix, mhs...)
	}

	require.Equal(t, 1, q.queue.prefixes.Size()) // Only shortest prefix should remain
	require.Equal(t, 1, q.NumRegions())
	require.Equal(t, 1, q.queue.queue.Len())
	require.GreaterOrEqual(t, q.queue.queue.Index(func(k bitstr.Key) bool { return k == prefixes[0] }), 0) // "000" is in queue
	require.Negative(t, q.queue.queue.Index(func(k bitstr.Key) bool { return k == prefixes[1] }))          // "0000" is NOT in queue

	// Verify the count of multihashes matches
	require.Equal(t, len(prefixes)*nMultihashesPerPrefix, q.Size())

	prefixes = []bitstr.Key{
		"1111",
		"111",
	}
	for _, prefix := range prefixes {
		mhs := genMultihashesMatchingPrefix(prefix, nMultihashesPerPrefix)
		q.Enqueue(prefix, mhs...)
	}

	require.Equal(t, 2, q.queue.prefixes.Size()) // only "000" and "111" should remain
	require.Equal(t, 2, q.NumRegions())
	require.Equal(t, 2, q.queue.queue.Len())
	require.GreaterOrEqual(t, q.queue.queue.Index(func(k bitstr.Key) bool { return k == prefixes[1] }), 0) // "111" is in queue
	require.Negative(t, q.queue.queue.Index(func(k bitstr.Key) bool { return k == prefixes[0] }))          // "1111" is NOT in queue

	// Verify the count of multihashes matches
	require.Equal(t, 2*len(prefixes)*nMultihashesPerPrefix, q.Size())
}

func TestProvideDequeue(t *testing.T) {
	nMultihashesPerPrefix := 1 << 4
	q := NewProvideQueue()
	prefixes := []bitstr.Key{
		"100",
		"001",
		"010",
		"11",
		"000",
	}
	mhMap := make(map[bitstr.Key][]mh.Multihash)
	for _, prefix := range prefixes {
		mhs := genMultihashesMatchingPrefix(prefix, nMultihashesPerPrefix)
		q.Enqueue(prefix, mhs...)
		mhMap[prefix] = mhs
	}
	require.Equal(t, q.queue.prefixes.Size(), len(prefixes))
	require.Equal(t, q.queue.queue.Len(), len(prefixes))
	require.Equal(t, q.Size(), len(prefixes)*nMultihashesPerPrefix)

	for i := 0; !q.IsEmpty(); i++ {
		prefix, mhs, ok := q.Dequeue()
		require.True(t, ok)
		require.Equal(t, prefixes[i], prefix)
		require.ElementsMatch(t, mhMap[prefix], mhs)
		require.Negative(t, q.queue.queue.Index(func(k bitstr.Key) bool { return k == prefix })) // prefix not in queue anymore
		require.False(t, q.queue.prefixes.Remove(prefix))
		require.Equal(t, q.Size(), (len(prefixes)-i-1)*nMultihashesPerPrefix)
	}

	prefix, mhs, ok := q.Dequeue()
	require.False(t, ok) // Queue is empty
	require.Equal(t, bitstr.Key(""), prefix)
	require.Empty(t, mhs)
}

func TestProvideDequeueMatching(t *testing.T) {
	nMultihashesPerPrefix := 1 << 4
	q := NewProvideQueue()
	prefixes := []bitstr.Key{
		"0010",
		"100",
		"010",
		"0011",
		"11",
		"000",
	}
	mhMap := make(map[bitstr.Key][]mh.Multihash)
	for _, prefix := range prefixes {
		mhs := genMultihashesMatchingPrefix(prefix, nMultihashesPerPrefix)
		q.Enqueue(prefix, mhs...)
		mhMap[prefix] = mhs
	}
	require.Equal(t, q.queue.prefixes.Size(), len(prefixes))
	require.Equal(t, q.queue.queue.Len(), len(prefixes))
	require.Equal(t, q.Size(), len(prefixes)*nMultihashesPerPrefix)

	// Prefix not in queue.
	mhs := q.DequeueMatching(bitstr.Key("101"))
	require.Empty(t, mhs)

	mhs = q.DequeueMatching(bitstr.Key("010"))
	require.ElementsMatch(t, mhMap[bitstr.Key("010")], mhs)
	require.Equal(t, 5, q.queue.queue.Len())
	require.Equal(t, 5, q.queue.prefixes.Size())
	// Verify queue order didn't change
	require.Equal(t, 0, q.queue.queue.Index(func(k bitstr.Key) bool { return k == bitstr.Key("0010") }))
	require.Equal(t, 1, q.queue.queue.Index(func(k bitstr.Key) bool { return k == bitstr.Key("100") }))
	require.Equal(t, 2, q.queue.queue.Index(func(k bitstr.Key) bool { return k == bitstr.Key("0011") }))
	require.Equal(t, 3, q.queue.queue.Index(func(k bitstr.Key) bool { return k == bitstr.Key("11") }))
	require.Equal(t, 4, q.queue.queue.Index(func(k bitstr.Key) bool { return k == bitstr.Key("000") }))

	mhs = q.DequeueMatching(bitstr.Key("001"))
	require.ElementsMatch(t, append(mhMap[bitstr.Key("0010")], mhMap[bitstr.Key("0011")]...), mhs)
	require.Equal(t, 3, q.queue.queue.Len())
	require.Equal(t, 3, q.queue.prefixes.Size())
	// Verify queue order didn't change
	require.Equal(t, 0, q.queue.queue.Index(func(k bitstr.Key) bool { return k == bitstr.Key("100") }))
	require.Equal(t, 1, q.queue.queue.Index(func(k bitstr.Key) bool { return k == bitstr.Key("11") }))
	require.Equal(t, 2, q.queue.queue.Index(func(k bitstr.Key) bool { return k == bitstr.Key("000") }))

	// Prefix not in queue.
	mhs = q.DequeueMatching(bitstr.Key("011"))
	require.Empty(t, mhs)

	// Partial prefix
	mhs0 := q.DequeueMatching(bitstr.Key("110"))
	if len(mhs0) > 0 {
		require.Equal(t, 3, q.queue.queue.Len())
		require.Equal(t, 3, q.queue.prefixes.Size())
		require.Equal(t, 0, q.queue.queue.Index(func(k bitstr.Key) bool { return k == bitstr.Key("100") }))
		require.Equal(t, 1, q.queue.queue.Index(func(k bitstr.Key) bool { return k == bitstr.Key("11") }))
		require.Equal(t, 2, q.queue.queue.Index(func(k bitstr.Key) bool { return k == bitstr.Key("000") }))
	}
	mhs1 := q.DequeueMatching(bitstr.Key("111"))
	require.Equal(t, 2, q.queue.queue.Len())
	require.Equal(t, 2, q.queue.prefixes.Size())
	require.Equal(t, 0, q.queue.queue.Index(func(k bitstr.Key) bool { return k == bitstr.Key("100") }))
	require.Equal(t, 1, q.queue.queue.Index(func(k bitstr.Key) bool { return k == bitstr.Key("000") }))
	require.ElementsMatch(t, append(mhs0, mhs1...), mhMap[bitstr.Key("11")])

	prefix, mhs, ok := q.Dequeue()
	require.True(t, ok)
	require.Equal(t, bitstr.Key("100"), prefix)
	require.ElementsMatch(t, mhMap[bitstr.Key("100")], mhs)

	mhs = q.DequeueMatching(bitstr.Key("000"))
	require.ElementsMatch(t, mhMap[bitstr.Key("000")], mhs)

	require.Equal(t, 0, q.queue.queue.Len())
	require.True(t, q.IsEmpty())

	mhs = q.DequeueMatching(bitstr.Key("000"))
	require.Empty(t, mhs)
}

func TestProvideRemove(t *testing.T) {
	nMultihashesPerPrefix := 1 << 2
	q := NewProvideQueue()
	prefixes := []bitstr.Key{
		"0010",
		"100",
		"010",
	}
	mhMap := make(map[bitstr.Key][]mh.Multihash)
	for _, prefix := range prefixes {
		mhs := genMultihashesMatchingPrefix(prefix, nMultihashesPerPrefix)
		q.Enqueue(prefix, mhs...)
		mhMap[prefix] = mhs
	}
	require.Equal(t, len(prefixes), q.queue.prefixes.Size())
	require.Equal(t, len(prefixes), q.queue.queue.Len())
	require.Equal(t, len(prefixes)*nMultihashesPerPrefix, q.Size())

	q.Remove(mhMap[bitstr.Key("0010")][:2]...)
	require.Equal(t, len(prefixes)*nMultihashesPerPrefix-2, q.Size())
	require.Equal(t, q.queue.queue.At(0), bitstr.Key("0010"))

	q.Remove(mhMap[bitstr.Key("100")]...)
	require.Equal(t, len(prefixes)*nMultihashesPerPrefix-6, q.Size())
	require.Equal(t, q.queue.queue.At(1), bitstr.Key("010"))

	q.Remove(mhMap[bitstr.Key("0010")][2])
	require.Equal(t, len(prefixes)*nMultihashesPerPrefix-7, q.Size())
	require.Equal(t, q.queue.queue.At(0), bitstr.Key("0010"))

	q.Remove(append([]mh.Multihash{mhMap[bitstr.Key("0010")][3]}, mhMap[bitstr.Key("010")][1:3]...)...)
	require.Equal(t, 2, q.Size())
	require.Equal(t, q.queue.queue.At(0), bitstr.Key("010"))
}

func TestProvideClearQueue(t *testing.T) {
	nMultihashesPerPrefix := 1 << 4
	q := NewProvideQueue()
	require.True(t, q.IsEmpty())
	prefixes := []bitstr.Key{
		"000",
		"001",
		"010",
		"011",
		"10",
	}
	for _, prefix := range prefixes {
		mhs := genMultihashesMatchingPrefix(prefix, nMultihashesPerPrefix)
		q.Enqueue(prefix, mhs...)
	}

	require.False(t, q.IsEmpty())
	require.Equal(t, q.queue.prefixes.Size(), len(prefixes))
	require.Equal(t, q.queue.queue.Len(), len(prefixes))
	require.Equal(t, q.Size(), len(prefixes)*nMultihashesPerPrefix)

	cleared := q.Clear()
	require.Equal(t, len(prefixes)*nMultihashesPerPrefix, cleared)
	require.True(t, q.IsEmpty())

	require.True(t, q.keys.IsEmptyLeaf())
	require.True(t, q.queue.prefixes.IsEmptyLeaf())
	require.Equal(t, 0, q.queue.queue.Len())
}

func TestProvidePersistAndDrainDatastore(t *testing.T) {
	ctx := context.Background()
	nMultihashesPerPrefix := 1 << 3

	// Create and populate a queue
	q1 := NewProvideQueue()
	prefixes := []bitstr.Key{
		"000",
		"001",
		"010",
		"011",
		"10",
	}

	mhMap := make(map[bitstr.Key][]mh.Multihash)
	for _, prefix := range prefixes {
		mhs := genMultihashesMatchingPrefix(prefix, nMultihashesPerPrefix)
		q1.Enqueue(prefix, mhs...)
		mhMap[prefix] = mhs
	}

	require.Equal(t, len(prefixes), q1.NumRegions())
	require.Equal(t, len(prefixes)*nMultihashesPerPrefix, q1.Size())

	// Create a datastore
	store := dssync.MutexWrap(ds.NewMapDatastore())

	// Persist the queue
	err := q1.Persist(ctx, store, 100)
	require.NoError(t, err)

	// Verify original queue is unchanged
	require.Equal(t, len(prefixes), q1.NumRegions())
	require.Equal(t, len(prefixes)*nMultihashesPerPrefix, q1.Size())

	// Create a new queue and load from datastore
	q2 := NewProvideQueue()
	err = q2.DrainDatastore(ctx, store)
	require.NoError(t, err)

	// Verify loaded queue matches original
	require.Equal(t, q1.Size(), q2.Size())
	require.Equal(t, q1.NumRegions(), q2.NumRegions())

	// Verify prefix order is preserved
	for i := range prefixes {
		require.Equal(t, q1.queue.queue.At(i), q2.queue.queue.At(i))
	}

	// Verify all keys are present by dequeuing and comparing
	for range prefixes {
		prefix1, mhs1, ok1 := q1.Dequeue()
		prefix2, mhs2, ok2 := q2.Dequeue()

		require.True(t, ok1)
		require.True(t, ok2)
		require.Equal(t, prefix1, prefix2)
		require.ElementsMatch(t, mhs1, mhs2)
	}

	require.True(t, q1.IsEmpty())
	require.True(t, q2.IsEmpty())

	// Verify datastore is empty after DrainDatastore
	results, err := store.Query(ctx, query.Query{KeysOnly: true})
	require.NoError(t, err)
	count := 0
	for range results.Next() {
		count++
	}
	require.Equal(t, 0, count)
}

func TestProvidePersistEmptyQueue(t *testing.T) {
	ctx := context.Background()

	// Create empty queue
	q := NewProvideQueue()
	require.True(t, q.IsEmpty())

	// Create a datastore
	store := dssync.MutexWrap(ds.NewMapDatastore())

	// Persist empty queue
	err := q.Persist(ctx, store, 100)
	require.NoError(t, err)

	// Load into new queue
	q2 := NewProvideQueue()
	err = q2.DrainDatastore(ctx, store)
	require.NoError(t, err)

	require.True(t, q2.IsEmpty())
}

func TestProvideDrainDatastoreFromEmptyDatastore(t *testing.T) {
	ctx := context.Background()

	// Create a datastore with no persisted data
	store := dssync.MutexWrap(ds.NewMapDatastore())

	// Load into queue
	q := NewProvideQueue()
	err := q.DrainDatastore(ctx, store)
	require.NoError(t, err)

	require.True(t, q.IsEmpty())
}

func TestProvideDrainDatastoreIsAdditive(t *testing.T) {
	ctx := context.Background()
	nMultihashesPerPrefix := 1 << 3

	// Create and populate first queue
	q1 := NewProvideQueue()
	prefixes1 := []bitstr.Key{"000", "001"}
	for _, prefix := range prefixes1 {
		mhs := genMultihashesMatchingPrefix(prefix, nMultihashesPerPrefix)
		q1.Enqueue(prefix, mhs...)
	}

	// Create a datastore
	store := dssync.MutexWrap(ds.NewMapDatastore())

	// Persist first queue
	err := q1.Persist(ctx, store, 100)
	require.NoError(t, err)

	// Create and populate second queue with different prefixes
	q2 := NewProvideQueue()
	prefixes2 := []bitstr.Key{"10", "11"}
	for _, prefix := range prefixes2 {
		mhs := genMultihashesMatchingPrefix(prefix, nMultihashesPerPrefix)
		q2.Enqueue(prefix, mhs...)
	}

	require.Equal(t, len(prefixes2), q2.NumRegions())
	initialSize := q2.Size()

	// Drain from datastore (should ADD to existing queue, not replace)
	err = q2.DrainDatastore(ctx, store)
	require.NoError(t, err)

	// Verify q2 now contains BOTH sets of prefixes
	require.Equal(t, initialSize+q1.Size(), q2.Size())
	require.Equal(t, len(prefixes1)+len(prefixes2), q2.NumRegions())

	// Verify datastore is empty after DrainDatastore
	results, err := store.Query(ctx, query.Query{KeysOnly: true})
	require.NoError(t, err)
	count := 0
	for range results.Next() {
		count++
	}
	require.Equal(t, 0, count)
}

func TestProvideDrainDatastoreRemovesPersisted(t *testing.T) {
	ctx := context.Background()
	nMultihashesPerPrefix := 1 << 3

	// Create and populate a queue
	q := NewProvideQueue()
	prefixes := []bitstr.Key{"000", "001", "010"}
	for _, prefix := range prefixes {
		mhs := genMultihashesMatchingPrefix(prefix, nMultihashesPerPrefix)
		q.Enqueue(prefix, mhs...)
	}

	// Create a datastore
	store := dssync.MutexWrap(ds.NewMapDatastore())

	// Persist the queue
	err := q.Persist(ctx, store, 100)
	require.NoError(t, err)

	// Verify data is in datastore
	results, err := store.Query(ctx, query.Query{KeysOnly: true})
	require.NoError(t, err)
	count := 0
	for range results.Next() {
		count++
	}
	require.Greater(t, count, 0)

	// Load and clear
	q2 := NewProvideQueue()
	err = q2.DrainDatastore(ctx, store)
	require.NoError(t, err)

	// Verify data is removed from datastore
	results, err = store.Query(ctx, query.Query{KeysOnly: true})
	require.NoError(t, err)
	count = 0
	for range results.Next() {
		count++
	}
	require.Equal(t, 0, count)

	// Verify loaded queue matches original
	require.Equal(t, q.Size(), q2.Size())
	require.Equal(t, q.NumRegions(), q2.NumRegions())

	// Loading from cleared datastore should result in empty queue
	q3 := NewProvideQueue()
	err = q3.DrainDatastore(ctx, store)
	require.NoError(t, err)
	require.True(t, q3.IsEmpty())
}

func TestProvidePersistWithBatching(t *testing.T) {
	ctx := context.Background()
	nMultihashesPerPrefix := 1 << 3

	// Create and populate a queue with enough prefixes to trigger batch commits
	q1 := NewProvideQueue()
	prefixes := []bitstr.Key{
		"000",
		"001",
		"010",
		"011",
		"100",
		"101",
		"110",
	}

	mhMap := make(map[bitstr.Key][]mh.Multihash)
	for _, prefix := range prefixes {
		mhs := genMultihashesMatchingPrefix(prefix, nMultihashesPerPrefix)
		q1.Enqueue(prefix, mhs...)
		mhMap[prefix] = mhs
	}

	require.Equal(t, len(prefixes), q1.NumRegions())
	require.Equal(t, len(prefixes)*nMultihashesPerPrefix, q1.Size())

	// Create a datastore
	store := dssync.MutexWrap(ds.NewMapDatastore())

	// Persist with small batch size to trigger mid-loop batch commits
	// With 7 prefixes and batchSize=3, we'll commit at i=3 and i=6, plus final commit
	err := q1.Persist(ctx, store, 3)
	require.NoError(t, err)

	// Verify original queue is unchanged
	require.Equal(t, len(prefixes), q1.NumRegions())
	require.Equal(t, len(prefixes)*nMultihashesPerPrefix, q1.Size())

	// Create a new queue and load from datastore
	q2 := NewProvideQueue()
	err = q2.DrainDatastore(ctx, store)
	require.NoError(t, err)

	// Verify loaded queue matches original
	require.Equal(t, q1.Size(), q2.Size())
	require.Equal(t, q1.NumRegions(), q2.NumRegions())

	// Verify prefix order is preserved
	for i := range prefixes {
		require.Equal(t, q1.queue.queue.At(i), q2.queue.queue.At(i))
	}

	// Verify all keys are present by dequeuing and comparing
	for range prefixes {
		prefix1, mhs1, ok1 := q1.Dequeue()
		prefix2, mhs2, ok2 := q2.Dequeue()

		require.True(t, ok1)
		require.True(t, ok2)
		require.Equal(t, prefix1, prefix2)
		require.ElementsMatch(t, mhs1, mhs2)
	}

	require.True(t, q1.IsEmpty())
	require.True(t, q2.IsEmpty())

	// Verify datastore is empty after DrainDatastore
	results, err := store.Query(ctx, query.Query{KeysOnly: true})
	require.NoError(t, err)
	count := 0
	for range results.Next() {
		count++
	}
	require.Equal(t, 0, count)
}

func TestProvidePersistLoadWithOverlappingPrefixes(t *testing.T) {
	ctx := context.Background()
	nMultihashesPerPrefix := 1 << 3

	// Create queue with overlapping prefixes that get consolidated
	q1 := NewProvideQueue()
	prefixes := []bitstr.Key{
		"000",
		"0000", // Should be consolidated into "000"
	}

	for _, prefix := range prefixes {
		mhs := genMultihashesMatchingPrefix(prefix, nMultihashesPerPrefix)
		q1.Enqueue(prefix, mhs...)
	}

	// After consolidation, should have 1 region
	require.Equal(t, 1, q1.NumRegions())
	require.Equal(t, len(prefixes)*nMultihashesPerPrefix, q1.Size())

	// Create a datastore
	store := dssync.MutexWrap(ds.NewMapDatastore())

	// Persist the queue
	err := q1.Persist(ctx, store, 100)
	require.NoError(t, err)

	// Load into new queue
	q2 := NewProvideQueue()
	err = q2.DrainDatastore(ctx, store)
	require.NoError(t, err)

	// Verify loaded queue matches consolidated state
	require.Equal(t, 1, q2.NumRegions())
	require.Equal(t, len(prefixes)*nMultihashesPerPrefix, q2.Size())
}
