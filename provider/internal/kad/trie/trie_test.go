package trie

import (
	"math/rand"
	"testing"

	"github.com/libp2p/go-libp2p-kad-dht/provider/internal/kad"
	"github.com/libp2p/go-libp2p-kad-dht/provider/internal/kad/kadtest"
	"github.com/libp2p/go-libp2p-kad-dht/provider/internal/kad/key"

	"github.com/stretchr/testify/require"
)

func trieFromKeys[K kad.Key[K], D any](kks []K) (*Trie[K, D], error) {
	t := New[K, D]()
	var err error
	for _, kk := range kks {
		var v D
		t, err = Add(t, kk, v)
		if err != nil {
			return nil, err
		}
	}
	return t, nil
}

func TestRepeatedAddRemove(t *testing.T) {
	r := New[kadtest.Key32, int]()
	testSeq(t, r)
	testSeq(t, r)
}

func testSeq(t *testing.T, tr *Trie[kadtest.Key32, int]) {
	t.Helper()
	var err error
	for _, s := range testInsertSeq {
		var v int
		tr, err = Add(tr, s.key, v)
		if err != nil {
			t.Fatalf("unexpected error during add: %v", err)
		}
		found, depth := Locate(tr, s.key)
		if !found {
			t.Fatalf("key not found: %v", s.key)
		}
		if depth != s.insertedDepth {
			t.Errorf("inserting expected depth %d, got %d", s.insertedDepth, depth)
		}
		if d := CheckInvariant(tr); d != nil {
			t.Fatalf("trie invariant discrepancy: %v", d)
		}
	}
	for _, s := range testRemoveSeq {
		tr, err = Remove(tr, s.key)
		if err != nil {
			t.Fatalf("unexpected error during remove: %v", err)
		}
		found, _ := Locate(tr, s.key)
		if found {
			t.Fatalf("key unexpectedly found: %v", s.key)
		}
		if d := CheckInvariant(tr); d != nil {
			t.Fatalf("trie invariant discrepancy: %v", d)
		}
	}
}

func TestCopy(t *testing.T) {
	tr, err := trieFromKeys[kadtest.Key32, int](sampleKeySet.Keys)
	if err != nil {
		t.Fatalf("unexpected error during from keys: %v", err)
	}
	copy := tr.Copy()
	if d := CheckInvariant(copy); d != nil {
		t.Fatalf("trie invariant discrepancy: %v", d)
	}
	if tr == copy {
		t.Errorf("Expected trie copy not to be the same reference as original")
	}
	if !Equal(tr, copy) {
		t.Errorf("Expected tries to be equal, original: %v\n, copy: %v\n", tr, copy)
	}
}

var testInsertSeq = []struct {
	key           kadtest.Key32
	insertedDepth int
}{
	{key: kadtest.Key32(0x00000000), insertedDepth: 0},
	{key: kadtest.Key32(0x80000000), insertedDepth: 1},
	{key: kadtest.Key32(0x10000000), insertedDepth: 4},
	{key: kadtest.Key32(0xc0000000), insertedDepth: 2},
	{key: kadtest.Key32(0x20000000), insertedDepth: 3},
}

var testRemoveSeq = []struct {
	key          kadtest.Key32
	reachedDepth int
}{
	{key: kadtest.Key32(0x00000000), reachedDepth: 4},
	{key: kadtest.Key32(0x10000000), reachedDepth: 3},
	{key: kadtest.Key32(0x20000000), reachedDepth: 1},
	{key: kadtest.Key32(0x80000000), reachedDepth: 2},
	{key: kadtest.Key32(0xc0000000), reachedDepth: 0},
}

func TestAddIsOrderIndependent(t *testing.T) {
	for _, s := range newKeySetList(100) {
		base := New[kadtest.Key32, any]()
		for _, k := range s.Keys {
			base.Add(k, nil)
		}
		if d := CheckInvariant(base); d != nil {
			t.Fatalf("base trie invariant discrepancy: %v", d)
		}
		for range 100 {
			perm := rand.Perm(len(s.Keys))
			reordered := New[kadtest.Key32, any]()
			for i := range s.Keys {
				reordered.Add(s.Keys[perm[i]], nil)
			}
			if d := CheckInvariant(reordered); d != nil {
				t.Fatalf("reordered trie invariant discrepancy: %v", d)
			}
			if !Equal(base, reordered) {
				t.Errorf("trie %v differs from trie %v", base, reordered)
			}
		}
	}
}

func TestImmutableAddIsOrderIndependent(t *testing.T) {
	for _, s := range newKeySetList(100) {
		base := New[kadtest.Key32, any]()
		for _, k := range s.Keys {
			base, _ = Add(base, k, nil)
		}
		if d := CheckInvariant(base); d != nil {
			t.Fatalf("base trie invariant discrepancy: %v", d)
		}
		for range 100 {
			perm := rand.Perm(len(s.Keys))
			reordered := New[kadtest.Key32, any]()
			for i := range s.Keys {
				reordered, _ = Add(reordered, s.Keys[perm[i]], nil)
			}
			if d := CheckInvariant(reordered); d != nil {
				t.Fatalf("reordered trie invariant discrepancy: %v", d)
			}
			if !Equal(base, reordered) {
				t.Errorf("trie %v differs from trie %v", base, reordered)
			}
		}
	}
}

func TestSize(t *testing.T) {
	tr := New[kadtest.Key32, any]()
	require.Equal(t, 0, tr.Size())

	var err error
	for _, kk := range sampleKeySet.Keys {
		tr, err = Add(tr, kk, nil)
		require.NoError(t, err)
	}

	require.Equal(t, len(sampleKeySet.Keys), tr.Size())
}

func TestImmutableAddReturnsNewTrie(t *testing.T) {
	tr := New[kadtest.Key32, any]()
	for _, kk := range sampleKeySet.Keys {
		var err error
		trBefore := *tr // take a copy of tr before Add is called
		trNext, err := Add(tr, kk, nil)
		require.NoError(t, err)
		// a new trie must be returned
		require.NotSame(t, tr, trNext)
		// old trie must be not be modified
		require.EqualValues(t, trBefore, *tr)
		tr = trNext
	}
	require.Equal(t, len(sampleKeySet.Keys), tr.Size())
}

func TestAddIgnoresDuplicates(t *testing.T) {
	tr := New[kadtest.Key32, any]()
	for _, kk := range sampleKeySet.Keys {
		added := tr.Add(kk, nil)
		require.True(t, added)
	}
	require.Equal(t, len(sampleKeySet.Keys), tr.Size())

	for _, kk := range sampleKeySet.Keys {
		added := tr.Add(kk, nil)
		require.False(t, added)
	}
	require.Equal(t, len(sampleKeySet.Keys), tr.Size())

	if d := CheckInvariant(tr); d != nil {
		t.Fatalf("reordered trie invariant discrepancy: %v", d)
	}
}

func TestAddManyEmpty(t *testing.T) {
	tr := New[kadtest.Key32, int]()

	entries := []Entry[kadtest.Key32, int]{
		{Key: kadtest.Key32(0x10000000), Data: 1},
		{Key: kadtest.Key32(0x20000000), Data: 2},
		{Key: kadtest.Key32(0x30000000), Data: 3},
	}

	added := tr.AddMany(entries...)
	require.Equal(t, 3, added)
	require.Equal(t, 3, tr.Size())

	// Verify all keys are findable
	for _, e := range entries {
		found, data := Find(tr, e.Key)
		require.True(t, found)
		require.Equal(t, e.Data, data)
	}

	if d := CheckInvariant(tr); d != nil {
		t.Fatalf("trie invariant discrepancy: %v", d)
	}
}

func TestAddManySingle(t *testing.T) {
	tr := New[kadtest.Key32, int]()

	entries := []Entry[kadtest.Key32, int]{
		{Key: kadtest.Key32(0x10000000), Data: 42},
	}

	added := tr.AddMany(entries...)
	require.Equal(t, 1, added)
	require.Equal(t, 1, tr.Size())

	found, data := Find(tr, entries[0].Key)
	require.True(t, found)
	require.Equal(t, 42, data)

	if d := CheckInvariant(tr); d != nil {
		t.Fatalf("trie invariant discrepancy: %v", d)
	}
}

func TestAddManyWithDuplicatesInNewEntries(t *testing.T) {
	tr := New[kadtest.Key32, int]()

	// Multiple entries with the same key
	entries := []Entry[kadtest.Key32, int]{
		{Key: kadtest.Key32(0x10000000), Data: 1},
		{Key: kadtest.Key32(0x20000000), Data: 2},
		{Key: kadtest.Key32(0x10000000), Data: 100}, // duplicate
		{Key: kadtest.Key32(0x30000000), Data: 3},
		{Key: kadtest.Key32(0x20000000), Data: 200}, // duplicate
	}

	added := tr.AddMany(entries...)

	// Should add unique keys only
	require.Equal(t, 3, added)
	require.Equal(t, 3, tr.Size())

	// First occurrence should win (based on lazy dedup logic)
	found, data := Find(tr, kadtest.Key32(0x10000000))
	require.True(t, found)
	require.Equal(t, 1, data)

	found, data = Find(tr, kadtest.Key32(0x20000000))
	require.True(t, found)
	require.Equal(t, 2, data)

	if d := CheckInvariant(tr); d != nil {
		t.Fatalf("trie invariant discrepancy: %v", d)
	}
}

func TestAddManyWithKeysAlreadyInTrie(t *testing.T) {
	tr := New[kadtest.Key32, int]()

	// Pre-populate trie
	existing := []Entry[kadtest.Key32, int]{
		{Key: kadtest.Key32(0x10000000), Data: 100},
		{Key: kadtest.Key32(0x20000000), Data: 200},
	}
	for _, e := range existing {
		tr.Add(e.Key, e.Data)
	}
	require.Equal(t, 2, tr.Size())

	// Try to add mix of new and existing keys
	entries := []Entry[kadtest.Key32, int]{
		{Key: kadtest.Key32(0x10000000), Data: 1}, // already exists
		{Key: kadtest.Key32(0x30000000), Data: 3}, // new
		{Key: kadtest.Key32(0x20000000), Data: 2}, // already exists
		{Key: kadtest.Key32(0x40000000), Data: 4}, // new
		{Key: kadtest.Key32(0x20000000), Data: 5}, // already exists
		{Key: kadtest.Key32(0x20000000), Data: 6}, // already exists
	}

	added := tr.AddMany(entries...)

	// Should only add the 2 new keys
	require.Equal(t, 2, added)
	require.Equal(t, 4, tr.Size())

	// Existing keys should keep their original data
	found, data := Find(tr, kadtest.Key32(0x10000000))
	require.True(t, found)
	require.Equal(t, 100, data) // original data preserved

	found, data = Find(tr, kadtest.Key32(0x20000000))
	require.True(t, found)
	require.Equal(t, 200, data) // original data preserved

	// New keys should be added
	found, data = Find(tr, kadtest.Key32(0x30000000))
	require.True(t, found)
	require.Equal(t, 3, data)

	found, data = Find(tr, kadtest.Key32(0x40000000))
	require.True(t, found)
	require.Equal(t, 4, data)

	if d := CheckInvariant(tr); d != nil {
		t.Fatalf("trie invariant discrepancy: %v", d)
	}
}

func TestAddManyOnlyDuplicates(t *testing.T) {
	tr := New[kadtest.Key32, int]()

	key := kadtest.Key32(0x10000000)
	tr.Add(key, 42)
	require.Equal(t, 1, tr.Size())

	// Try to add only the existing key
	entries := []Entry[kadtest.Key32, int]{
		{Key: key, Data: 100},
	}

	added := tr.AddMany(entries...)
	require.Equal(t, 0, added)
	require.Equal(t, 1, tr.Size())

	// Data should be unchanged
	found, data := Find(tr, key)
	require.True(t, found)
	require.Equal(t, 42, data)

	if d := CheckInvariant(tr); d != nil {
		t.Fatalf("trie invariant discrepancy: %v", d)
	}
}

func TestAddManyIsOrderIndependent(t *testing.T) {
	for _, s := range newKeySetList(10) {
		entries := make([]Entry[kadtest.Key32, int], len(s.Keys))
		for i, k := range s.Keys {
			entries[i] = Entry[kadtest.Key32, int]{Key: k, Data: i}
		}

		base := New[kadtest.Key32, int]()
		base.AddMany(entries...)
		if d := CheckInvariant(base); d != nil {
			t.Fatalf("base trie invariant discrepancy: %v", d)
		}

		for range 10 {
			perm := rand.Perm(len(entries))
			reorderedEntries := make([]Entry[kadtest.Key32, int], len(entries))
			for i := range entries {
				reorderedEntries[i] = entries[perm[i]]
			}

			reordered := New[kadtest.Key32, int]()
			reordered.AddMany(reorderedEntries...)
			if d := CheckInvariant(reordered); d != nil {
				t.Fatalf("reordered trie invariant discrepancy: %v", d)
			}

			// Note: Equal doesn't check data, so we verify size and lookups
			require.Equal(t, base.Size(), reordered.Size())
			for _, k := range s.Keys {
				foundBase, _ := Find(base, k)
				foundReordered, _ := Find(reordered, k)
				require.Equal(t, foundBase, foundReordered)
			}
		}
	}
}

func TestAddManyLarge(t *testing.T) {
	tr := New[kadtest.Key32, int]()

	n := 100
	entries := make([]Entry[kadtest.Key32, int], 0, n)
	seen := make(map[string]bool)

	for i := 0; len(entries) < n; i++ {
		k := kadtest.RandomKey()
		if seen[k.String()] {
			continue
		}
		seen[k.String()] = true
		entries = append(entries, Entry[kadtest.Key32, int]{
			Key:  k,
			Data: i,
		})
	}

	added := tr.AddMany(entries...)
	require.Equal(t, n, added)
	require.Equal(t, n, tr.Size())

	// Verify all keys are findable
	for _, e := range entries {
		found, data := Find(tr, e.Key)
		require.True(t, found)
		require.Equal(t, e.Data, data)
	}

	if d := CheckInvariant(tr); d != nil {
		t.Fatalf("trie invariant discrepancy: %v", d)
	}
}

func TestImmutableAddIgnoresDuplicates(t *testing.T) {
	tr := New[kadtest.Key32, any]()
	var err error
	for _, kk := range sampleKeySet.Keys {
		tr, err = Add(tr, kk, nil)
		require.NoError(t, err)
	}
	require.Equal(t, len(sampleKeySet.Keys), tr.Size())

	for _, kk := range sampleKeySet.Keys {
		tr, err = Add(tr, kk, nil)
		require.NoError(t, err)
	}
	require.Equal(t, len(sampleKeySet.Keys), tr.Size())

	if d := CheckInvariant(tr); d != nil {
		t.Fatalf("reordered trie invariant discrepancy: %v", d)
	}
}

func TestImmutableAddReturnsOriginalTrieForDuplicates(t *testing.T) {
	tr := New[kadtest.Key32, any]()
	var err error
	for _, kk := range sampleKeySet.Keys {
		tr, err = Add(tr, kk, nil)
		require.NoError(t, err)
	}
	require.Equal(t, len(sampleKeySet.Keys), tr.Size())

	for _, kk := range sampleKeySet.Keys {
		next, err := Add(tr, kk, nil)
		require.NoError(t, err)
		// trie has not been changed
		require.Same(t, tr, next)
	}
	require.Equal(t, len(sampleKeySet.Keys), tr.Size())

	if d := CheckInvariant(tr); d != nil {
		t.Fatalf("reordered trie invariant discrepancy: %v", d)
	}
}

func TestAddWithData(t *testing.T) {
	tr := New[kadtest.Key32, int]()
	for i, kk := range sampleKeySet.Keys {
		added := tr.Add(kk, i)
		require.True(t, added)
	}
	require.Equal(t, len(sampleKeySet.Keys), tr.Size())

	for i, kk := range sampleKeySet.Keys {
		found, v := Find(tr, kk)
		require.True(t, found)
		require.Equal(t, i, v)
	}
}

func TestImmutableAddWithData(t *testing.T) {
	tr := New[kadtest.Key32, int]()
	for i, kk := range sampleKeySet.Keys {
		var err error
		trNext, err := Add(tr, kk, i)
		require.NoError(t, err)
		// a new trie must be returned
		require.NotSame(t, tr, trNext)
		tr = trNext
	}
	require.Equal(t, len(sampleKeySet.Keys), tr.Size())

	for i, kk := range sampleKeySet.Keys {
		found, v := Find(tr, kk)
		require.True(t, found)
		require.Equal(t, i, v)
	}
}

func TestRemove(t *testing.T) {
	tr, err := trieFromKeys[kadtest.Key32, any](sampleKeySet.Keys)
	if err != nil {
		t.Fatalf("unexpected error during from keys: %v", err)
	}
	require.Equal(t, len(sampleKeySet.Keys), tr.Size())

	removed := tr.Remove(sampleKeySet.Keys[0])
	require.True(t, removed)
	require.Equal(t, len(sampleKeySet.Keys)-1, tr.Size())

	if d := CheckInvariant(tr); d != nil {
		t.Fatalf("reordered trie invariant discrepancy: %v", d)
	}
}

func TestImmutableRemove(t *testing.T) {
	tr, err := trieFromKeys[kadtest.Key32, any](sampleKeySet.Keys)
	if err != nil {
		t.Fatalf("unexpected error during from keys: %v", err)
	}
	require.Equal(t, len(sampleKeySet.Keys), tr.Size())

	trNext, err := Remove(tr, sampleKeySet.Keys[0])
	require.NoError(t, err)
	require.Equal(t, len(sampleKeySet.Keys)-1, trNext.Size())

	// a new trie must be returned
	require.NotSame(t, tr, trNext)

	if d := CheckInvariant(tr); d != nil {
		t.Fatalf("reordered trie invariant discrepancy: %v", d)
	}
}

func TestRemoveFromEmpty(t *testing.T) {
	tr := New[kadtest.Key32, any]()
	removed := tr.Remove(sampleKeySet.Keys[0])
	require.False(t, removed)
	require.Equal(t, 0, tr.Size())

	if d := CheckInvariant(tr); d != nil {
		t.Fatalf("reordered trie invariant discrepancy: %v", d)
	}
}

func TestImmutableRemoveFromEmpty(t *testing.T) {
	tr := New[kadtest.Key32, any]()
	trNext, err := Remove(tr, sampleKeySet.Keys[0])
	require.NoError(t, err)
	require.Equal(t, 0, tr.Size())

	// trie has not been changed
	require.Same(t, tr, trNext)
}

func TestRemoveUnknown(t *testing.T) {
	tr, err := trieFromKeys[kadtest.Key32, any](sampleKeySet.Keys)
	if err != nil {
		t.Fatalf("unexpected error during from keys: %v", err)
	}
	require.Equal(t, len(sampleKeySet.Keys), tr.Size())

	unknown := newKeyNotInSet(sampleKeySet)

	removed := tr.Remove(unknown)
	require.False(t, removed)
	require.Equal(t, len(sampleKeySet.Keys), tr.Size())

	if d := CheckInvariant(tr); d != nil {
		t.Fatalf("reordered trie invariant discrepancy: %v", d)
	}
}

func TestImmutableRemoveUnknown(t *testing.T) {
	tr, err := trieFromKeys[kadtest.Key32, any](sampleKeySet.Keys)
	if err != nil {
		t.Fatalf("unexpected error during from keys: %v", err)
	}
	require.Equal(t, len(sampleKeySet.Keys), tr.Size())

	unknown := newKeyNotInSet(sampleKeySet)

	trNext, err := Remove(tr, unknown)
	require.NoError(t, err)

	// trie has not been changed
	require.Same(t, tr, trNext)

	if d := CheckInvariant(trNext); d != nil {
		t.Fatalf("reordered trie invariant discrepancy: %v", d)
	}
}

func TestEqual(t *testing.T) {
	a, err := trieFromKeys[kadtest.Key32, any](sampleKeySet.Keys)
	if err != nil {
		t.Fatalf("unexpected error during from keys: %v", err)
	}
	b, err := trieFromKeys[kadtest.Key32, any](sampleKeySet.Keys)
	if err != nil {
		t.Fatalf("unexpected error during from keys: %v", err)
	}
	require.True(t, Equal(a, b))

	sampleKeySet2 := newKeySetOfLength(12)
	c, err := trieFromKeys[kadtest.Key32, any](sampleKeySet2.Keys)
	if err != nil {
		t.Fatalf("unexpected error during from keys: %v", err)
	}
	require.False(t, Equal(a, c))
}

func TestFindNoData(t *testing.T) {
	tr, err := trieFromKeys[kadtest.Key32, any](sampleKeySet.Keys)
	if err != nil {
		t.Fatalf("unexpected error during from keys: %v", err)
	}

	for _, kk := range sampleKeySet.Keys {
		found, _ := Find(tr, kk)
		require.True(t, found)
	}
}

func TestFindNotFound(t *testing.T) {
	tr, err := trieFromKeys[kadtest.Key32, any](sampleKeySet.Keys[1:])
	if err != nil {
		t.Fatalf("unexpected error during from keys: %v", err)
	}

	found, _ := Find(tr, sampleKeySet.Keys[0])
	require.False(t, found)
}

func TestFindMismatchedKeyLength(t *testing.T) {
	tr, err := trieFromKeys[kadtest.Key32, any](sampleKeySet.Keys)
	if err != nil {
		t.Fatalf("unexpected error during from keys: %v", err)
	}

	found, _ := Find(tr, kadtest.RandomKey())
	require.False(t, found)
}

func TestFindWithData(t *testing.T) {
	tr := New[kadtest.Key32, int]()

	var err error
	for i, kk := range sampleKeySet.Keys {
		tr, err = Add(tr, kk, i)
		require.NoError(t, err)
	}

	for i, kk := range sampleKeySet.Keys {
		found, v := Find(tr, kk)
		require.True(t, found)
		require.Equal(t, i, v)
	}
}

func TestClosest(t *testing.T) {
	key0 := kadtest.Key8(0)

	keys := []kadtest.Key8{
		kadtest.Key8(0b00010000),
		kadtest.Key8(0b00100000),
		kadtest.Key8(0b00110000),
		kadtest.Key8(0b00111000),
		kadtest.Key8(0b00011000),
		kadtest.Key8(0b00011100),
		kadtest.Key8(0b00000110),
		kadtest.Key8(0b00000101),
		kadtest.Key8(0b00000100),
		kadtest.Key8(0b00001000),
		kadtest.Key8(0b00001100),
	}

	tr, err := trieFromKeys[kadtest.Key8, int](keys)
	require.NoError(t, err)

	requireEntriesOrdered := func(t *testing.T, target kadtest.Key8, found []Entry[kadtest.Key8, int]) {
		t.Helper()
		for i := range found {
			t.Logf("found[%d]=%v (distance=%v)", i, found[i].Key, target.Xor(found[i].Key))
			// assert that first key is closer or same as every other possible key
			if i == 0 {
				distanceFirst := target.Xor(found[0].Key)
				for i, k := range keys {
					// skip target and first key
					if key.Equal(target, k) || key.Equal(found[0].Key, k) {
						continue
					}
					distanceKey := target.Xor(k)
					t.Logf("first result distance to target=%v, distance to key[%d]=%v", distanceFirst, i, distanceKey)
					require.True(t, distanceFirst.Compare(distanceKey) <= 0, "distance from first(key:%v) to target(key:%v)=%v, distance to key[%d](key:%v)=%v", found[0].Key, distanceFirst, target, i, k, distanceKey)
				}
				continue
			}
			// assert that this key is further or same as the previous key
			distancePrev := target.Xor(found[i-1].Key)
			distanceThis := target.Xor(found[i].Key)
			require.True(t, distancePrev.Compare(distanceThis) <= 0, "distance[%d]=%v, distance[%d]=%v", i-1, distancePrev, i, distanceThis)
		}
	}

	t.Run("zero", func(t *testing.T) {
		// find the 5 nearest keys to the zero key
		found := Closest(tr, key0, 5)
		require.Equal(t, 5, len(found))
		requireEntriesOrdered(t, key0, found)
	})

	t.Run("key8", func(t *testing.T) {
		// find the 3 nearest keys to keys[8]
		found := Closest(tr, keys[8], 3)
		require.Equal(t, 3, len(found))
		requireEntriesOrdered(t, keys[8], found)
	})

	t.Run("key3overflow", func(t *testing.T) {
		// find the more than available number of keys close to keys[3]
		found := Closest(tr, keys[3], 20)
		require.Equal(t, len(keys), len(found))
		requireEntriesOrdered(t, keys[3], found)
	})
}

func BenchmarkBuildTrieMutable(b *testing.B) {
	b.Run("1000", benchmarkBuildTrieMutable(1000))
	b.Run("10000", benchmarkBuildTrieMutable(10000))
	b.Run("100000", benchmarkBuildTrieMutable(100000))
}

func BenchmarkBuildTrieImmutable(b *testing.B) {
	b.Run("1000", benchmarkBuildTrieImmutable(1000))
	b.Run("10000", benchmarkBuildTrieImmutable(10000))
	b.Run("100000", benchmarkBuildTrieImmutable(100000))
}

func BenchmarkAddMutable(b *testing.B) {
	b.Run("1000", benchmarkAddMutable(1000))
	b.Run("10000", benchmarkAddMutable(10000))
	b.Run("100000", benchmarkAddMutable(100000))
}

func BenchmarkAddImmutable(b *testing.B) {
	b.Run("1000", benchmarkAddImmutable(1000))
	b.Run("10000", benchmarkAddImmutable(10000))
	b.Run("100000", benchmarkAddImmutable(100000))
}

func BenchmarkRemoveMutable(b *testing.B) {
	b.Run("1000", benchmarkRemoveMutable(1000))
	b.Run("10000", benchmarkRemoveMutable(10000))
	b.Run("100000", benchmarkRemoveMutable(100000))
}

func BenchmarkRemoveImmutable(b *testing.B) {
	b.Run("1000", benchmarkRemoveImmutable(1000))
	b.Run("10000", benchmarkRemoveImmutable(10000))
	b.Run("100000", benchmarkRemoveImmutable(100000))
}

func BenchmarkFindPositive(b *testing.B) {
	b.Run("1000", benchmarkFindPositive(1000))
	b.Run("10000", benchmarkFindPositive(10000))
	b.Run("100000", benchmarkFindPositive(100000))
}

func BenchmarkFindNegative(b *testing.B) {
	b.Run("1000", benchmarkFindNegative(1000))
	b.Run("10000", benchmarkFindNegative(10000))
	b.Run("100000", benchmarkFindNegative(100000))
}

func benchmarkBuildTrieMutable(n int) func(b *testing.B) {
	return func(b *testing.B) {
		keys := make([]kadtest.Key32, n)
		for i := range n {
			keys[i] = kadtest.RandomKey()
		}
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			tr := New[kadtest.Key32, any]()
			for _, kk := range keys {
				tr.Add(kk, nil)
			}
		}
		kadtest.ReportTimePerItemMetric(b, n, "key")
	}
}

func benchmarkBuildTrieImmutable(n int) func(b *testing.B) {
	return func(b *testing.B) {
		keys := make([]kadtest.Key32, n)
		for i := range n {
			keys[i] = kadtest.RandomKey()
		}
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			tr := New[kadtest.Key32, any]()
			for _, kk := range keys {
				tr, _ = Add(tr, kk, nil)
			}
		}
		kadtest.ReportTimePerItemMetric(b, n, "key")
	}
}

func benchmarkAddMutable(n int) func(b *testing.B) {
	return func(b *testing.B) {
		keys := make([]kadtest.Key32, n)
		for i := range n {
			keys[i] = kadtest.RandomKey()
		}
		tr := New[kadtest.Key32, any]()
		for _, kk := range keys {
			tr, _ = Add(tr, kk, nil)
		}

		// number of additions has to be large enough so that benchmarking takes
		// more time than cloning the trie
		// see https://github.com/golang/go/issues/27217
		additions := make([]kadtest.Key32, n/4)
		for i := range additions {
			additions[i] = kadtest.RandomKey()
		}
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			trclone := tr.Copy()
			b.StartTimer()
			for _, kk := range additions {
				trclone.Add(kk, nil)
			}
		}
		kadtest.ReportTimePerItemMetric(b, len(additions), "key")
	}
}

func benchmarkAddImmutable(n int) func(b *testing.B) {
	return func(b *testing.B) {
		keys := make([]kadtest.Key32, n)
		for i := range n {
			keys[i] = kadtest.RandomKey()
		}
		trBase := New[kadtest.Key32, any]()
		for _, kk := range keys {
			trBase, _ = Add(trBase, kk, nil)
		}

		additions := make([]kadtest.Key32, n/4)
		for i := range additions {
			additions[i] = kadtest.RandomKey()
		}
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			tr := trBase
			for _, kk := range additions {
				tr, _ = Add(tr, kk, nil)
			}
		}
		kadtest.ReportTimePerItemMetric(b, len(additions), "key")
	}
}

func benchmarkRemoveMutable(n int) func(b *testing.B) {
	return func(b *testing.B) {
		keys := make([]kadtest.Key32, n)
		for i := range n {
			keys[i] = kadtest.RandomKey()
		}
		tr := New[kadtest.Key32, any]()
		for _, kk := range keys {
			tr, _ = Add(tr, kk, nil)
		}

		// number of removals has to be large enough so that benchmarking takes
		// more time than cloning the trie
		// see https://github.com/golang/go/issues/27217
		removals := make([]kadtest.Key32, n/4)
		for i := range removals {
			removals[i] = keys[i*4] // every 4th key
		}
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			trclone := tr.Copy()
			b.StartTimer()
			for _, kk := range removals {
				trclone.Remove(kk)
			}
		}
		kadtest.ReportTimePerItemMetric(b, len(removals), "key")
	}
}

func benchmarkRemoveImmutable(n int) func(b *testing.B) {
	return func(b *testing.B) {
		keys := make([]kadtest.Key32, n)
		for i := range n {
			keys[i] = kadtest.RandomKey()
		}
		trBase := New[kadtest.Key32, any]()
		for _, kk := range keys {
			trBase, _ = Add(trBase, kk, nil)
		}

		removals := make([]kadtest.Key32, n/4)
		for i := range removals {
			removals[i] = keys[i*4] // every 4th key
		}
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			tr := trBase
			for _, kk := range removals {
				tr, _ = Remove(tr, kk)
			}
		}
		kadtest.ReportTimePerItemMetric(b, len(removals), "key")
	}
}

func benchmarkFindPositive(n int) func(b *testing.B) {
	return func(b *testing.B) {
		keys := make([]kadtest.Key32, n)
		for i := range n {
			keys[i] = kadtest.RandomKey()
		}
		tr := New[kadtest.Key32, any]()
		for _, kk := range keys {
			tr, _ = Add(tr, kk, nil)
		}
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			Find(tr, keys[i%len(keys)])
		}
	}
}

func benchmarkFindNegative(n int) func(b *testing.B) {
	return func(b *testing.B) {
		keys := make([]kadtest.Key32, n)
		for i := range n {
			keys[i] = kadtest.RandomKey()
		}
		tr := New[kadtest.Key32, any]()
		for _, kk := range keys {
			tr, _ = Add(tr, kk, nil)
		}
		unknown := make([]kadtest.Key32, n)
		for i := range n {
			kk := kadtest.RandomKey()
			if found, _ := Find(tr, kk); found {
				continue
			}
			unknown[i] = kk
		}

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			Find(tr, unknown[i%len(unknown)])
		}
	}
}

type keySet struct {
	Keys []kadtest.Key32
}

var sampleKeySet = newKeySetOfLength(12)

func newKeySetList(n int) []*keySet {
	s := make([]*keySet, n)
	for i := range s {
		s[i] = newKeySetOfLength(31)
	}
	return s
}

func newKeySetOfLength(n int) *keySet {
	set := make([]kadtest.Key32, 0, n)
	seen := make(map[string]bool)
	for len(set) < n {
		kk := kadtest.RandomKey()
		if seen[kk.String()] {
			continue
		}
		seen[kk.String()] = true
		set = append(set, kk)
	}
	return &keySet{
		Keys: set,
	}
}

func newKeyNotInSet(ks *keySet) kadtest.Key32 {
	seen := make(map[string]bool)
	for i := range ks.Keys {
		seen[ks.Keys[i].String()] = true
	}

	kk := kadtest.RandomKey()
	for seen[kk.String()] {
		kk = kadtest.RandomKey()
	}

	return kk
}

type InvariantDiscrepancy struct {
	Reason            string
	PathToDiscrepancy string
	KeyAtDiscrepancy  string
}

// CheckInvariant panics of the trie does not meet its invariant.
func CheckInvariant[K kad.Key[K], D any](tr *Trie[K, D]) *InvariantDiscrepancy {
	return checkInvariant(tr, 0, nil)
}

func checkInvariant[K kad.Key[K], D any](tr *Trie[K, D], depth int, pathSoFar *triePath[K]) *InvariantDiscrepancy {
	switch {
	case tr.IsEmptyLeaf():
		return nil
	case tr.IsNonEmptyLeaf():
		if !pathSoFar.matchesKey(*tr.key) {
			return &InvariantDiscrepancy{
				Reason:            "key found at invalid location in trie",
				PathToDiscrepancy: pathSoFar.BitString(),
				KeyAtDiscrepancy:  key.BitString(*tr.key),
			}
		}
		return nil
	default:
		if !tr.HasKey() {
			b0, b1 := tr.branch[0], tr.branch[1]
			if d0 := checkInvariant(b0, depth+1, pathSoFar.Push(0)); d0 != nil {
				return d0
			}
			if d1 := checkInvariant(b1, depth+1, pathSoFar.Push(1)); d1 != nil {
				return d1
			}
			switch {
			case b0.IsEmptyLeaf() && b1.IsEmptyLeaf():
				return &InvariantDiscrepancy{
					Reason:            "intermediate node with two empty leaves",
					PathToDiscrepancy: pathSoFar.BitString(),
					KeyAtDiscrepancy:  "none",
				}
			case b0.IsEmptyLeaf() && b1.IsNonEmptyLeaf():
				return &InvariantDiscrepancy{
					Reason:            "intermediate node with one empty leaf/0",
					PathToDiscrepancy: pathSoFar.BitString(),
					KeyAtDiscrepancy:  "none",
				}
			case b0.IsNonEmptyLeaf() && b1.IsEmptyLeaf():
				return &InvariantDiscrepancy{
					Reason:            "intermediate node with one empty leaf/1",
					PathToDiscrepancy: pathSoFar.BitString(),
					KeyAtDiscrepancy:  "none",
				}
			default:
				return nil
			}
		} else {
			return &InvariantDiscrepancy{
				Reason:            "intermediate node with a key",
				PathToDiscrepancy: pathSoFar.BitString(),
				KeyAtDiscrepancy:  key.BitString(*tr.key),
			}
		}
	}
}

type triePath[K kad.Key[K]] struct {
	parent *triePath[K]
	bit    byte
}

func (p *triePath[K]) Push(bit byte) *triePath[K] {
	return &triePath[K]{parent: p, bit: bit}
}

func (p *triePath[K]) RootPath() []byte {
	if p == nil {
		return nil
	} else {
		return append(p.parent.RootPath(), p.bit)
	}
}

func (p *triePath[K]) matchesKey(kk K) bool {
	ok, _ := p.walk(kk, 0)
	return ok
}

func (p *triePath[K]) walk(kk K, depthToLeaf int) (ok bool, depthToRoot int) {
	if p == nil {
		return true, 0
	} else {
		parOk, parDepthToRoot := p.parent.walk(kk, depthToLeaf+1)
		return kk.Bit(parDepthToRoot) == uint(p.bit) && parOk, parDepthToRoot + 1
	}
}

func (p *triePath[K]) BitString() string {
	return p.bitString(0)
}

func (p *triePath[K]) bitString(depthToLeaf int) string {
	if p == nil {
		return ""
	} else {
		switch p.bit {
		case 0:
			return p.parent.bitString(depthToLeaf+1) + "0"
		case 1:
			return p.parent.bitString(depthToLeaf+1) + "1"
		default:
			panic("bit digit > 1")
		}
	}
}
