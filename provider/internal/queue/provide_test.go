package queue

import (
	"crypto/rand"
	"testing"

	"github.com/libp2p/go-libp2p-kad-dht/provider/internal/helpers"
	mh "github.com/multiformats/go-multihash"
	"github.com/probe-lab/go-libdht/kad/key/bitstr"
	"github.com/probe-lab/go-libdht/kad/trie"
	"github.com/stretchr/testify/require"
)

func genMultihashesMatchingPrefix(prefix bitstr.Key, n int) []mh.Multihash {
	mhs := make([]mh.Multihash, 0, n)
	for i := 0; len(mhs) < n; i++ {
		digest := make([]byte, 32)
		if _, err := rand.Read(digest); err != nil {
			panic(err)
		}
		h, err := mh.Encode(digest, mh.SHA2_256)
		if err != nil {
			panic(err)
		}
		k := helpers.MhToBit256(h)
		if helpers.IsPrefix(prefix, k) {
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
