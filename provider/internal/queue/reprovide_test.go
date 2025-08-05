package queue

import (
	"testing"

	"github.com/probe-lab/go-libdht/kad/key/bitstr"
	"github.com/probe-lab/go-libdht/kad/trie"
	"github.com/stretchr/testify/require"
)

func TestReprovideEnqueue(t *testing.T) {
	q := NewReprovideQueue()

	q.Enqueue("000")
	require.Equal(t, 1, q.Size())
	q.Enqueue("101")
	require.Equal(t, 2, q.Size())
	q.Enqueue("000")
	require.Equal(t, 2, q.Size()) // Duplicate prefix, size should not change
	q.Enqueue("001")
	require.Equal(t, 3, q.Size())
	q.Enqueue("1000")
	require.Equal(t, 4, q.Size())
	q.Enqueue("1001")
	require.Equal(t, 5, q.Size())

	q.Enqueue("10")
	require.Equal(t, 3, q.Size()) // "10" consolidates "1000", "1001" and "101"
	require.Equal(t, bitstr.Key("000"), q.queue.queue.At(0))
	require.Equal(t, bitstr.Key("10"), q.queue.queue.At(1)) // "10" has taken the place of "101"
	require.Equal(t, bitstr.Key("001"), q.queue.queue.At(2))
}

func TestReprovideDequeue(t *testing.T) {
	keys := []bitstr.Key{
		"10",
		"001",
		"00001",
		"00000",
		"111",
		"1101",
	}

	q := NewReprovideQueue()
	for _, k := range keys {
		q.Enqueue(k)
	}
	require.False(t, q.IsEmpty())

	for i, k := range keys {
		dequeued, ok := q.Dequeue()
		require.True(t, ok)
		require.Equal(t, k, dequeued)
		require.Equal(t, len(keys)-i-1, q.Size())
	}

	require.True(t, q.IsEmpty())
	_, ok := q.Dequeue()
	require.False(t, ok)

	require.True(t, q.IsEmpty())
}

func TestReprovideRemove(t *testing.T) {
	keys := []bitstr.Key{
		"10",
		"001",
		"00001",
		"00000",
		"111",
		"1101",
	}
	q := NewReprovideQueue()
	for _, k := range keys {
		q.Enqueue(k)
	}

	ok := q.Remove("001")
	require.True(t, ok)
	require.Negative(t, q.queue.queue.Index(func(k bitstr.Key) bool { return k == bitstr.Key("001") })) // not in queue
	ok, _ = trie.Find(q.queue.prefixes, bitstr.Key("001"))                                              // not in trie
	require.False(t, ok)

	ok = q.Remove("1111") // not in queue
	require.False(t, ok)

	require.Equal(t, len(keys)-1, q.Size())
	// Test order
	require.Equal(t, keys[0], q.queue.queue.At(0))
	require.Equal(t, keys[2], q.queue.queue.At(1))
}

func TestReprovideClearQueue(t *testing.T) {
	keys := []bitstr.Key{
		"10",
		"001",
		"00001",
		"00000",
		"111",
		"1101",
	}
	q := NewReprovideQueue()
	for _, k := range keys {
		q.Enqueue(k)
	}

	cleared := q.Clear()
	require.Equal(t, len(keys), cleared)
	require.True(t, q.IsEmpty())
	require.Equal(t, 0, q.queue.prefixes.Size())
	require.Equal(t, 0, q.queue.queue.Len())
}
