package queue

import (
	"fmt"
	"slices"

	"github.com/gammazero/deque"
	"github.com/libp2p/go-libp2p-kad-dht/provider/internal/helpers"
	"github.com/probe-lab/go-libdht/kad/key/bit256"
	"github.com/probe-lab/go-libdht/kad/key/bitstr"
	"github.com/probe-lab/go-libdht/kad/trie"
)

type prefixQueue struct {
	queue    deque.Deque[bitstr.Key]          // used to preserve the queue order
	prefixes *trie.Trie[bitstr.Key, struct{}] // used to track prefixes in the queue
}

func (q *prefixQueue) Push(prefix bitstr.Key) {
	if subtrie, ok := helpers.FindSubtrie(q.prefixes, prefix); ok {
		// Prefix is a prefix of (at least) an existing prefix in the queue.
		entriesToRemove := helpers.AllEntries(subtrie, bit256.ZeroKey())
		prefixesToRemove := make([]bitstr.Key, len(entriesToRemove))
		for i, entry := range entriesToRemove {
			prefixesToRemove[i] = entry.Key
		}
		// Remove superstrings of `prefix` from the queue
		firstRemovedIndex := q.removePrefixesFromQueue(prefixesToRemove)
		// Insert `prefix` in the queue at the location of the first removed
		// prefix (last in order of deletion).
		q.queue.Insert(firstRemovedIndex, prefix)
		// Add `prefix` to prefixes trie.
		q.prefixes.Add(prefix, struct{}{})
	} else if _, ok := helpers.FindPrefixOfKey(q.prefixes, prefix); !ok {
		// No prefixes of `prefix` found in the queue.
		q.queue.PushBack(prefix)
		q.prefixes.Add(prefix, struct{}{})
	}
}

func (q *prefixQueue) Pop() (bitstr.Key, bool) {
	if q.queue.Len() == 0 {
		return bitstr.Key(""), false
	}
	// Dequeue the first prefix from the queue.
	prefix := q.queue.PopFront()
	// Remove the prefix from the prefixes trie.
	q.prefixes.Remove(prefix)

	return prefix, true
}

func (q *prefixQueue) Remove(prefix bitstr.Key) bool {
	subtrie, ok := helpers.FindSubtrie(q.prefixes, prefix)
	if !ok {
		return false
	}
	entriesToRemove := helpers.AllEntries(subtrie, bit256.ZeroKey())
	prefixesToRemove := make([]bitstr.Key, len(entriesToRemove))
	for i, entry := range entriesToRemove {
		prefixesToRemove[i] = entry.Key
	}
	if len(prefixesToRemove) == 0 {
		return false
	}
	q.removePrefixesFromQueue(prefixesToRemove)
	return true
}

// IsEmpty returns true if the queue is empty.
func (q *prefixQueue) IsEmpty() bool {
	return q.queue.Len() == 0
}

func (q *prefixQueue) Size() int {
	return q.queue.Len()
}

// Clear removes all keys from the queue and returns the number of keys that
// were removed.
func (q *prefixQueue) Clear() int {
	size := q.Size()

	q.queue.Clear()
	*q.prefixes = trie.Trie[bitstr.Key, struct{}]{}

	return size
}

// removeSubtrieFromQueue removes all keys in the provided subtrie from q.queue
// and q.prefixes. Returns the position of the first removed key in the queue.
func (q *prefixQueue) removePrefixesFromQueue(prefixes []bitstr.Key) int {
	fmt.Println("removing prefixes from queue", prefixes, len(prefixes))
	indexes := make([]int, 0, len(prefixes))
	for _, prefix := range prefixes {
		// Remove elements from the queue that are superstrings of `prefix`.
		fmt.Println("prefix", prefix)
		q.prefixes.Remove(prefix)
		// Find indexes of the superstrings in the queue.
		index := q.queue.Index(func(element bitstr.Key) bool { return element == prefix })
		if index >= 0 {
			indexes = append(indexes, index)
		}
	}
	// Sort indexes to remove in descending order so that we can remove them
	// without affecting the indexes of the remaining elements.
	slices.Sort(indexes)
	slices.Reverse(indexes)
	// Remove items in the queue that are prefixes of `prefix`
	for _, index := range indexes {
		q.queue.Remove(index)
	}
	return indexes[len(indexes)-1] // return the position of the first removed key
}
