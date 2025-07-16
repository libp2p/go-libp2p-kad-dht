package queue

import (
	"slices"
	"sync"

	"github.com/gammazero/deque"
	"github.com/libp2p/go-libp2p-kad-dht/provider/internal/helpers"
	mh "github.com/multiformats/go-multihash"

	"github.com/probe-lab/go-libdht/kad/key/bit256"
	"github.com/probe-lab/go-libdht/kad/key/bitstr"
	"github.com/probe-lab/go-libdht/kad/trie"
)

type ProvideQueue struct {
	lk sync.Mutex

	queue    deque.Deque[bitstr.Key]          // used to preserve the queue order
	prefixes *trie.Trie[bitstr.Key, struct{}] // used to track prefixes that have been added to the queue

	keys *trie.Trie[bit256.Key, mh.Multihash] // used to store keys in the queue
}

func New() *ProvideQueue {
	return &ProvideQueue{
		prefixes: trie.New[bitstr.Key, struct{}](),
		keys:     trie.New[bit256.Key, mh.Multihash](),
	}
}

// all keys MUST match prefix
func (q *ProvideQueue) Enqueue(prefix bitstr.Key, keys ...mh.Multihash) {
	if len(keys) == 0 {
		return
	}
	q.lk.Lock()
	defer q.lk.Unlock()

	if subtrie, ok := helpers.SubtrieMatchingPrefix(q.prefixes, prefix); ok {
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

	// Add keys to the keys trie.
	for _, h := range keys {
		q.keys.Add(helpers.MhToBit256(h), h)
	}
}

func (q *ProvideQueue) Dequeue() (bitstr.Key, []mh.Multihash) {
	q.lk.Lock()
	defer q.lk.Unlock()

	// Dequeue the first prefix from the queue.
	prefix := q.queue.PopFront()
	// Remove the prefix from the prefixes trie.
	q.prefixes.Remove(prefix)

	// Get all keys that match the prefix.
	subtrie, _ := helpers.SubtrieMatchingPrefix(q.keys, prefix)
	keys := helpers.AllValues(subtrie, bit256.ZeroKey())

	// Remove the keys from the keys trie.
	helpers.PruneSubtrie(q.keys, prefix)

	return prefix, keys
}

func (q *ProvideQueue) DequeueMatching(prefix bitstr.Key) []mh.Multihash {
	q.lk.Lock()
	defer q.lk.Unlock()

	subtrie, ok := helpers.SubtrieMatchingPrefix(q.keys, prefix)
	if !ok {
		// No keys matching the prefix.
		return nil
	}
	keys := helpers.AllValues(subtrie, bit256.ZeroKey())

	// Remove the keys from the keys trie.
	helpers.PruneSubtrie(q.keys, prefix)

	// Remove prefix from queue and prefixes trie.
	if subtrie, ok := helpers.SubtrieMatchingPrefix(q.prefixes, prefix); ok {
		// There are superstrings of `prefix` in the queue.
		entriesToRemove := helpers.AllEntries(subtrie, bit256.ZeroKey())
		prefixesToRemove := make([]bitstr.Key, len(entriesToRemove))
		for i, entry := range entriesToRemove {
			prefixesToRemove[i] = entry.Key
		}
		// Remove superstrings of `prefix` from the queue
		q.removePrefixesFromQueue(prefixesToRemove)
	} else if shorterPrefix, ok := helpers.FindPrefixOfKey(q.prefixes, prefix); ok {
		// `prefix` is a superstring of some other shorter prefix in the queue.
		// Leave it in the queue, unless the shorter prefix doesn't have any
		// matching keys left.
		if _, ok := helpers.SubtrieMatchingPrefix(q.keys, shorterPrefix); !ok {
			q.prefixes.Remove(shorterPrefix)
			index := q.queue.Index(func(element bitstr.Key) bool { return element == shorterPrefix })
			if index >= 0 {
				q.queue.Remove(index)
			}
		}
	}

	return keys
}

// Empty returns true if the queue is empty.
func (q *ProvideQueue) Empty() bool {
	q.lk.Lock()
	defer q.lk.Unlock()
	return q.queue.Len() == 0
}

// Size returns the number of regions containing at least one key in the queue.
func (q *ProvideQueue) Size() int {
	q.lk.Lock()
	defer q.lk.Unlock()
	return q.sizeNoLock()
}

// sizeNoLock returns the number of regions containing at least one key in the
// queue. It assumes the mutex is held already.
func (q *ProvideQueue) sizeNoLock() int {
	return q.keys.Size()
}

// Clear removes all keys from the queue and returns the number of keys that
// were removed.
func (q *ProvideQueue) Clear() int {
	q.lk.Lock()
	defer q.lk.Unlock()
	size := q.sizeNoLock()

	q.queue.Clear()
	*q.prefixes = trie.Trie[bitstr.Key, struct{}]{}
	*q.keys = trie.Trie[bit256.Key, mh.Multihash]{}

	return size
}

// removeSubtrieFromQueue removes all keys in the provided subtrie from q.queue
// and q.prefixes. Returns the position of the first removed key in the queue.
func (q *ProvideQueue) removePrefixesFromQueue(prefixes []bitstr.Key) int {
	indexes := make([]int, 0, len(prefixes))
	for _, prefix := range prefixes {
		// Remove elements from the queue that are superstrings of `prefix`.
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
