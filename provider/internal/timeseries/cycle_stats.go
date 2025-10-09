package timeseries

import (
	"time"

	"github.com/libp2p/go-libp2p-kad-dht/provider/internal/keyspace"
	"github.com/probe-lab/go-libdht/kad/key/bit256"
	"github.com/probe-lab/go-libdht/kad/key/bitstr"
	"github.com/probe-lab/go-libdht/kad/trie"
)

type entry struct {
	time time.Time
	val  int64
}

// CycleStats tracks statistics organized by keyspace prefixes with TTL-based cleanup.
// It maintains a trie structure where statistics are aggregated by prefix and
// automatically cleaned up after the retention period.
type CycleStats struct {
	trie *trie.Trie[bitstr.Key, entry]

	queue *trie.Trie[bitstr.Key, entry]

	ttl, maxDelay time.Duration
}

// NewCycleStats creates a new CycleStats with the specified TTL and maximum delay.
func NewCycleStats(ttl, maxDelay time.Duration) CycleStats {
	return CycleStats{
		trie:     trie.New[bitstr.Key, entry](),
		queue:    trie.New[bitstr.Key, entry](),
		ttl:      ttl,
		maxDelay: maxDelay,
	}
}

// Cleanup removes entries that have exceeded their TTL plus maximum delay.
func (s *CycleStats) Cleanup() {
	allEntries := keyspace.AllEntries(s.trie, bit256.ZeroKey())
	for _, e := range allEntries {
		if e.Data.time.Add(s.ttl + s.maxDelay).Before(time.Now()) {
			s.trie.Remove(e.Key)
			if subtrie, ok := keyspace.FindSubtrie(s.queue, e.Key); ok {
				for _, qe := range keyspace.AllEntries(subtrie, bit256.ZeroKey()) {
					s.trie.Add(qe.Key, qe.Data)
				}
			}
		}
	}
}

// Add records a value for the given prefix, handling prefix aggregation logic.
func (s *CycleStats) Add(prefix bitstr.Key, val int64) {
	e := entry{time: time.Now(), val: val}
	if _, ok := keyspace.FindSubtrie(s.trie, prefix); ok {
		// shorter prefix
		keyspace.PruneSubtrie(s.trie, prefix)
		s.trie.Add(prefix, e)
		return
	}
	// longer prefix, group with complements before replacing
	target, ok := keyspace.FindPrefixOfKey(s.trie, prefix)
	if !ok {
		// No keys in s.trie is a prefix of `prefix`
		s.trie.Add(prefix, e)
		return
	}

	if queuePrefix, ok := keyspace.FindPrefixOfKey(s.queue, prefix); ok {
		_, entry := trie.Find(s.queue, queuePrefix)
		if time.Since(entry.time) < s.maxDelay {
			// A recent entry is a superset of the current one, skip.
			return
		}
		// Remove old entry
		keyspace.PruneSubtrie(s.queue, queuePrefix)
	} else {
		// Remove (older) superstrings from queue
		keyspace.PruneSubtrie(s.queue, prefix)
	}
	// Add prefix to queue
	s.queue.Add(prefix, e)

	subtrie, ok := keyspace.FindSubtrie(s.queue, target)
	if !ok || !keyspace.KeyspaceCovered(subtrie) {
		// Subtrie not complete
		return
	}
	// Target keyspace is fully covered by queue entries. Replace target with
	// queue entries.
	keyspace.PruneSubtrie(s.trie, target)
	for _, e := range keyspace.AllEntries(subtrie, bit256.ZeroKey()) {
		s.trie.Add(e.Key, e.Data)
	}
}

// Sum returns the sum of all values in the trie.
func (s *CycleStats) Sum() int64 {
	var sum int64
	for _, v := range keyspace.AllValues(s.trie, bit256.ZeroKey()) {
		sum += v.val
	}
	return sum
}

// Avg returns the average of all values in the trie.
func (s *CycleStats) Avg() float64 {
	allValues := keyspace.AllValues(s.trie, bit256.ZeroKey())
	if len(allValues) == 0 {
		return 0
	}
	var sum int64
	for _, v := range allValues {
		sum += v.val
	}
	return float64(sum) / float64(len(allValues))
}

// Count returns the number of entries in the trie.
func (s *CycleStats) Count() int {
	return s.trie.Size()
}

// FullyCovered returns true if the trie covers the complete keyspace.
func (s *CycleStats) FullyCovered() bool {
	return keyspace.KeyspaceCovered(s.trie)
}
