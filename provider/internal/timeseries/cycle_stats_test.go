package timeseries

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/ipfs/go-libdht/kad/key/bitstr"
	"github.com/stretchr/testify/require"
)

func TestCycleStatsSimple(t *testing.T) {
	cs := NewCycleStats(time.Minute)

	// Test empty stats
	require.Equal(t, int64(0), cs.Sum(), "sum of empty CycleStats")
	require.Equal(t, 0.0, cs.Avg(), "avg of empty CycleStats")
	require.Equal(t, 0, cs.Count(), "count of empty CycleStats")

	// Add non-overlapping prefixes
	cs.Add(bitstr.Key("0"), 10)
	cs.Add(bitstr.Key("1"), 20)

	require.Equal(t, int64(30), cs.Sum(), "sum with two prefixes")
	require.Equal(t, 2, cs.Count(), "count with two prefixes")
	require.Equal(t, 15.0, cs.Avg(), "average with two prefixes")
}

func TestCycleStatsSimpleFullyCovered(t *testing.T) {
	cs := NewCycleStats(time.Minute)

	// Single bit prefixes should cover full keyspace
	cs.Add(bitstr.Key("0"), 10)
	cs.Add(bitstr.Key("1"), 20)

	require.True(t, cs.FullyCovered(), "should be fully covered with prefixes '0' and '1'")
}

func TestCycleStatsSimpleCleanup(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// Use very short durations for testing
		maxDelay := time.Millisecond
		cs := NewCycleStats(maxDelay)

		cs.Add(bitstr.Key("0"), 10)
		cs.Add(bitstr.Key("1"), 20)

		require.Equal(t, 2, cs.Count(), "count before cleanup")

		// Wait for entries to expire
		time.Sleep(5 * time.Millisecond)
		cs.Cleanup(2 * time.Millisecond)

		require.Equal(t, 0, cs.Count(), "count after cleanup")
		require.Equal(t, int64(0), cs.Sum(), "sum after cleanup")
	})
}

func TestCycleStatsReplacement(t *testing.T) {
	cs := NewCycleStats(time.Minute)

	// Add parent prefix
	cs.Add(bitstr.Key(""), 100) // Root covers everything

	require.Equal(t, 1, cs.Count(), "count with root prefix")
	require.True(t, cs.FullyCovered(), "should be fully covered with root prefix")

	// Add more specific prefixes that should replace the root
	cs.Add(bitstr.Key("0"), 20)
	cs.Add(bitstr.Key("1"), 20)

	// Should now have 2 entries instead of 1
	require.Equal(t, 2, cs.Count(), "count after replacement")
	require.Equal(t, int64(40), cs.Sum(), "sum after replacement")
}

func TestCycleStatsShorterPrefix(t *testing.T) {
	cs := NewCycleStats(time.Minute)

	// Add some specific prefixes
	cs.Add(bitstr.Key("000"), 10)
	cs.Add(bitstr.Key("001"), 20)
	cs.Add(bitstr.Key("010"), 30)

	require.Equal(t, 3, cs.Count(), "count with three specific prefixes")

	// Add a shorter prefix that should prune the subtrie
	cs.Add(bitstr.Key("00"), 100)

	// Should now have "00"->100 and "010"->30
	require.Equal(t, 2, cs.Count(), "count after shorter prefix addition")
	require.Equal(t, int64(130), cs.Sum(), "sum after shorter prefix addition")
}

func TestCycleStatsZeroValues(t *testing.T) {
	cs := NewCycleStats(time.Minute)

	cs.Add(bitstr.Key("0"), 0)
	cs.Add(bitstr.Key("1"), 10)

	require.Equal(t, 2, cs.Count(), "count with zero value")
	require.Equal(t, int64(10), cs.Sum(), "sum with zero value")
	require.Equal(t, 5.0, cs.Avg(), "average with zero value")
}

func TestCycleStatsCleanupPromotesQueue(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// This tests that when a parent entry expires, queued child entries are promoted
		maxDelay := time.Millisecond
		cs := NewCycleStats(maxDelay)

		// Add parent prefix
		cs.Add(bitstr.Key("0"), 100)
		require.Equal(t, 1, cs.Count(), "should have 1 entry in main trie")
		require.Equal(t, int64(100), cs.Sum(), "sum should be 100")

		// Add one child prefix that goes into queue (doesn't fully cover "0")
		time.Sleep(2 * time.Millisecond)
		cs.Add(bitstr.Key("00"), 30)

		// Child entry is in queue, not fully covering parent
		// Main trie still shows parent
		require.Equal(t, 1, cs.Count(), "should still have 1 entry (partial coverage)")
		require.Equal(t, int64(100), cs.Sum(), "sum should still be 100 (queue not visible)")

		// Now wait for parent to expire and clean up
		time.Sleep(5 * time.Millisecond)
		cs.Cleanup(3 * time.Millisecond)

		// After cleanup, parent is removed and queued entry is promoted
		require.Equal(t, 1, cs.Count(), "should have 1 entry after promotion")
		require.Equal(t, int64(30), cs.Sum(), "sum should be 30 (queued entry promoted)")
	})
}

func TestCycleStatsCleanupWithoutQueue(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// Test that cleanup removes entries even when there's nothing in queue
		maxDelay := time.Millisecond
		cs := NewCycleStats(maxDelay)

		cs.Add(bitstr.Key("0"), 10)
		cs.Add(bitstr.Key("1"), 20)
		require.Equal(t, 2, cs.Count(), "should have 2 entries")
		require.Equal(t, int64(30), cs.Sum(), "sum should be 30")

		// Wait for entries to expire
		time.Sleep(5 * time.Millisecond)
		cs.Cleanup(2 * time.Millisecond)

		// Entries removed, nothing to promote
		require.Equal(t, 0, cs.Count(), "should have 0 entries after cleanup")
		require.Equal(t, int64(0), cs.Sum(), "sum should be 0")
	})
}

func TestCycleStatsQueueCoverageAtRootLevel(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// Test queue coverage detection - using root level like TestCycleStatsReplacement
		maxDelay := time.Minute
		cs := NewCycleStats(maxDelay)

		// Add root
		cs.Add(bitstr.Key(""), 100)
		require.Equal(t, 1, cs.Count(), "should have 1 entry")
		require.Equal(t, int64(100), cs.Sum(), "sum should be 100")

		// Add first child - goes to queue
		cs.Add(bitstr.Key("0"), 30)
		// Queue doesn't fully cover yet
		require.Equal(t, 1, cs.Count(), "should still have 1 entry (incomplete coverage)")
		require.Equal(t, int64(100), cs.Sum(), "sum should still be 100")

		// Add second child - achieves full coverage at root level
		cs.Add(bitstr.Key("1"), 40)

		// Now root is replaced with queue entries
		require.Equal(t, 2, cs.Count(), "should have 2 entries after full coverage")
		require.Equal(t, int64(70), cs.Sum(), "sum should be 70")
	})
}

func TestCycleStatsCleanupWithDifferentDeadlines(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// Test cleanup behavior with different deadline durations
		maxDelay := time.Millisecond
		cs := NewCycleStats(maxDelay)

		// Add entries at different times
		cs.Add(bitstr.Key("0"), 10)
		time.Sleep(3 * time.Millisecond)
		cs.Add(bitstr.Key("1"), 20)
		time.Sleep(3 * time.Millisecond)
		cs.Add(bitstr.Key("00"), 30) // Child of "0", goes to queue

		require.Equal(t, 2, cs.Count(), "should have 2 entries in main trie")

		// Cleanup with short deadline - only oldest entry expires
		cs.Cleanup(4 * time.Millisecond)

		// "0" expired and "00" promoted from queue, "1" still there
		require.Equal(t, 2, cs.Count(), "should have 2 entries")
		require.Contains(t, []int64{50, 50}, cs.Sum(), "sum should be 50")

		// Cleanup with longer deadline - nothing else expires
		cs.Cleanup(10 * time.Millisecond)
		require.Equal(t, 2, cs.Count(), "should still have 2 entries")
	})
}

func TestCycleStatsQueueDeduplicationWithinMaxDelay(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// Test that queue entries within maxDelay window don't duplicate
		maxDelay := 100 * time.Millisecond
		cs := NewCycleStats(maxDelay)

		// Add parent
		cs.Add(bitstr.Key("0"), 100)
		require.Equal(t, 1, cs.Count(), "should have 1 entry")
		require.Equal(t, int64(100), cs.Sum(), "sum should be 100")

		// Add child that goes to queue
		cs.Add(bitstr.Key("00"), 30)
		require.Equal(t, 1, cs.Count(), "parent still in main trie")
		require.Equal(t, int64(100), cs.Sum(), "sum still 100 (queue not visible)")

		// Try to add another entry for same prefix within maxDelay
		// This should be skipped due to maxDelay check
		cs.Add(bitstr.Key("00"), 40)
		require.Equal(t, 1, cs.Count(), "should still have 1 entry")
		require.Equal(t, int64(100), cs.Sum(), "sum still 100 (duplicate rejected)")

		// Wait past maxDelay, then cleanup with a deadline that expires the parent
		time.Sleep(150 * time.Millisecond)
		cs.Cleanup(120 * time.Millisecond)

		// Parent expired (added 150ms ago, deadline 120ms) and queued entry is promoted
		// Queue had "00"->30 (the first add), the duplicate "00"->40 was rejected by maxDelay
		require.Equal(t, 1, cs.Count(), "should have 1 entry (queue promoted)")
		require.Equal(t, int64(30), cs.Sum(), "sum should be 30 (queued entry promoted)")
	})
}
