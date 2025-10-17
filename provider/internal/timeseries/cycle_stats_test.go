package timeseries

import (
	"testing"
	"time"

	"github.com/probe-lab/go-libdht/kad/key/bitstr"
	"github.com/stretchr/testify/require"
)

func TestCycleStatsSimple(t *testing.T) {
	cs := NewCycleStats(time.Hour, time.Minute)

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
	cs := NewCycleStats(time.Hour, time.Minute)

	// Single bit prefixes should cover full keyspace
	cs.Add(bitstr.Key("0"), 10)
	cs.Add(bitstr.Key("1"), 20)

	require.True(t, cs.FullyCovered(), "should be fully covered with prefixes '0' and '1'")
}

func TestCycleStatsSimpleCleanup(t *testing.T) {
	// Use very short TTL for testing
	cs := NewCycleStats(time.Millisecond, time.Millisecond)

	cs.Add(bitstr.Key("0"), 10)
	cs.Add(bitstr.Key("1"), 20)

	require.Equal(t, 2, cs.Count(), "count before cleanup")

	// Wait for entries to expire
	time.Sleep(5 * time.Millisecond)
	cs.Cleanup()

	require.Equal(t, 0, cs.Count(), "count after cleanup")
	require.Equal(t, int64(0), cs.Sum(), "sum after cleanup")
}

func TestCycleStatsReplacement(t *testing.T) {
	cs := NewCycleStats(time.Hour, time.Minute)

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
	cs := NewCycleStats(time.Hour, time.Minute)

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
	cs := NewCycleStats(time.Hour, time.Minute)

	cs.Add(bitstr.Key("0"), 0)
	cs.Add(bitstr.Key("1"), 10)

	require.Equal(t, 2, cs.Count(), "count with zero value")
	require.Equal(t, int64(10), cs.Sum(), "sum with zero value")
	require.Equal(t, 5.0, cs.Avg(), "average with zero value")
}
