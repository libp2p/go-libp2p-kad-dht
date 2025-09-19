package timeseries

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestIntTimeSeries(t *testing.T) {
	ts := NewIntTimeSeries(time.Hour)

	// Test empty series
	require.Equal(t, int64(0), ts.Sum(), "sum of empty series")
	require.Equal(t, 0.0, ts.Avg(), "avg of empty series")
	require.Equal(t, 0, ts.Count(), "count of empty series")

	// Add some values
	ts.Add(10)
	ts.Add(20)
	ts.Add(30)

	// Test sum
	require.Equal(t, int64(60), ts.Sum(), "sum after adding values")

	// Test average
	require.Equal(t, 20.0, ts.Avg(), "average after adding values")

	// Test count
	require.Equal(t, 3, ts.Count(), "count after adding values")
}

func TestIntTimeSeriesRetention(t *testing.T) {
	ts := NewIntTimeSeries(time.Millisecond)

	ts.Add(10)
	ts.Add(20)

	// Wait for entries to expire
	time.Sleep(2 * time.Millisecond)

	// Add new value
	ts.Add(30)

	// Should only have the latest value
	require.Equal(t, int64(30), ts.Sum(), "sum after retention cleanup")
}

func TestFloatTimeSeries(t *testing.T) {
	ts := NewFloatTimeSeries(time.Hour)

	// Test empty series
	require.Equal(t, 0.0, ts.Sum(), "sum of empty float series")
	require.Equal(t, 0.0, ts.Avg(), "avg of empty float series")

	// Add weighted values using AddWeighted
	ts.AddWeighted(10.0, 2) // 10 * 2 = 20
	ts.AddWeighted(20.0, 3) // 20 * 3 = 60
	ts.AddWeighted(30.0, 1) // 30 * 1 = 30

	// Test weighted sum: 20 + 60 + 30 = 110
	require.Equal(t, 110.0, ts.Sum(), "weighted sum")

	// Test weighted average: 110 / (2+3+1) = 110/6 ≈ 18.33
	expectedAvg := 110.0 / 6.0
	require.Equal(t, expectedAvg, ts.Avg(), "weighted average")
}

func TestFloatTimeSeriesRetention(t *testing.T) {
	ts := NewFloatTimeSeries(time.Millisecond)

	ts.AddWeighted(10.0, 1)
	ts.AddWeighted(20.0, 2)

	// Wait for entries to expire
	time.Sleep(2 * time.Millisecond)

	// Add new value
	ts.AddWeighted(30.0, 3)

	// Should only have the latest value: 30 * 3 = 90
	require.Equal(t, 90.0, ts.Sum(), "weighted sum after retention cleanup")
}

func TestGenericTimeSeries(t *testing.T) {
	// Test the generic version directly with int64
	ts := NewTimeSeries[int64](time.Hour)

	ts.Add(100)
	ts.Add(200)

	require.Equal(t, int64(300), ts.Sum(), "generic int64 sum")
	require.Equal(t, 150.0, ts.Avg(), "generic int64 average")

	// Test the generic version with float64 (non-weighted)
	floatTS := NewTimeSeries[float64](time.Hour)

	floatTS.Add(1.5)
	floatTS.Add(2.5)

	require.Equal(t, 4.0, floatTS.Sum(), "generic float64 sum")
	require.Equal(t, 2.0, floatTS.Avg(), "generic float64 average")
}

func TestWeightedVsNonWeighted(t *testing.T) {
	// Compare weighted vs non-weighted behavior
	weighted := NewWeightedTimeSeries[float64](time.Hour)
	nonWeighted := NewTimeSeries[float64](time.Hour)

	// Add same values
	weighted.AddWeighted(10.0, 2)
	weighted.AddWeighted(20.0, 1)

	nonWeighted.Add(10.0)
	nonWeighted.Add(20.0)

	// Weighted sum: (10*2) + (20*1) = 40
	require.Equal(t, 40.0, weighted.Sum(), "weighted sum")

	// Non-weighted sum: 10 + 20 = 30
	require.Equal(t, 30.0, nonWeighted.Sum(), "non-weighted sum")

	// Weighted average: 40 / (2+1) = 13.33
	expectedWeightedAvg := 40.0 / 3.0
	require.Equal(t, expectedWeightedAvg, weighted.Avg(), "weighted average")

	// Non-weighted average: 30 / 2 = 15.0
	require.Equal(t, 15.0, nonWeighted.Avg(), "non-weighted average")
}

func TestTimeSeriesAddWithWeight(t *testing.T) {
	// Test that Add() method works on weighted series (should use weight=1)
	ts := NewFloatTimeSeries(time.Hour)

	ts.Add(10.0) // Should be equivalent to AddWeighted(10.0, 1)
	ts.AddWeighted(20.0, 2)

	// Sum: (10*1) + (20*2) = 50
	require.Equal(t, 50.0, ts.Sum(), "mixed Add and AddWeighted sum")

	// Average: 50 / (1+2) = 16.67
	expectedAvg := 50.0 / 3.0
	require.Equal(t, expectedAvg, ts.Avg(), "mixed Add and AddWeighted average")
}
