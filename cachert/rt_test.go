package cachert

import (
	"bytes"
	"testing"
	"time"
)

func TestRT_InsertRangeAndGetRanges(t *testing.T) {
	rt := NewRT()

	// Valid 32-byte keys
	key1 := bytes.Repeat([]byte("a"), 32)
	key2 := bytes.Repeat([]byte("b"), 32)
	key3 := bytes.Repeat([]byte("c"), 32)
	key4 := bytes.Repeat([]byte("d"), 32)

	// Insert non-overlapping range
	rt.InsertRange(string(key1), string(key2), time.Now().Add(1*time.Hour))
	ranges := rt.GetRanges()
	if len(ranges) != 1 {
		t.Fatalf("expected 1 range, got %d", len(ranges))
	}

	// Insert overlapping range
	rt.InsertRange(string(key2), string(key3), time.Now().Add(2*time.Hour))
	ranges = rt.GetRanges()
	if len(ranges) != 2 {
		t.Fatalf("expected 2 ranges, got %d", len(ranges))
	}

	// Insert range that merges with existing ranges
	rt.InsertRange(string(key1), string(key4), time.Now().Add(3*time.Hour))
	ranges = rt.GetRanges()
	if len(ranges) != 1 {
		t.Fatalf("expected 1 merged range, got %d", len(ranges))
	}
	if ranges[0].KeyLower != string(key1) || ranges[0].KeyUpper != string(key4) {
		t.Fatalf("merged range has incorrect bounds: %+v", ranges[0])
	}
}

func TestRT_RangeIsCovered(t *testing.T) {
	rt := NewRT()

	// Valid 32-byte keys
	key1 := bytes.Repeat([]byte("a"), 32)
	key2 := bytes.Repeat([]byte("b"), 32)
	key3 := bytes.Repeat([]byte("c"), 32)
	key4 := bytes.Repeat([]byte("d"), 32)

	// Insert ranges
	rt.InsertRange(string(key1), string(key2), time.Now().Add(1*time.Hour))
	rt.InsertRange(string(key2), string(key3), time.Now().Add(2*time.Hour))

	// Check covered range
	if !rt.RangeIsCovered(string(key1), string(key3)) {
		t.Fatalf("expected range [%s, %s] to be covered", key1, key3)
	}

	// Check partially covered range
	if rt.RangeIsCovered(string(key1), string(key4)) {
		t.Fatalf("expected range [%s, %s] to not be fully covered", key1, key4)
	}

	// Check uncovered range
	if rt.RangeIsCovered(string(key3), string(key4)) {
		t.Fatalf("expected range [%s, %s] to not be covered", key3, key4)
	}
}

func TestRT_CollectGarbage(t *testing.T) {
	rt := NewRT()

	// Valid 32-byte keys
	key1 := bytes.Repeat([]byte("a"), 32)
	key2 := bytes.Repeat([]byte("b"), 32)

	// Insert range with expiration
	expiredTime := time.Now().Add(-1 * time.Hour)
	rt.InsertRange(string(key1), string(key2), expiredTime)

	// Collect garbage
	rt.CollectGarbage(time.Now())

	// Validate range is removed
	ranges := rt.GetRanges()
	if len(ranges) != 0 {
		t.Fatalf("expected 0 ranges after garbage collection, got %d", len(ranges))
	}
}
