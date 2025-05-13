package cachert

import (
	"time"

	"github.com/emirpasic/gods/v2/trees/avltree"
)

type Key = string

type KeySearchRange struct {
	KeyLower Key
	KeyUpper Key
	Time     time.Time
}

type RT struct {
	at     *avltree.Tree[Key, *KeySearchRange]
	ranges map[*KeySearchRange]struct{}
}

func NewRT() *RT {
	return &RT{
		at:     avltree.New[Key, *KeySearchRange](),
		ranges: make(map[*KeySearchRange]struct{}),
	}
}

func (t *RT) GetRanges() []KeySearchRange {
	ranges := make([]KeySearchRange, 0, len(t.ranges))
	for k := range t.ranges {
		if k.KeyLower > k.KeyUpper {
			panic("lower key must be less than upper key")
		}
		ranges = append(ranges, KeySearchRange{
			KeyLower: k.KeyLower,
			KeyUpper: k.KeyUpper,
			Time:     k.Time,
		})
	}
	return ranges
}

func (t *RT) InsertRange(lower, upper Key, expiration time.Time) {
	if lower > upper {
		panic("lower key must be less than upper key")
	}
	if len([]byte(lower)) != len([]byte(upper)) && len([]byte(lower)) != 32 {
		panic("lower and upper keys must be 32 bytes")
	}

	r := &KeySearchRange{lower, upper, expiration}

	// Find the range that starts the latest, but before the start of this range
	f, ok := t.at.Floor(r.KeyLower)
	// If there are no nodes that start before this one
	type rr struct {
		k Key
		r *KeySearchRange
	}

	var rangesToReplace []rr

	if !ok {
		f = t.at.Left()
	} else {
		if f.Value.KeyUpper > r.KeyLower {
			// Truncate the previous range to stop where this one starts
			f.Value.KeyUpper = r.KeyLower
			rangesToReplace = append(rangesToReplace, rr{k: f.Key, r: f.Value})
		}
	}
	// Insert this one
	t.at.Put(r.KeyLower, r)
	t.ranges[r] = struct{}{}

	// Continue through subsequent ranges seeing if any get clobbered by this one
	for f := f.Next(); f != nil; f = f.Next() {
		if f.Value == r {
			// This is the same range, so we can skip it
			continue
		}
		if f.Value.KeyLower < r.KeyUpper {
			if f.Value.KeyUpper <= r.KeyUpper {
				rangesToReplace = append(rangesToReplace, rr{k: f.Key, r: nil})
				delete(t.ranges, f.Value)
			} else {
				f.Value.KeyUpper = r.KeyUpper
				rangesToReplace = append(rangesToReplace, rr{k: f.Key, r: f.Value})
			}
		} else {
			break
		}
	}

	for _, rng := range rangesToReplace {
		t.at.Remove(rng.k)
		if rng.r == nil {
			continue
		}
		if rng.r.KeyLower == rng.r.KeyUpper {
			// If the range is empty, we don't want to keep it
			delete(t.ranges, rng.r)
		} else {
			t.at.Put(rng.r.KeyLower, rng.r)
		}
	}
}

func (t *RT) RangeIsCovered(lower, upper Key) bool {
	if lower > upper {
		panic("lower key must be less than upper key")
	}
	if len([]byte(lower)) != len([]byte(upper)) && len([]byte(lower)) != 32 {
		panic("lower and upper keys must be 32 bytes")
	}

	lowest := lower
	f, ok := t.at.Floor(lowest)
	if !ok {
		return false
	}

	for ; f != nil; f = f.Next() {
		if f.Value.KeyLower > lowest {
			return false
		}

		if f.Value.KeyUpper >= upper {
			return true
		}

		lowest = f.Value.KeyUpper
	}
	return false
}

func (t *RT) CollectGarbage(ti time.Time) {
	for r, _ := range t.ranges {
		if r.Time.Before(ti) {
			t.at.Remove(r.KeyLower)
			delete(t.ranges, r)
		}
	}
}
