package xortrie

import "testing"

func TestIntersectRandom(t *testing.T) {
	for _, s := range testIntersectSamples {
		testIntersect(t, s)
	}
}

func testIntersect(t *testing.T, sample *testIntersectSample) {
	left, right, expected := NewXorTrie(), NewXorTrie(), NewXorTrie()
	for _, l := range sample.LeftKeys {
		left.Add(l)
	}
	for _, r := range sample.RightKeys {
		right.Add(r)
	}
	for _, s := range setIntersect(sample.LeftKeys, sample.RightKeys) {
		expected.Add(s)
	}
	got := Intersect(left, right)
	if !XorTrieEqual(expected, got) {
		t.Errorf("intersection of %v and %v: expected %v, got %v",
			sample.LeftKeys, sample.RightKeys, expected, got)
	}
}

func setIntersect(left, right []TrieKey) []TrieKey {
	intersection := []TrieKey{}
	for _, l := range left {
		for _, r := range right {
			if TrieKeyEqual(l, r) {
				intersection = append(intersection, r)
			}
		}
	}
	return intersection
}

type testIntersectSample struct {
	LeftKeys  []TrieKey
	RightKeys []TrieKey
}

var testIntersectSamples = []*testIntersectSample{
	{
		LeftKeys:  []TrieKey{{1, 2, 3}},
		RightKeys: []TrieKey{{1, 3, 5}},
	},
	{
		LeftKeys:  []TrieKey{{1, 2, 3, 4, 5, 6}},
		RightKeys: []TrieKey{{3, 5, 7}},
	},
	{
		LeftKeys:  []TrieKey{{23, 3, 7, 13, 17}},
		RightKeys: []TrieKey{{2, 11, 17, 19, 23}},
	},
}
