package xortrie

import "testing"

// Verify mutable and immutable add do the same thing.
func TestMutableAndImmutableAddSame(t *testing.T) {
	for _, s := range testAddSameSamples {
		mut := NewXorTrie()
		immut := NewXorTrie()
		for _, k := range s.Keys {
			mut.Add(k)
			immut = Add(immut, k)
		}
		if !XorTrieEqual(mut, immut) {
			t.Errorf("mutable trie %v differs from immutable trie %v", mut, immut)
		}
	}
}

type testAddSameSample struct {
	Keys []TrieKey
}

var testAddSameSamples = []*testAddSameSample{
	{Keys: []TrieKey{{1, 3, 5, 7, 11, 13}}},
}
