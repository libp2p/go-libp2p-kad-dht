package helpers

import (
	"github.com/probe-lab/go-libdht/kad"
	"github.com/probe-lab/go-libdht/kad/key"
	"github.com/probe-lab/go-libdht/kad/trie"
)

// AllEntries returns all entries (key + value) stored in the trie `t` sorted
// by their keys in the supplied `order`.
func AllEntries[K0 kad.Key[K0], K1 kad.Key[K1], D any](t *trie.Trie[K0, D], order K1) []*trie.Entry[K0, D] {
	return allEntriesAtDepth(t, order, 0)
}

func allEntriesAtDepth[K0 kad.Key[K0], K1 kad.Key[K1], D any](t *trie.Trie[K0, D], order K1, depth int) []*trie.Entry[K0, D] {
	if t == nil || t.IsEmptyLeaf() {
		return nil
	}
	if t.IsNonEmptyLeaf() {
		return []*trie.Entry[K0, D]{{Key: *t.Key(), Data: t.Data()}}
	}
	b := int(order.Bit(depth))
	return append(allEntriesAtDepth(t.Branch(b), order, depth+1),
		allEntriesAtDepth(t.Branch(1-b), order, depth+1)...)
}

// AllValues returns all values stored in the trie `t` sorted by their keys in
// the supplied `order`.
func AllValues[K0 kad.Key[K0], K1 kad.Key[K1], D any](t *trie.Trie[K0, D], order K1) []D {
	entries := AllEntries(t, order)
	out := make([]D, len(entries))
	for i, entry := range entries {
		out[i] = entry.Data
	}
	return out
}

// FindPrefixOfKey checks whether the trie contains a leave whose key is a
// prefix or exact match of `k`.
//
// If there is a match, the function returns the matching key and true.
// Otherwise it returns the zero key and false.
func FindPrefixOfKey[K0 kad.Key[K0], K1 kad.Key[K1], D any](t *trie.Trie[K0, D], k K1) (K0, bool) {
	return findPrefixOfKeyAtDepth(t, k, 0)
}

func findPrefixOfKeyAtDepth[K0 kad.Key[K0], K1 kad.Key[K1], D any](t *trie.Trie[K0, D], k K1, depth int) (K0, bool) {
	if t.IsLeaf() {
		if !t.HasKey() {
			var zero K0
			return zero, false
		}
		return *t.Key(), key.CommonPrefixLength(*t.Key(), k) == (*t.Key()).BitLen()
	}
	b := int(k.Bit(depth))
	return findPrefixOfKeyAtDepth(t.Branch(b), k, depth+1)
}

// FindSubtrie returns the potential subtrie of `t` that matches the prefix
// `k`, and true if there was a match and false otherwise.
func FindSubtrie[K0 kad.Key[K0], K1 kad.Key[K1], D any](t *trie.Trie[K0, D], k K1) (*trie.Trie[K0, D], bool) {
	if t.IsEmptyLeaf() {
		return t, false
	}
	branch := t
	for i := range k.BitLen() {
		if branch.IsEmptyLeaf() {
			return t, false
		}
		if branch.IsNonEmptyLeaf() {
			return branch, key.CommonPrefixLength(*branch.Key(), k) == k.BitLen()
		}
		branch = branch.Branch(int(k.Bit(i)))
	}
	return branch, !branch.IsEmptyLeaf()
}
