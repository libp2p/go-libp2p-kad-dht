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

// NextNonEmptyLeaf returns the leaf following the provided key `k` in the trie
// according to the provided `order`.
func NextNonEmptyLeaf[K0 kad.Key[K0], K1 kad.Key[K1], D any](t *trie.Trie[K0, D], k K0, order K1) *trie.Entry[K0, D] {
	return nextNonEmptyLeafAtDepth(t, k, order, 0, false)
}

// nextNonEmptyLeafAtDepth is a recursive function that finds the non empty
// leaf right after the supplied key `k`.
//
// We first need to go down the trie until we find the supplied `k` (if it
// exists). Once we have found the key, or its closest neighbor (if it doesn't
// exist) we hit the bottom of the trie. Then we go back up in the trie until
// we are able to go deeper again following `order` until we fall on a
// non-empty leaf. This is the leaf we are looking for.
func nextNonEmptyLeafAtDepth[K0 kad.Key[K0], K1 kad.Key[K1], D any](t *trie.Trie[K0, D], k K0, order K1, depth int, hitBottom bool) *trie.Entry[K0, D] {
	if hitBottom {
		if t.IsNonEmptyLeaf() {
			// Found the next non-empty leaf.
			return &trie.Entry[K0, D]{Key: *t.Key(), Data: t.Data()}
		}
		if t.IsEmptyLeaf() {
			return nil
		}
		// Going down the trie, looking for next non-empty leaf according to order.
		orderBit := int(order.Bit(depth))
		nextLeaf := nextNonEmptyLeafAtDepth(t.Branch(orderBit), k, order, depth+1, true)
		if nextLeaf != nil {
			return nextLeaf
		}
		return nextNonEmptyLeafAtDepth(t.Branch(1-orderBit), k, order, depth+1, true)
	}

	if t.IsLeaf() {
		// We have reached the bottom of the trie at k or its closest leaf
		if t.HasKey() {
			if depth == 0 {
				// Depth is 0, meaning there is a single key in the trie.
				return &trie.Entry[K0, D]{Key: *t.Key(), Data: t.Data()}
			}
			cpl := k.CommonPrefixLength(*t.Key())
			if cpl < k.BitLen() && cpl < order.BitLen() && order.Bit(cpl) == k.Bit(cpl) {
				// k is closer to order than t.Key, so t.Key AFTER k, return it
				return &trie.Entry[K0, D]{Key: *t.Key(), Data: t.Data()}
			}
		}
		return nil
	}
	kBit := int(k.Bit(depth))
	// Recursive call until we hit the bottom of the trie.
	nextLeaf := nextNonEmptyLeafAtDepth(t.Branch(kBit), k, order, depth+1, false)
	if nextLeaf != nil {
		// Branch has found the next leaf, return it.
		return nextLeaf
	}
	orderBit := int(order.Bit(depth))
	if kBit == orderBit || depth == 0 {
		// Neighbor branch is up next, according to order.
		nextLeaf = nextNonEmptyLeafAtDepth(t.Branch(1-kBit), k, order, depth+1, true)
		if nextLeaf != nil {
			return nextLeaf
		}
		if depth == 0 {
			// We have reached the end of the trie, start again from the first leaf.
			return nextNonEmptyLeafAtDepth(t.Branch(kBit), k, order, depth+1, true)
		}
	}
	// Next leaf not found, signal it to parent by returning an empty entry.
	return nil
}

// PruneSubtrie removes the subtrie at the given key `k` from the trie `t` if
// it exists.
//
// All keys starting with the prefix `k` are purged from the trie.
func PruneSubtrie[K0 kad.Key[K0], K1 kad.Key[K1], D any](t *trie.Trie[K0, D], k K1) {
	pruneSubtrieAtDepth(t, k, 0)
}

func pruneSubtrieAtDepth[K0 kad.Key[K0], K1 kad.Key[K1], D any](t *trie.Trie[K0, D], k K1, depth int) bool {
	if t.IsLeaf() {
		if t.HasKey() && IsPrefix(k, *t.Key()) {
			*t = trie.Trie[K0, D]{}
			return true
		}
		return false
	}

	// Not a leaf, continue pruning branches.
	if depth == k.BitLen() {
		*t = trie.Trie[K0, D]{}
		return true
	}

	pruned := pruneSubtrieAtDepth(t.Branch(int(k.Bit(depth))), k, depth+1)
	if pruned && t.Branch(1-int(k.Bit(depth))).IsEmptyLeaf() {
		*t = trie.Trie[K0, D]{}
		return true
	}
	return false
}
