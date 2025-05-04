package reprovider

import (
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/probe-lab/go-libdht/kad"
	"github.com/probe-lab/go-libdht/kad/key"
	"github.com/probe-lab/go-libdht/kad/key/bit256"
	"github.com/probe-lab/go-libdht/kad/key/bitstr"
	"github.com/probe-lab/go-libdht/kad/trie"
)

// returns the list of all non-overlapping subtries of `t` having more than
// `size` elements, sorted according to `order`. every element is included in
// exactly one region.
func extractMinimalRegions(t *trie.Trie[bit256.Key, peer.ID], path bitstr.Key, size int, order bit256.Key) []region {
	if t.IsEmptyLeaf() {
		return nil
	}
	if t.Branch(0).Size() > size && t.Branch(1).Size() > size {
		b := int(order.Bit(len(path)))
		return append(extractMinimalRegions(t.Branch(b), path+bitstr.Key(rune('0'+b)), size, order),
			extractMinimalRegions(t.Branch(1-b), path+bitstr.Key(rune('1'-b)), size, order)...)
	}
	return []region{{prefix: path, peers: t}}
}

// trieHasPrefixOfKey checks if the trie contains a leave whose key is a prefix
// (or a match) of the provided k
func trieHasPrefixOfKey[K0 kad.Key[K0], K1 kad.Key[K1], D any](t *trie.Trie[K0, D], k K1) bool {
	return trieHasPrefixOfKeyAtDepth(t, k, 0)
}

func trieHasPrefixOfKeyAtDepth[K0 kad.Key[K0], K1 kad.Key[K1], D any](t *trie.Trie[K0, D], k K1, depth int) bool {
	if t.IsLeaf() {
		if !t.HasKey() {
			return false
		}
		return key.CommonPrefixLength(*t.Key(), k) == (*t.Key()).BitLen()
	}
	b := int(k.Bit(depth))
	return trieHasPrefixOfKeyAtDepth(t.Branch(b), k, depth+1)
}

// nextNonEmptyLeaf returns the leaf right after the provided key `k` in the
// trie according to the provided `order`.
func nextNonEmptyLeaf[K0 kad.Key[K0], K1 kad.Key[K1], D any](t *trie.Trie[K0, D], k K0, order K1) trie.Entry[K0, D] {
	return nextNonEmptyLeafAtDepth(t, k, order, 0, false)
}

func nextNonEmptyLeafAtDepth[K0 kad.Key[K0], K1 kad.Key[K1], D any](t *trie.Trie[K0, D], k K0, order K1, depth int, hitBottom bool) trie.Entry[K0, D] {
	if hitBottom {
		if t.IsNonEmptyLeaf() {
			// Found the next non-empty leaf.
			return trie.Entry[K0, D]{Key: *t.Key(), Data: t.Data()}
		}
		// Going down the trie, looking for next non-empty leaf according to order.
		orderBit := int(order.Bit(depth))
		return nextNonEmptyLeafAtDepth(t.Branch(orderBit), k, order, depth+1, true)
	}

	if t.IsLeaf() {
		// We have reached the bottom of the trie at k or its closest leaf
		if t.HasKey() {
			cpl := k.CommonPrefixLength(*t.Key())
			if cpl < k.BitLen() && cpl < order.BitLen() && order.Bit(cpl) == k.Bit(cpl) {
				// k is closer to order than t.Key, so t.Key AFTER k, return it
				return trie.Entry[K0, D]{Key: *t.Key(), Data: t.Data()}
			}
		}
		return trie.Entry[K0, D]{}
	}
	kBit := int(k.Bit(depth))
	// Recursive call until we hit the bottom of the trie.
	nextLeaf := nextNonEmptyLeafAtDepth(t.Branch(kBit), k, order, depth+1, false)
	if nextLeaf.Key.BitLen() > 0 {
		// Branch has found the next leaf, return it.
		return nextLeaf
	}
	orderBit := int(order.Bit(depth))
	if kBit == orderBit || depth == 0 {
		// Neighbor branch is up next, according to order.
		return nextNonEmptyLeafAtDepth(t.Branch(1-kBit), k, order, depth+1, true)
	}
	// Next leaf not found, signal it to parent by returning an empty entry.
	return trie.Entry[K0, D]{}
}

// allKeys returns a slice containing all keys in the trie `t` sorted according
// to the provided `order`.
func allKeys[K0 kad.Key[K0], K1 kad.Key[K1], D any](t *trie.Trie[K0, D], order K1) []trie.Entry[K0, D] {
	return allKeysAtDepth(t, order, 0)
}

func allKeysAtDepth[K0 kad.Key[K0], K1 kad.Key[K1], D any](t *trie.Trie[K0, D], order K1, depth int) []trie.Entry[K0, D] {
	if t.IsEmptyLeaf() {
		return nil
	}
	if t.IsNonEmptyLeaf() {
		return []trie.Entry[K0, D]{{Key: *t.Key(), Data: t.Data()}}
	}
	b := int(order.Bit(depth))
	return append(allKeysAtDepth(t.Branch(b), order, depth+1),
		allKeysAtDepth(t.Branch(1-b), order, depth+1)...)
}
