package provider

import (
	"github.com/libp2p/go-libp2p/core/peer"
	mh "github.com/multiformats/go-multihash"
	"github.com/probe-lab/go-libdht/kad"
	"github.com/probe-lab/go-libdht/kad/key"
	"github.com/probe-lab/go-libdht/kad/key/bit256"
	"github.com/probe-lab/go-libdht/kad/key/bitstr"
	"github.com/probe-lab/go-libdht/kad/trie"
)

type region struct {
	prefix bitstr.Key
	peers  *trie.Trie[bit256.Key, peer.ID]
	keys   *trie.Trie[bit256.Key, mh.Multihash]
}

// returns the list of all non-overlapping subtries of `t` having more than
// `size` elements, sorted according to `order`. every element is included in
// exactly one region.
func extractMinimalRegions(t *trie.Trie[bit256.Key, peer.ID], path bitstr.Key, size int, order bit256.Key) []region {
	if t.IsEmptyLeaf() {
		return nil
	}
	if t.Branch(0).Size() > size && t.Branch(1).Size() > size {
		b := int(order.Bit(len(path)))
		return append(extractMinimalRegions(t.Branch(b), path+bitstr.Key(byte('0'+b)), size, order),
			extractMinimalRegions(t.Branch(1-b), path+bitstr.Key(byte('1'-b)), size, order)...)
	}
	return []region{{prefix: path, peers: t}}
}

func assignKeysToRegions(regions []region, keys []mh.Multihash) []region {
	for i := range regions {
		regions[i].keys = trie.New[bit256.Key, mh.Multihash]()
	}
	for _, k := range keys {
		h := mhToBit256(k)
		for i, r := range regions {
			if isPrefix(r.prefix, h) {
				regions[i].keys.Add(h, k)
				break
			}
		}
	}
	return regions
}

// trieHasPrefixOfKey checks if the trie contains a leave whose key is a prefix
// (or a match) of the provided k
func trieHasPrefixOfKey[K0 kad.Key[K0], K1 kad.Key[K1], D any](t *trie.Trie[K0, D], k K1) (bool, K0) {
	return trieHasPrefixOfKeyAtDepth(t, k, 0)
}

func trieHasPrefixOfKeyAtDepth[K0 kad.Key[K0], K1 kad.Key[K1], D any](t *trie.Trie[K0, D], k K1, depth int) (bool, K0) {
	if t.IsLeaf() {
		if !t.HasKey() {
			var zero K0
			return false, zero
		}
		return key.CommonPrefixLength(*t.Key(), k) == (*t.Key()).BitLen(), *t.Key()
	}
	b := int(k.Bit(depth))
	return trieHasPrefixOfKeyAtDepth(t.Branch(b), k, depth+1)
}

// nextNonEmptyLeaf returns the leaf right after the provided key `k` in the
// trie according to the provided `order`.
func nextNonEmptyLeaf[K0 kad.Key[K0], K1 kad.Key[K1], D any](t *trie.Trie[K0, D], k K0, order K1) *trie.Entry[K0, D] {
	return nextNonEmptyLeafAtDepth(t, k, order, 0, false)
}

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

func allValues[K0 kad.Key[K0], K1 kad.Key[K1], D any](t *trie.Trie[K0, D], order K1) []D {
	entries := allEntries(t, order)
	out := make([]D, len(entries))
	for i, entry := range entries {
		out[i] = entry.Data
	}
	return out
}

// allKeys returns a slice containing all keys in the trie `t` sorted according
// to the provided `order`.
func allEntries[K0 kad.Key[K0], K1 kad.Key[K1], D any](t *trie.Trie[K0, D], order K1) []*trie.Entry[K0, D] {
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

func subtrieMatchingPrefix[K0 kad.Key[K0], K1 kad.Key[K1], D any](t *trie.Trie[K0, D], k K1) (*trie.Trie[K0, D], bool) {
	if t.IsEmptyLeaf() {
		return t, false
	}
	branch := t
	for i := range k.BitLen() {
		if branch.IsEmptyLeaf() {
			return t, false
		}
		if branch.IsNonEmptyLeaf() {
			return t, key.CommonPrefixLength(*branch.Key(), k) == k.BitLen()
		}
		branch = branch.Branch(int(k.Bit(i)))
	}
	return branch, true
}

// mapInsert appends a slice of values to the map entry for the given key. If
// the key doesn't exist, it creates a new slice pre-sized to the length of the
// values being inserted to avoid multiple allocations.
func mapInsert[K comparable, V any](m map[K][]V, k K, vs []V) {
	if cur := m[k]; cur == nil {
		// pre-size once
		m[k] = append(make([]V, 0, len(vs)), vs...)
	} else {
		m[k] = append(cur, vs...)
	}
}

// mapMerge merges all key-value pairs from the source map into the destination
// map. Values from the source are appended to existing slices in the
// destination.
func mapMerge[K comparable, V any](dst, src map[K][]V) {
	for k1, vs1 := range src {
		mapInsert(dst, k1, vs1)
	}
}

// allocateToKClosest distributes items from the items trie to the k closest
// destinations in the dests trie based on XOR distance between their keys.
//
// The algorithm uses the trie structure to efficiently compute proximity
// without explicit distance calculations. Items are allocated to destinations
// by traversing both tries simultaneously and selecting the k destinations
// with the smallest XOR distance to each item's key.
//
// Returns a map where each destination value is associated with all items
// allocated to it. If k is 0 or either trie is empty, returns an empty map.
func allocateToKClosest[K kad.Key[K], V0 any, V1 comparable](items *trie.Trie[K, V0], dests *trie.Trie[K, V1], k int) map[V1][]V0 {
	return allocateToKClosestAtDepth(items, dests, k, 0)
}

// allocateToKClosestAtDepth performs the recursive allocation algorithm at a specific
// trie depth. At each depth, it processes both branches (0 and 1) of the trie,
// determining which destinations are closest to the items based on matching bit
// patterns at the current depth.
//
// The algorithm prioritizes destinations in the same branch as items (smaller XOR
// distance) and recursively processes deeper levels when more granular distance
// calculations are needed to select exactly k destinations.
//
// Parameters:
//   - items: trie containing items to be allocated
//   - dests: trie containing destination candidates
//   - k: maximum number of destinations to allocate each item to
//   - depth: current bit depth in the trie traversal
//
// Returns a map of destination values to their allocated items.
func allocateToKClosestAtDepth[K kad.Key[K], V0 any, V1 comparable](items *trie.Trie[K, V0], dests *trie.Trie[K, V1], k, depth int) map[V1][]V0 {
	m := make(map[V1][]V0)
	if k == 0 {
		return m
	}
	for i := range 2 {
		// Assign all items from branch i

		matchingItemsBranch := items.Branch(i)
		matchingItems := allValues(matchingItemsBranch, bit256.ZeroKey())
		if len(matchingItems) == 0 {
			if items.IsNonEmptyLeaf() && int((*items.Key()).Bit(depth)) == i {
				// items' current branch contains a single leaf
				matchingItems = []V0{items.Data()}
				matchingItemsBranch = items
			} else {
				// items' current branch is empty, skip it
				continue
			}
		}

		matchingDestsBranch := dests.Branch(i)
		otherDestsBranch := dests.Branch(1 - i)
		matchingDests := allValues(matchingDestsBranch, bit256.ZeroKey())
		otherDests := allValues(otherDestsBranch, bit256.ZeroKey())
		if dests.IsLeaf() {
			// Single key (leaf) in dests
			if dests.IsNonEmptyLeaf() {
				if int((*dests.Key()).Bit(depth)) == i {
					// Leaf matches current branch
					matchingDests = []V1{dests.Data()}
					matchingDestsBranch = dests
				} else {
					// Leaf matches other branch
					otherDests = []V1{dests.Data()}
					otherDestsBranch = dests
				}
			} else {
				// Empty leaf, no dests to allocate items.
				return m
			}
		}

		if nMatchingDests := len(matchingDests); nMatchingDests <= k {
			// Allocate matching items to the matching dests branch
			for _, dest := range matchingDests {
				mapInsert(m, dest, matchingItems)
			}
			if nMatchingDests == k || len(otherDests) == 0 {
				// Items were assigned to all k dests, or other branch is empty.
				continue
			}

			nMissingDests := k - nMatchingDests
			if len(otherDests) <= nMissingDests {
				// Other branch contains at most the missing number of dests to be
				// allocated to. Allocate matching items to the other dests branch.
				for _, dest := range otherDests {
					mapInsert(m, dest, matchingItems)
				}
			} else {
				// Other branch contains more than the missing number of dests, go one
				// level deeper to assign matching items to the closest dests.
				allocs := allocateToKClosestAtDepth(matchingItemsBranch, otherDestsBranch, nMissingDests, depth+1)
				mapMerge(m, allocs)
			}
		} else {
			// Number of matching dests is larger than k, go one level deeper.
			allocs := allocateToKClosestAtDepth(matchingItemsBranch, matchingDestsBranch, k, depth+1)
			mapMerge(m, allocs)
		}
	}
	return m
}
