package xortrie

// Intersect computes the intersection of the keys in p and q.
// p and q must be non-nil. The returned trie is never nil.
func Intersect(p, q *XorTrie) *XorTrie {
	return intersect(0, p, q)
}

func intersect(depth int, p, q *XorTrie) *XorTrie {
	switch {
	case p.isLeaf() && q.isLeaf():
		if p.isEmpty() || q.isEmpty() {
			return &XorTrie{} // empty set
		} else {
			if TrieKeyEqual(p.key, q.key) {
				return &XorTrie{key: p.key} // singleton
			} else {
				return &XorTrie{} // empty set
			}
		}
	case p.isLeaf() && !q.isLeaf():
		if p.isEmpty() {
			return &XorTrie{} // empty set
		} else {
			if _, found := q.find(depth, p.key); found {
				return &XorTrie{key: p.key}
			} else {
				return &XorTrie{} // empty set
			}
		}
	case !p.isLeaf() && q.isLeaf():
		return Intersect(q, p)
	case !p.isLeaf() && !q.isLeaf():
		disjointUnion := &XorTrie{
			branch: [2]*XorTrie{
				intersect(depth+1, p.branch[0], q.branch[0]),
				intersect(depth+1, p.branch[1], q.branch[1]),
			},
		}
		disjointUnion.shrink()
		return disjointUnion
	}
	panic("unreachable")
}
