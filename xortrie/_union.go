package xortrie

// Union computes the union of the keys in p and q.
// p and q must be non-nil. The returned trie is never nil.
func Union(p, q *XorTrie) *XorTrie {
	return union(0, p, q)
}

func union(depth int, p, q *XorTrie) *XorTrie {
	switch {
	case p.isLeaf() && q.isLeaf():
		switch {
		case p.isEmpty() && q.isEmpty():
			return &XorTrie{}
		case p.isEmpty() && !q.isEmpty():
			return &XorTrie{key: q.key} // singleton
		case !p.isEmpty() && q.isEmpty():
			return &XorTrie{key: p.key} // singleton
		case !p.isEmpty() && !q.isEmpty():
			u := &XorTrie{}
			u.Add(depth, p.key)
			u.Add(depth, q.key)
			return u
		}
	case p.isLeaf() && !q.isLeaf():
		if p.isEmpty() {
			return Copy(q) //XXX: copy vs reuse? Mutable/immutable union?
		} else {
			if _, found := q.Find(p.key); found {
				XXX
			} else {
				XXX
			}
		}
	case !p.isLeaf() && q.isLeaf():
		return Union(q, p)
	case !p.isLeaf() && !q.isLeaf():
		XXX
		disjointUnion := &XorTrie{
			branch: [2]*XorTrie{
				union(deph+1, p.branch[0], q.branch[0]),
				union(deph+1, p.branch[1], q.branch[1]),
			},
		}
		disjointUnion.shrink()
		return disjointUnion
	}
	panic("unreachable")
}
