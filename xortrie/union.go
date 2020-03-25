package xortrie

// Union computes the union of the keys in p and q.
// p and q must be non-nil. The returned trie is never nil.
func Union(p, q *XorTrie) *XorTrie {
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
			u.Add(p.key)
			u.Add(q.key)
			return u
		}
	case p.isLeaf() && !q.isLeaf():
		XXX //XXX
		if p.isEmpty() {
			XXX
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
				Intersect(p.branch[0], q.branch[0]),
				Intersect(p.branch[1], q.branch[1]),
			},
		}
		disjointUnion.shrink()
		return disjointUnion
	}
	panic("unreachable")
}
