package xortrie

// Intersect computes the intersection of the keys in p and q.
// p and q must be non-nil. The returned trie is never nil.
func Intersect(p, q *XorTrie) *XorTrie {
	switch {
	case p.isLeaf() && q.isLeaf():
		if p.isEmpty() || q.isEmpty() {
			return NewXorTrie() // empty set
		} else {
			if TrieKeyEqual(p.key, q.key) {
				return NewXorTrie().AddThen(p.key)
			} else {
				return NewXorTrie() // empty set
			}
		}
	case p.isLeaf() && !q.isLeaf():
		if p.isEmpty() {
			return NewXorTrie() // empty set
		} else {
			if _, found := q.Find(p.key); found {
				return NewXorTrie().AddThen(p.key)
			} else {
				return NewXorTrie() // empty set
			}
		}
	case !p.isLeaf() && q.isLeaf():
		return Intersect(q, p)
	case !p.isLeaf() && !q.isLeaf():
		return Union(Intersect(p.branch[0], q.branch[0]), Intersect(p.branch[1], q.branch[1]))
	}
	panic("unreachable")
}
