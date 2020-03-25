package xortrie

func XorTrieEqual(p, q *XorTrie) bool {
	switch {
	case p.isLeaf() && q.isLeaf():
		return TrieKeyEqual(p.key, q.key)
	case !p.isLeaf() && !q.isLeaf():
		return XorTrieEqual(p.branch[0], q.branch[0]) && XorTrieEqual(p.branch[1], q.branch[1])
	}
	return false
}
