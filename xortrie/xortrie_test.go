package xortrie

import "testing"

func TestInsertRemove(t *testing.T) {
	r := NewXorTrie()
	testSeq(r, t)
	testSeq(r, t)
}

func testSeq(r *XorTrie, t *testing.T) {
	for _, s := range testInsertSeq {
		depth, _ := r.Add(TrieKey(s.key))
		if depth != s.insertedDepth {
			t.Errorf("inserting expected %d, got %d", s.insertedDepth, depth)
		}
	}
	for _, s := range testRemoveSeq {
		depth, _ := r.Remove(TrieKey(s.key))
		if depth != s.reachedDepth {
			t.Errorf("removing expected %d, got %d", s.reachedDepth, depth)
		}
	}
}

var testInsertSeq = []struct {
	key           []byte
	insertedDepth int
}{
	{key: []byte{0x0}, insertedDepth: 0},
	{key: []byte{0x1}, insertedDepth: 1},
	{key: []byte{0x8}, insertedDepth: 4},
	{key: []byte{0x3}, insertedDepth: 2},
	{key: []byte{0x4}, insertedDepth: 3},
}

var testRemoveSeq = []struct {
	key          []byte
	reachedDepth int
}{
	{key: []byte{0x0}, reachedDepth: 4},
	{key: []byte{0x8}, reachedDepth: 3},
	{key: []byte{0x4}, reachedDepth: 1},
	{key: []byte{0x1}, reachedDepth: 2},
	{key: []byte{0x3}, reachedDepth: 0},
}
