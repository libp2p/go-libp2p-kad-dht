package query

import (
	"context"

	"github.com/plprobelab/go-kademlia/kad"
	"github.com/plprobelab/go-kademlia/key"
	"github.com/plprobelab/go-kademlia/key/trie"
)

// A NodeIter iterates nodes according to some strategy.
type NodeIter[K kad.Key[K]] interface {
	// Add adds node information to the iterator
	Add(*NodeStatus[K])

	// Find returns the node information corresponding to the given Kademlia key
	Find(K) (*NodeStatus[K], bool)

	// Each applies fn to each entry in the iterator in order. Each stops and returns true if fn returns true.
	// Otherwise Each returns false when there are no further entries.
	Each(ctx context.Context, fn func(context.Context, *NodeStatus[K]) bool) bool
}

// A ClosestNodesIter iterates nodes in order of ascending distance from a key.
type ClosestNodesIter[K kad.Key[K]] struct {
	// target is the key whose distance to a node determines the position of that node in the iterator.
	target K

	// nodelist holds the nodes discovered so far, ordered by increasing distance from the target.
	nodes *trie.Trie[K, *NodeStatus[K]]
}

var _ NodeIter[key.Key8] = (*ClosestNodesIter[key.Key8])(nil)

// NewClosestNodesIter creates a new ClosestNodesIter
func NewClosestNodesIter[K kad.Key[K]](target K) *ClosestNodesIter[K] {
	return &ClosestNodesIter[K]{
		target: target,
		nodes:  trie.New[K, *NodeStatus[K]](),
	}
}

func (iter *ClosestNodesIter[K]) Add(ni *NodeStatus[K]) {
	iter.nodes.Add(ni.NodeID.Key(), ni)
}

func (iter *ClosestNodesIter[K]) Find(k K) (*NodeStatus[K], bool) {
	found, ni := trie.Find(iter.nodes, k)
	return ni, found
}

func (iter *ClosestNodesIter[K]) Each(ctx context.Context, fn func(context.Context, *NodeStatus[K]) bool) bool {
	// get all the nodes in order of distance from the target
	// TODO: turn this into a walk or iterator on trie.Trie
	entries := trie.Closest(iter.nodes, iter.target, iter.nodes.Size())
	for _, e := range entries {
		ni := e.Data
		if fn(ctx, ni) {
			return true
		}
	}
	return false
}

// A SequentialIter iterates nodes in the order they were added to the iterator.
type SequentialIter[K kad.Key[K]] struct {
	// nodelist holds the nodes discovered so far, ordered by increasing distance from the target.
	nodes []*NodeStatus[K]
}

var _ NodeIter[key.Key8] = (*SequentialIter[key.Key8])(nil)

// NewSequentialIter creates a new SequentialIter
func NewSequentialIter[K kad.Key[K]]() *SequentialIter[K] {
	return &SequentialIter[K]{
		nodes: make([]*NodeStatus[K], 0),
	}
}

func (iter *SequentialIter[K]) Add(ni *NodeStatus[K]) {
	iter.nodes = append(iter.nodes, ni)
}

// Find returns the node information corresponding to the given Kademlia key. It uses a linear
// search which makes it unsuitable for large numbers of entries.
func (iter *SequentialIter[K]) Find(k K) (*NodeStatus[K], bool) {
	for i := range iter.nodes {
		if key.Equal(k, iter.nodes[i].NodeID.Key()) {
			return iter.nodes[i], true
		}
	}

	return nil, false
}

func (iter *SequentialIter[K]) Each(ctx context.Context, fn func(context.Context, *NodeStatus[K]) bool) bool {
	for _, ns := range iter.nodes {
		if fn(ctx, ns) {
			return true
		}
	}
	return false
}
