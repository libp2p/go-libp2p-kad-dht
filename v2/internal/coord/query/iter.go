package query

import (
	"context"

	"github.com/plprobelab/go-kademlia/kad"
	"github.com/plprobelab/go-kademlia/key"
	"github.com/plprobelab/go-kademlia/key/trie"
)

// A NodeIter iterates nodes according to some strategy.
type NodeIter[K kad.Key[K], N kad.NodeID[K]] interface {
	// Add adds node information to the iterator
	Add(*NodeStatus[K, N])

	// Find returns the node information corresponding to the given Kademlia key
	Find(K) (*NodeStatus[K, N], bool)

	// Each applies fn to each entry in the iterator in order. Each stops and returns true if fn returns true.
	// Otherwise, Each returns false when there are no further entries.
	Each(ctx context.Context, fn func(context.Context, *NodeStatus[K, N]) bool) bool
}

// A ClosestNodesIter iterates nodes in order of ascending distance from a key.
type ClosestNodesIter[K kad.Key[K], N kad.NodeID[K]] struct {
	// target is the key whose distance to a node determines the position of that node in the iterator.
	target K

	// nodelist holds the nodes discovered so far, ordered by increasing distance from the target.
	nodes *trie.Trie[K, *NodeStatus[K, N]]
}

// NewClosestNodesIter creates a new ClosestNodesIter
func NewClosestNodesIter[K kad.Key[K], N kad.NodeID[K]](target K) *ClosestNodesIter[K, N] {
	return &ClosestNodesIter[K, N]{
		target: target,
		nodes:  trie.New[K, *NodeStatus[K, N]](),
	}
}

func (iter *ClosestNodesIter[K, N]) Add(ni *NodeStatus[K, N]) {
	iter.nodes.Add(ni.NodeID.Key(), ni)
}

func (iter *ClosestNodesIter[K, N]) Find(k K) (*NodeStatus[K, N], bool) {
	found, ni := trie.Find(iter.nodes, k)
	return ni, found
}

func (iter *ClosestNodesIter[K, N]) Each(ctx context.Context, fn func(context.Context, *NodeStatus[K, N]) bool) bool {
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
type SequentialIter[K kad.Key[K], N kad.NodeID[K]] struct {
	// nodelist holds the nodes discovered so far, ordered by increasing distance from the target.
	nodes []*NodeStatus[K, N]
}

// NewSequentialIter creates a new SequentialIter
func NewSequentialIter[K kad.Key[K], N kad.NodeID[K]]() *SequentialIter[K, N] {
	return &SequentialIter[K, N]{
		nodes: make([]*NodeStatus[K, N], 0),
	}
}

func (iter *SequentialIter[K, N]) Add(ni *NodeStatus[K, N]) {
	iter.nodes = append(iter.nodes, ni)
}

// Find returns the node information corresponding to the given Kademlia key. It uses a linear
// search which makes it unsuitable for large numbers of entries.
func (iter *SequentialIter[K, N]) Find(k K) (*NodeStatus[K, N], bool) {
	for i := range iter.nodes {
		if key.Equal(k, iter.nodes[i].NodeID.Key()) {
			return iter.nodes[i], true
		}
	}

	return nil, false
}

func (iter *SequentialIter[K, N]) Each(ctx context.Context, fn func(context.Context, *NodeStatus[K, N]) bool) bool {
	for _, ns := range iter.nodes {
		if fn(ctx, ns) {
			return true
		}
	}
	return false
}
