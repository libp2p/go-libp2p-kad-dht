package triert

import "github.com/plprobelab/go-libdht/kad"

// KeyFilterFunc is a function that is applied before a key is added to the table.
// Return false to prevent the key from being added.
type KeyFilterFunc[K kad.Key[K], N kad.NodeID[K]] func(rt *TrieRT[K, N], kk K) bool

// BucketLimit20 is a filter function that limits the occupancy of buckets in the table to 20 keys.
func BucketLimit20[K kad.Key[K], N kad.NodeID[K]](rt *TrieRT[K, N], kk K) bool {
	cpl := rt.Cpl(kk)
	return rt.CplSize(cpl) < 20
}

// NodeFilter provides a stateful way to filter nodes before they are added to
// the table. The filter is applied after the key filter.
type NodeFilter[K kad.Key[K], N kad.NodeID[K]] interface {
	// TryAdd is called when a node is added to the table. Return true to allow
	// the node to be added. Return false to prevent the node from being added
	// to the table. When updating its state, the NodeFilter considers that the
	// node has been added to the table if TryAdd returns true.
	TryAdd(rt *TrieRT[K, N], node N) bool
	// Remove is called when a node is removed from the table, allowing the
	// filter to update its state.
	Remove(node N)
}
