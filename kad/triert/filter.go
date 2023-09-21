package triert

import "github.com/libp2p/go-libdht/kad"

// KeyFilterFunc is a function that is applied before a key is added to the table.
// Return false to prevent the key from being added.
type KeyFilterFunc[K kad.Key[K], N kad.NodeID[K]] func(rt *TrieRT[K, N], kk K) bool

// BucketLimit20 is a filter function that limits the occupancy of buckets in the table to 20 keys.
func BucketLimit20[K kad.Key[K], N kad.NodeID[K]](rt *TrieRT[K, N], kk K) bool {
	cpl := rt.Cpl(kk)
	return rt.CplSize(cpl) < 20
}
