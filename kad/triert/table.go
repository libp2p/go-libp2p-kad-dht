package triert

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/plprobelab/go-libdht/kad"
	"github.com/plprobelab/go-libdht/kad/kadtest"
	"github.com/plprobelab/go-libdht/kad/key/bit256"
	"github.com/plprobelab/go-libdht/kad/trie"
)

// TrieRT is a routing table backed by a XOR Trie which offers good scalablity and performance
// for large networks.
type TrieRT[K kad.Key[K], N kad.NodeID[K]] struct {
	self       K
	keyFilter  KeyFilterFunc[K, N]
	nodeFilter NodeFilter[K, N]

	mu   sync.Mutex   // held to synchronise mutations to the trie
	trie atomic.Value // holds a *trie.Trie[K, N]
}

var _ kad.RoutingTable[bit256.Key, kadtest.ID[bit256.Key]] = (*TrieRT[bit256.Key, kadtest.ID[bit256.Key]])(nil)

// New creates a new TrieRT using the supplied key as the local node's Kademlia key.
// If cfg is nil, the default config is used.
func New[K kad.Key[K], N kad.NodeID[K]](self N, cfg *Config[K, N]) (*TrieRT[K, N], error) {
	rt := &TrieRT[K, N]{
		self: self.Key(),
	}
	rt.trie.Store(&trie.Trie[K, N]{})

	if err := rt.apply(cfg); err != nil {
		return nil, fmt.Errorf("apply config: %w", err)
	}
	return rt, nil
}

func (rt *TrieRT[K, N]) apply(cfg *Config[K, N]) error {
	if cfg == nil {
		cfg = DefaultConfig[K, N]()
	}

	rt.keyFilter = cfg.KeyFilter
	rt.nodeFilter = cfg.NodeFilter

	return nil
}

// Self returns the local node's Kademlia key.
func (rt *TrieRT[K, N]) Self() K {
	return rt.self
}

// AddNode tries to add a node to the routing table.
func (rt *TrieRT[K, N]) AddNode(node N) bool {
	kk := node.Key()
	if rt.keyFilter != nil && !rt.keyFilter(rt, kk) {
		return false
	}
	if rt.nodeFilter != nil && !rt.nodeFilter.TryAdd(rt, node) {
		return false
	}

	rt.mu.Lock()
	defer rt.mu.Unlock()
	this := rt.trie.Load().(*trie.Trie[K, N])
	next, _ := trie.Add(this, kk, node)
	if next == this {
		return false
	}
	rt.trie.Store(next)
	return true
}

// RemoveKey tries to remove a node identified by its Kademlia key from the
// routing table. It returns true if the key was found to be present in the table and was removed.
func (rt *TrieRT[K, N]) RemoveKey(kk K) bool {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	this := rt.trie.Load().(*trie.Trie[K, N])
	next, _ := trie.Remove(this, kk)
	if next == this {
		return false
	}
	rt.trie.Store(next)
	if rt.nodeFilter != nil {
		_, node := trie.Find(this, kk)
		rt.nodeFilter.Remove(node)
	}
	return true
}

// NearestNodes returns the n closest nodes to a given key.
func (rt *TrieRT[K, N]) NearestNodes(target K, n int) []N {
	this := rt.trie.Load().(*trie.Trie[K, N])
	closestEntries := trie.Closest(this, target, n)
	if len(closestEntries) == 0 {
		return []N{}
	}

	nodes := make([]N, 0, len(closestEntries))
	for _, c := range closestEntries {
		nodes = append(nodes, c.Data)
	}

	return nodes
}

func (rt *TrieRT[K, N]) GetNode(kk K) (N, bool) {
	this := rt.trie.Load().(*trie.Trie[K, N])
	found, node := trie.Find(this, kk)
	if !found {
		var zero N
		return zero, false
	}
	return node, true
}

// Size returns the number of peers contained in the table.
func (rt *TrieRT[K, N]) Size() int {
	this := rt.trie.Load().(*trie.Trie[K, N])
	return this.Size()
}

// Cpl returns the longest common prefix length the supplied key shares with the table's key.
func (rt *TrieRT[K, N]) Cpl(kk K) int {
	return rt.self.CommonPrefixLength(kk)
}

// CplSize returns the number of peers in the table whose longest common prefix with the table's key is of length cpl.
func (rt *TrieRT[K, N]) CplSize(cpl int) int {
	this := rt.trie.Load().(*trie.Trie[K, N])
	n, err := countCpl(this, rt.self, cpl, 0)
	if err != nil {
		return 0
	}
	return n
}

func countCpl[K kad.Key[K], N kad.NodeID[K]](t *trie.Trie[K, N], kk K, cpl int, depth int) (int, error) {
	// special cases for very small tables where keys may be placed higher in the trie due to low population
	if t.IsLeaf() {
		if !t.HasKey() {
			return 0, nil
		}
		keyCpl := kk.CommonPrefixLength(*t.Key())
		if keyCpl == cpl {
			return 1, nil
		}
		return 0, nil
	}

	if depth > kk.BitLen() {
		return 0, nil
	}

	if depth == cpl {
		// return the number of entries that do not share the next bit with kk
		return t.Branch(1 - int(kk.Bit(depth))).Size(), nil
	}

	return countCpl(t.Branch(int(kk.Bit(depth))), kk, cpl, depth+1)
}
