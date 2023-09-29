package dht

import (
	"sync"

	"github.com/libp2p/go-libp2p-kad-dht/v2/kadt"
	"github.com/libp2p/go-libp2p-kbucket/peerdiversity"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/plprobelab/go-libdht/kad/triert"
)

var _ triert.NodeFilter[kadt.Key, kadt.PeerID] = (*TrieRTPeerDiversityFilter)(nil)

// TrieRTPeerDiversityFilter is a wrapper around the `peerdiversity.Filter` used
// as `triert.NodeFilter` to configure the diversity filter for the TrieRT
// Routing Table.
// Please see the docs for `peerdiversity.Filter` for more details
type TrieRTPeerDiversityFilter struct {
	*peerdiversity.Filter
}

// NewRTPeerDiversityFilter constructs the `TrieRTPeerDiversityFilter` defining
// the diversity filter for the TrieRT Routing Table.
// `maxPerCpl` represents the maximum number of peers per common prefix length
// allowed to share the same /16 IP group.
// `maxForTable` represents the maximum number of peers in the routing table
// allowed to share the same /16 IP group.
func NewRTPeerDiversityFilter(h host.Host, maxPerCpl, maxForTable int) *TrieRTPeerDiversityFilter {
	multiaddrsFn := func(p peer.ID) []ma.Multiaddr {
		cs := h.Network().ConnsToPeer(p)
		addr := make([]ma.Multiaddr, 0, len(cs))
		for _, c := range cs {
			addr = append(addr, c.RemoteMultiaddr())
		}
		return addr
	}

	peerIpGroupFilter := newRtPeerIPGroupFilter(maxPerCpl, maxForTable, multiaddrsFn)
	filter, err := peerdiversity.NewFilter(peerIpGroupFilter, "triert/diversity",
		func(p peer.ID) int {
			return kadt.PeerID(h.ID()).Key().CommonPrefixLength(kadt.PeerID(p).Key())
		})

	if err != nil {
		panic(err)
	}

	return &TrieRTPeerDiversityFilter{
		Filter: filter,
	}
}

// TryAdd is called by TrieRT when a new node is added to the routing table.
func (f *TrieRTPeerDiversityFilter) TryAdd(rt *triert.TrieRT[kadt.Key, kadt.PeerID], n kadt.PeerID) bool {
	return f.Filter.TryAdd(peer.ID(n))
}

// Remove is called by TrieRT when a node is removed from the routing table.
func (f *TrieRTPeerDiversityFilter) Remove(n kadt.PeerID) {
	f.Filter.Remove(peer.ID(n))
}

var _ peerdiversity.PeerIPGroupFilter = (*rtPeerIPGroupFilter)(nil)

// rtPeerIPGroupFilter is an implementation of `peerdiversity.PeerIPGroupFilter`.
// Please see the docs for `peerdiversity.PeerIPGroupFilter` for more details.
type rtPeerIPGroupFilter struct {
	mu sync.RWMutex

	maxPerCpl   int
	maxForTable int

	multiaddrsFn func(peer.ID) []ma.Multiaddr

	cplIpGroupCount   map[int]map[peerdiversity.PeerIPGroupKey]int
	tableIpGroupCount map[peerdiversity.PeerIPGroupKey]int
}

// newRtPeerIPGroupFilter constructs the `PeerIPGroupFilter` that will be used
// to configure the diversity filter for the Routing Table.
func newRtPeerIPGroupFilter(maxPerCpl, maxForTable int,
	multiaddrsFn func(peer.ID) []ma.Multiaddr) *rtPeerIPGroupFilter {
	return &rtPeerIPGroupFilter{
		multiaddrsFn: multiaddrsFn,

		maxPerCpl:   maxPerCpl,
		maxForTable: maxForTable,

		cplIpGroupCount:   make(map[int]map[peerdiversity.PeerIPGroupKey]int),
		tableIpGroupCount: make(map[peerdiversity.PeerIPGroupKey]int),
	}

}

// Allow is called by the `peerdiversity.Filter` to check if a peer is allowed
// to be added to the routing table.
func (r *rtPeerIPGroupFilter) Allow(g peerdiversity.PeerGroupInfo) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	key := g.IPGroupKey
	cpl := g.Cpl

	if r.tableIpGroupCount[key] >= r.maxForTable {

		return false
	}

	c, ok := r.cplIpGroupCount[cpl]
	allow := !ok || c[key] < r.maxPerCpl
	return allow
}

// Increment is called by the `peerdiversity.Filter` when a peer is added to the
// routing table.
func (r *rtPeerIPGroupFilter) Increment(g peerdiversity.PeerGroupInfo) {
	r.mu.Lock()
	defer r.mu.Unlock()

	key := g.IPGroupKey
	cpl := g.Cpl

	r.tableIpGroupCount[key] = r.tableIpGroupCount[key] + 1
	if _, ok := r.cplIpGroupCount[cpl]; !ok {
		r.cplIpGroupCount[cpl] = make(map[peerdiversity.PeerIPGroupKey]int)
	}

	r.cplIpGroupCount[cpl][key] = r.cplIpGroupCount[cpl][key] + 1
}

// Decrement is called by the `peerdiversity.Filter` when a peer is removed from
// the routing table.
func (r *rtPeerIPGroupFilter) Decrement(g peerdiversity.PeerGroupInfo) {
	r.mu.Lock()
	defer r.mu.Unlock()

	key := g.IPGroupKey
	cpl := g.Cpl

	r.tableIpGroupCount[key] = r.tableIpGroupCount[key] - 1
	if r.tableIpGroupCount[key] == 0 {
		delete(r.tableIpGroupCount, key)
	}

	r.cplIpGroupCount[cpl][key] = r.cplIpGroupCount[cpl][key] - 1
	if r.cplIpGroupCount[cpl][key] == 0 {
		delete(r.cplIpGroupCount[cpl], key)
	}
	if len(r.cplIpGroupCount[cpl]) == 0 {
		delete(r.cplIpGroupCount, cpl)
	}
}

// PeerAddresses is called by the `peerdiversity.Filter` to get the list of
// addresses of a peer.
func (r *rtPeerIPGroupFilter) PeerAddresses(p peer.ID) []ma.Multiaddr {
	return r.multiaddrsFn(p)
}
