package dht

import (
	"fmt"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-kbucket/peerdiversity"
	ma "github.com/multiformats/go-multiaddr"
	"time"
)

var _ peerdiversity.PeerIPGroupFilter = (*rtPeerIPGroupFilter)(nil)

// locking is the responsibility of the caller
type rtPeerIPGroupFilter struct {
	h host.Host

	maxPerCpl   int
	maxForTable int

	cplPeerCount map[int]int

	cplIpGroupCount   map[int]map[peerdiversity.PeerIPGroupKey]int
	tableIpGroupCount map[peerdiversity.PeerIPGroupKey]int

	allowAll bool
}

// NewRTPeerDiversityFilter constructs the `PeerIPGroupFilter` that will be used to configure
// the diversity filter for the Routing Table.
// Please see the docs for `peerdiversity.PeerIPGroupFilter` AND `peerdiversity.Filter` for more details.
func NewRTPeerDiversityFilter(h host.Host, maxPerCpl, maxForTable int, allowAll bool) *rtPeerIPGroupFilter {
	return &rtPeerIPGroupFilter{
		h: h,

		maxPerCpl:   maxPerCpl,
		maxForTable: maxForTable,

		cplPeerCount:      make(map[int]int),
		cplIpGroupCount:   make(map[int]map[peerdiversity.PeerIPGroupKey]int),
		tableIpGroupCount: make(map[peerdiversity.PeerIPGroupKey]int),

		allowAll:allowAll,
	}

}

func (r *rtPeerIPGroupFilter) Allow(g peerdiversity.PeerGroupInfo) bool {
	if r.allowAll {
		return true
	}
	key := g.IPGroupKey
	cpl := g.Cpl

	if r.tableIpGroupCount[key] >= r.maxForTable {
		fmt.Println("\n Rejecting because table limit is hit")
		return false
	}

	c, ok := r.cplIpGroupCount[cpl]
	allow :=  !ok || c[key] < r.maxPerCpl
	if !allow {
		fmt.Println("\n Rejecting because CPL limit is hit")
	}
	return allow
}

func (r *rtPeerIPGroupFilter) Increment(g peerdiversity.PeerGroupInfo) {
	key := g.IPGroupKey
	cpl := g.Cpl

	r.tableIpGroupCount[key] = r.tableIpGroupCount[key] + 1
	_, ok := r.cplIpGroupCount[cpl]
	if !ok {
		r.cplIpGroupCount[cpl] = make(map[peerdiversity.PeerIPGroupKey]int)
	}
	r.cplIpGroupCount[cpl][key] = r.cplIpGroupCount[cpl][key] + 1

	r.cplPeerCount[cpl] = r.cplPeerCount[cpl] + 1
}

func (r *rtPeerIPGroupFilter) Decrement(g peerdiversity.PeerGroupInfo) {
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

	r.cplPeerCount[cpl] = r.cplPeerCount[cpl] - 1
	if r.cplPeerCount[cpl] == 0 {
		delete(r.cplPeerCount, cpl)
	}
}

func (r *rtPeerIPGroupFilter) PeerAddresses(p peer.ID) []ma.Multiaddr {
	cs := r.h.Network().ConnsToPeer(p)
	addr := make([]ma.Multiaddr, 0, len(cs))
	for _, c := range cs {
		addr = append(addr, c.RemoteMultiaddr())
	}
	return addr
}

func (r *rtPeerIPGroupFilter) PrintStats() {
	fmt.Printf("\n -------------- Routing Table Peer Diversity Stats At %v----------------", time.Now().String())

	for cpl := range r.cplIpGroupCount {
		fmt.Printf("\n\t Cpl=%d\tTotalPeers=%d", cpl, r.cplPeerCount[cpl])
		for k, v := range r.cplIpGroupCount[cpl] {
			fmt.Printf("\n\t\t Prefix=%s\tNPeers=%d", k, v)
		}
	}
	fmt.Println("\n-------------------------------------------------------------------")

}