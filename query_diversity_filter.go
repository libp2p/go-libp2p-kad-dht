package dht

import (
	"fmt"
	"sync"

	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"

	"github.com/libp2p/go-libp2p-kbucket/peerdiversity"

	ma "github.com/multiformats/go-multiaddr"
)

var _ peerdiversity.PeerIPGroupFilter = (*queryIPGroupFilter)(nil)

type queryIPGroupFilter struct {
	mu sync.RWMutex
	h  host.Host

	maxPrefixCount int

	prefixGroupCount map[peerdiversity.PeerIPGroupKey]int
}

// NewQueryDiversityFilter constructs the `PeerIPGroupFilter` that will be used to configure
// the diversity filter for the DHT Query.
// Please see the docs for `peerdiversity.PeerIPGroupFilter` AND `peerdiversity.Filter` for more details.
func NewQueryDiversityFilter(h host.Host, maxPrefixCount int) *queryIPGroupFilter {
	return &queryIPGroupFilter{
		h:                h,
		maxPrefixCount:   maxPrefixCount,
		prefixGroupCount: make(map[peerdiversity.PeerIPGroupKey]int),
	}

}

func (q *queryIPGroupFilter) Allow(g peerdiversity.PeerGroupInfo) bool {
	q.mu.RLock()
	defer q.mu.RUnlock()

	key := g.IPGroupKey

	return q.prefixGroupCount[key] < q.maxPrefixCount
}

func (q *queryIPGroupFilter) Increment(g peerdiversity.PeerGroupInfo) {
	q.mu.Lock()
	defer q.mu.Unlock()

	key := g.IPGroupKey

	q.prefixGroupCount[key] = q.prefixGroupCount[key] + 1
}

func (q *queryIPGroupFilter) Decrement(g peerdiversity.PeerGroupInfo) {
	q.mu.Lock()
	defer q.mu.Unlock()

	key := g.IPGroupKey

	if q.prefixGroupCount[key] == 0 {
		panic(fmt.Errorf("cannot decrement state below zero for group key %s", string(key)))
	}

	q.prefixGroupCount[key] = q.prefixGroupCount[key] - 1
	if q.prefixGroupCount[key] == 0 {
		delete(q.prefixGroupCount, key)
	}
}

func (q *queryIPGroupFilter) PeerAddresses(p peer.ID) []ma.Multiaddr {
	cs := q.h.Peerstore().Addrs(p)

	// Note: this should ONLY be used for the WAN DHT.
	// Otherwise, the peer wont be allowed if we return an empty set of addresses here
	// i.e. the peer ONLY has private addresses.
	addrs := make([]ma.Multiaddr, 0, len(cs))
	for _, a := range cs {
		if isPublicAddr(a) {
			addrs = append(addrs, a)
		}
	}
	return addrs
}
