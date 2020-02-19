package kpeerset

import (
	"github.com/libp2p/go-libp2p-core/peer"
	"math/big"
)

type IPeerMetric interface {
	Peer() peer.ID
	Metric() *big.Int
}

// peerMetric tracks a peer and its distance to something else.
type peerMetric struct {
	// the peer
	peer peer.ID

	// big.Int for XOR metric
	metric *big.Int
}

func (pm peerMetric) Peer() peer.ID    { return pm.peer }
func (pm peerMetric) Metric() *big.Int { return pm.metric }

type peerMetricHeapItem struct {
	IPeerMetric

	// The index of the item in the heap
	index int
}

// peerMetricHeap implements a heap of peerDistances.
// The heap sorts by furthest if direction = 1 and closest if direction = -1
type peerMetricHeap struct {
	data      []*peerMetricHeapItem
	direction int
}

func (ph *peerMetricHeap) Len() int {
	return len(ph.data)
}

func (ph *peerMetricHeap) Less(i, j int) bool {
	h := ph.data
	return ph.direction == h[i].Metric().Cmp(h[j].Metric())
}

func (ph *peerMetricHeap) Swap(i, j int) {
	h := ph.data
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (ph *peerMetricHeap) Push(x interface{}) {
	n := len(ph.data)
	item := x.(*peerMetricHeapItem)
	item.index = n
	ph.data = append(ph.data, item)
}

func (ph *peerMetricHeap) Pop() interface{} {
	old := ph.data
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // avoid memory leak
	item.index = -1 // for safety
	ph.data = old[0 : n-1]
	return item
}
