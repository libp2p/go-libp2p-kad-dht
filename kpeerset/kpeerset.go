package kpeerset

import (
	"container/heap"
	"math/big"
	"sync"

	"github.com/libp2p/go-libp2p-peer"
	ks "github.com/whyrusleeping/go-keyspace"
)

// peerMetric tracks a peer and its distance to something else.
type peerMetric struct {
	// the peer
	peer peer.ID

	// big.Int for XOR metric
	metric *big.Int
}

// peerMetricHeap implements a heap of peerDistances. Taken from
// go-libp2p-peerstore/queue but inverted. This heap sorts by _furthest_.
type peerMetricHeap []*peerMetric

func (ph peerMetricHeap) Len() int {
	return len(ph)
}

func (ph peerMetricHeap) Less(i, j int) bool {
	return 1 == ph[i].metric.Cmp(ph[j].metric)
}

func (ph peerMetricHeap) Swap(i, j int) {
	ph[i], ph[j] = ph[j], ph[i]
}

func (ph *peerMetricHeap) Push(x interface{}) {
	item := x.(*peerMetric)
	*ph = append(*ph, item)
}

func (ph *peerMetricHeap) Pop() interface{} {
	old := *ph
	n := len(old)
	item := old[n-1]
	*ph = old[0 : n-1]
	return item
}

// KPeerSet implements heap.Interface and PeerQueue
type KPeerSet struct {
	kvalue int

	// from is the Key this PQ measures against
	from ks.Key

	// heap is a heap of peerDistance items
	heap peerMetricHeap

	lock sync.RWMutex
}

func (pq *KPeerSet) Len() int {
	pq.lock.RLock()
	defer pq.lock.RUnlock()

	return len(pq.heap)
}

func (pq *KPeerSet) Check(p peer.ID) bool {
	pq.lock.RLock()
	defer pq.lock.RUnlock()

	if pq.heap.Len() < pq.kvalue {
		return true
	}

	distance := ks.XORKeySpace.Key([]byte(p)).Distance(pq.from)
	return distance.Cmp(pq.heap[0].metric) != -1
}

func (pq *KPeerSet) Add(p peer.ID) bool {
	pq.lock.Lock()
	defer pq.lock.Unlock()

	distance := ks.XORKeySpace.Key([]byte(p)).Distance(pq.from)

	if pq.heap.Len() >= pq.kvalue {
		// If we're not closer than the worst peer, drop this.
		if distance.Cmp(pq.heap[0].metric) != -1 {
			return false
		}
		// Replacing something, remove it.
		heap.Pop(&pq.heap)
	}

	heap.Push(&pq.heap, &peerMetric{
		peer:   p,
		metric: distance,
	})
	return true
}

func (pq *KPeerSet) Peers() []peer.ID {
	pq.lock.RLock()
	defer pq.lock.RUnlock()

	ret := make([]peer.ID, len(pq.heap))
	for _, pm := range pq.heap {
		ret = append(ret, pm.peer)
	}
	return ret
}

func New(kvalue int, from string) *KPeerSet {
	return &KPeerSet{
		from:   ks.XORKeySpace.Key([]byte(from)),
		kvalue: kvalue,
		heap:   make([]*peerMetric, 0, kvalue),
	}
}
