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

/*
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

func (pq *KPeerSet) Add(p peer.ID) (bool, peer.ID) {
	pq.lock.Lock()
	defer pq.lock.Unlock()

	distance := ks.XORKeySpace.Key([]byte(p)).Distance(pq.from)

	var replacedPeer peer.ID
	if pq.heap.Len() >= pq.kvalue {
		// If we're not closer than the worst peer, drop this.
		if distance.Cmp(pq.heap[0].metric) != -1 {
			return false, replacedPeer
		}
		// Replacing something, remove it.
		replacedPeer = heap.Pop(&pq.heap).(*peerMetric).peer
	}

	heap.Push(&pq.heap, &peerMetric{
		peer:   p,
		metric: distance,
	})
	return true, replacedPeer
}

func (pq *KPeerSet) Remove(id peer.ID) {
	pq.lock.Lock()
	defer pq.lock.Unlock()

	for i, pm := range pq.heap {
		if pm.peer == id {
			heap.Remove(&pq.heap, i)
			return
		}
	}
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
*/
