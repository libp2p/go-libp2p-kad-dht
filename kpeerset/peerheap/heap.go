package peerheap

import (
	"github.com/libp2p/go-libp2p-core/peer"
)

// Comparator is the type of a function that compares two peers to determine the ordering between them.
// It returns true if p1 is "less" than p2 and false otherwise.
type Comparator func(p1 peer.ID, p2 peer.ID) bool

// Item is one "item" in the Heap.
// it contains the peer Id & the index of the "item" in the heap.
type Item struct {
	Peer  peer.ID
	Index int
}

// Heap implements a heap of peer Items.
// It uses the "compare" member function to compare two peers to determine the order between them.
// If isMaxHeap is set to true, this Heap is a maxHeap, otherwise it's a minHeap.
type Heap struct {
	items     []*Item
	compare   Comparator
	isMaxHeap bool
}

// New creates & returns a peer Heap.
func New(isMaxHeap bool, compare Comparator) *Heap {
	return &Heap{isMaxHeap: isMaxHeap, compare: compare}
}

// PeekTop returns a copy of the top/first Item in the heap.
// This would be the "maximum" or the "minimum" peer depending on whether
// the heap is a maxHeap or a minHeap.
//
// A call to PeekTop will panic if the Heap is empty.
func (ph *Heap) PeekTop() Item {
	return *ph.items[0]
}

// Peers returns all the peers in the heap.
func (ph *Heap) Peers() []peer.ID {
	peers := make([]peer.ID, 0, ph.Len())

	for _, i := range ph.items {
		peers = append(peers, i.Peer)
	}
	return peers
}

// Note: The functions below make the Heap satisfy the "heap.Interface" as required by the "heap" package in the
// standard library. Please refer to the docs for "heap.Interface" in the standard library for more details.

func (ph *Heap) Len() int {
	return len(ph.items)
}

func (ph *Heap) Less(i, j int) bool {
	h := ph.items

	isLess := ph.compare(h[i].Peer, h[j].Peer)

	// because the "compare" function returns true if peer1 is less than peer2,
	// we need to reverse it's result if the Heap is a maxHeap.
	if ph.isMaxHeap {
		return !isLess
	}
	return isLess
}

func (ph *Heap) Swap(i, j int) {
	h := ph.items
	h[i], h[j] = h[j], h[i]
	h[i].Index = i
	h[j].Index = j
}

func (ph *Heap) Push(x interface{}) {
	n := len(ph.items)
	item := x.(*Item)
	item.Index = n
	ph.items = append(ph.items, item)
}

func (ph *Heap) Pop() interface{} {
	old := ph.items
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // avoid memory leak
	item.Index = -1 // for safety
	ph.items = old[0 : n-1]
	return item
}
