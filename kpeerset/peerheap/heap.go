package peerheap

import (
	"github.com/libp2p/go-libp2p-core/peer"
)

// Comparator is the type of a function that compares two peer Heap items to determine the ordering between them.
// It returns true if i1 is "less" than i2 and false otherwise.
type Comparator func(i1 Item, i2 Item) bool

// Item is one "item" in the Heap.
// It contains the Id of the peer, an arbitrary value associated with the peer
// and the index of the "item" in the Heap.
type Item struct {
	Peer  peer.ID
	Value interface{}
	Index int
}

// Heap implements a heap of peer Items.
// It uses the "compare" member function to compare two peers to determine the order between them.
// If isMaxHeap is set to true, this Heap is a maxHeap, otherwise it's a minHeap.
//
// Note: It is the responsibility of the caller to enforce locking & synchronization.
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

// FilterItems returns Copies of ALL Items in the Heap that satisfy the given predicate
func (ph *Heap) FilterItems(p func(i Item) bool) []Item {
	var items []Item

	for _, i := range ph.items {
		ih := *i
		if p(ih) {
			items = append(items, ih)
		}
	}
	return items
}

// Peers returns all the peers currently in the heap
func (ph *Heap) Peers() []peer.ID {
	peers := make([]peer.ID, 0, len(ph.items))

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

	isLess := ph.compare(*h[i], *h[j])

	// because the "compare" function returns true if item1 is less than item2,
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
