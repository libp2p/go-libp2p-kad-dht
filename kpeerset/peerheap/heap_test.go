package peerheap

import (
	"container/heap"
	"testing"

	"github.com/libp2p/go-libp2p-core/peer"

	"github.com/stretchr/testify/require"
)

// a comparator that compares peer Ids based on their length
var cmp = func(i1 Item, i2 Item) bool {
	return len(i1.Peer) < len(i2.Peer)
}

var (
	peer1 = peer.ID("22")
	peer2 = peer.ID("1")
	peer3 = peer.ID("333")
)

func TestMinHeap(t *testing.T) {
	// create new
	ph := New(false, cmp)
	require.Zero(t, ph.Len())

	// push the element
	heap.Push(ph, &Item{Peer: peer1})
	// assertions
	require.True(t, ph.Len() == 1)
	require.Equal(t, peer1, ph.PeekTop().Peer)

	// push another element
	heap.Push(ph, &Item{Peer: peer2})
	// assertions
	require.True(t, ph.Len() == 2)
	require.Equal(t, peer2, ph.PeekTop().Peer)

	// push another element
	heap.Push(ph, &Item{Peer: peer3})
	// assertions
	require.True(t, ph.Len() == 3)
	require.Equal(t, peer2, ph.PeekTop().Peer)

	// remove & add again
	heap.Remove(ph, 1)
	require.True(t, ph.Len() == 2)
	heap.Remove(ph, 0)
	require.True(t, ph.Len() == 1)

	heap.Push(ph, &Item{Peer: peer1})
	heap.Push(ph, &Item{Peer: peer2})

	// test filter peers
	filtered := ph.FilterItems(func(i Item) bool {
		return len(i.Peer) != 2
	})
	require.Len(t, filtered, 2)
	require.Contains(t, itemsToPeers(filtered), peer2)
	require.Contains(t, itemsToPeers(filtered), peer3)

	// Assert Min Heap Order
	require.Equal(t, peer2, heap.Pop(ph).(*Item).Peer)
	require.Equal(t, peer1, heap.Pop(ph).(*Item).Peer)
	require.Equal(t, peer3, heap.Pop(ph).(*Item).Peer)
}

func itemsToPeers(is []Item) []peer.ID {
	peers := make([]peer.ID, 0, len(is))
	for _, i := range is {
		peers = append(peers, i.Peer)
	}
	return peers
}

func TestMaxHeap(t *testing.T) {
	// create new
	ph := New(true, cmp)
	require.Zero(t, ph.Len())

	// push all three peers
	heap.Push(ph, &Item{Peer: peer1})
	heap.Push(ph, &Item{Peer: peer3})
	heap.Push(ph, &Item{Peer: peer2})

	// Assert Max Heap Order
	require.Equal(t, peer3, heap.Pop(ph).(*Item).Peer)
	require.Equal(t, peer1, heap.Pop(ph).(*Item).Peer)
	require.Equal(t, peer2, heap.Pop(ph).(*Item).Peer)
}
