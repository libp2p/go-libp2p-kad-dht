package kpeerset

import (
	"container/heap"
	"math/big"
	"sync"

	"github.com/libp2p/go-libp2p-core/peer"

	"github.com/libp2p/go-libp2p-kad-dht/kpeerset/peerheap"

	ks "github.com/whyrusleeping/go-keyspace"
)

// SortedPeerset is a data-structure that maintains the queried & unqueried peers for a query
// based on their distance from the key.
// It's main use is to allow peer addition, removal & retrieval for the query as per the
// semantics described in the Kad DHT paper.
type SortedPeerset struct {
	// the key being searched for
	key ks.Key

	// the K parameter in the Kad DHT paper
	kvalue int

	// a maxHeap maintaining the K closest(Kademlia XOR distance) peers to the key.
	// the topmost peer will be the peer furthest from the key in this heap.
	heapKClosestPeers *peerheap.Heap

	// a minHeap for for rest of the peers ordered by their distance from the key.
	// the topmost peer will be the peer closest to the key in this heap.
	heapRestOfPeers *peerheap.Heap

	// pointer to the item in the heap of K closest peers.
	kClosestPeers map[peer.ID]*peerheap.Item

	// pointer to the item in the heap of the rest of peers.
	restOfPeers map[peer.ID]*peerheap.Item

	// peers that have already been queried.
	queried map[peer.ID]struct{}

	// the closest peer to the key that we have heard about
	closestKnownPeer peer.ID
	// the distance of the closest known peer from the key
	dClosestKnownPeer *big.Int

	lock sync.Mutex
}

// NewSortedPeerset creates and returns a new SortedPeerset.
func NewSortedPeerset(kvalue int, key string) *SortedPeerset {
	compare := func(i1 peerheap.Item, i2 peerheap.Item) bool {
		// distance of the first peer from the key
		d1 := i1.Value.(*big.Int)
		// distance of the second peer from the key
		d2 := i2.Value.(*big.Int)

		// Is the first peer closer to the key than the second peer ?
		return d1.Cmp(d2) == -1
	}

	return &SortedPeerset{
		key:               ks.XORKeySpace.Key([]byte(key)),
		kvalue:            kvalue,
		heapKClosestPeers: peerheap.New(true, compare),
		heapRestOfPeers:   peerheap.New(false, compare),
		kClosestPeers:     make(map[peer.ID]*peerheap.Item),
		restOfPeers:       make(map[peer.ID]*peerheap.Item),
		queried:           make(map[peer.ID]struct{}),
	}
}

// Add adds the peer to the SortedPeerset.
//
// If there are less than K peers in the K closest peers, we add the peer to
// the K closest peers.
//
// Otherwise, we do one of the following:
// 1. If this peer is closer to the key than the peer furthest from the key in the
//    K closest peers, we move that furthest peer to the rest of peers and then
//    add this peer to the K closest peers.
// 2. If this peer is further from the key than the peer furthest from the key in the
//    K closest peers, we add it to the rest of peers.
//
// Returns true if the peer is closer to key than the closet peer we've heard about.
func (ps *SortedPeerset) Add(p peer.ID) bool {
	ps.lock.Lock()
	defer ps.lock.Unlock()

	// we've already added the peer
	if ps.kClosestPeers[p] != nil || ps.restOfPeers[p] != nil {
		return false
	}

	// calculate the distance of the given peer from the key
	distancePeer := ks.XORKeySpace.Key([]byte(p)).Distance(ps.key)
	item := &peerheap.Item{Peer: p, Value: distancePeer}

	if ps.heapKClosestPeers.Len() < ps.kvalue {
		// add the peer to the K closest peers if we have space
		heap.Push(ps.heapKClosestPeers, item)
		ps.kClosestPeers[p] = item
	} else if top := ps.heapKClosestPeers.PeekTop(); distancePeer.Cmp(top.Value.(*big.Int)) == -1 {
		// peer is closer to the key than the top peer in the heap of K closest peers
		// which is basically the peer furthest from the key because the K closest peers
		// are stored in a maxHeap ordered by the distance from the key.

		// remove the top peer from the K closest peers & add it to the rest of peers.
		bumpedPeer := heap.Pop(ps.heapKClosestPeers).(*peerheap.Item)
		delete(ps.kClosestPeers, bumpedPeer.Peer)
		heap.Push(ps.heapRestOfPeers, bumpedPeer)
		ps.restOfPeers[bumpedPeer.Peer] = bumpedPeer

		// add the peer p to the K closest peers
		heap.Push(ps.heapKClosestPeers, item)
		ps.kClosestPeers[p] = item
	} else {
		// add the peer to the rest of peers.
		heap.Push(ps.heapRestOfPeers, item)
		ps.restOfPeers[p] = item
	}

	if ps.closestKnownPeer == "" || (distancePeer.Cmp(ps.dClosestKnownPeer) == -1) {
		// given peer is closer to the key than the current closest known peer.
		// So, let's update the closest known peer
		ps.closestKnownPeer = p
		ps.dClosestKnownPeer = distancePeer
		return true
	}

	return false
}

// UnqueriedFromKClosest returns the unqueried peers among the K closest peers AFTER
// sorting them in Ascending Order with the given comparator.
func (ps *SortedPeerset) UnqueriedFromKClosest(sortWith peerheap.Comparator) []peer.ID {
	ps.lock.Lock()
	defer ps.lock.Unlock()

	unqueriedPeerItems := ps.heapKClosestPeers.FilterItems(ps.isPeerItemQueried)

	// create a min-heap to sort the unqueried peer Items using the given comparator
	ph := peerheap.New(false, sortWith)
	for _, i := range unqueriedPeerItems {
		v := i
		heap.Push(ph, &v)
	}
	// now pop so we get them in sorted order
	peers := make([]peer.ID, 0, ph.Len())
	for ph.Len() != 0 {
		popped := heap.Pop(ph).(*peerheap.Item)
		peers = append(peers, popped.Peer)
	}

	return peers
}

// LenUnqueriedFromKClosest returns the number of unqueried peers among
// the K closest peers.
func (ps *SortedPeerset) LenUnqueriedFromKClosest() int {
	ps.lock.Lock()
	defer ps.lock.Unlock()

	unqueriedPeerItems := ps.heapKClosestPeers.FilterItems(ps.isPeerItemQueried)

	return len(unqueriedPeerItems)
}

// caller is responsible for the locking
func (ps *SortedPeerset) isPeerItemQueried(i peerheap.Item) bool {
	_, ok := ps.queried[i.Peer]
	return !ok
}

// MarkQueried marks the peer as queried.
// It should be called when we have successfully dialed to and gotten a response from the peer.
func (ps *SortedPeerset) MarkQueried(p peer.ID) {
	ps.lock.Lock()
	defer ps.lock.Unlock()

	ps.queried[p] = struct{}{}
}

// Remove removes the peer from the SortedPeerset.
//
// If the removed peer was among the K closest peers, we pop a peer from the heap of rest of peers
// and add it to the K closest peers to replace the removed peer. The peer added to the K closest peers in this way
// would be the peer that was closest to the key among the rest of peers since the rest of peers are in a
// minHeap ordered on the distance from the key.
func (ps *SortedPeerset) Remove(p peer.ID) {
	ps.lock.Lock()
	defer ps.lock.Unlock()

	delete(ps.queried, p)

	if item, ok := ps.kClosestPeers[p]; ok {
		// peer is among the K closest peers

		// remove it from the K closest peers
		heap.Remove(ps.heapKClosestPeers, item.Index)
		delete(ps.kClosestPeers, p)

		// we now need to add a peer to the K closest peers from the rest of peers
		// to make up for the peer that was just removed
		if ps.heapRestOfPeers.Len() > 0 {
			// pop a peer from the rest of peers & add it to the K closest peers
			upgrade := heap.Pop(ps.heapRestOfPeers).(*peerheap.Item)
			delete(ps.restOfPeers, upgrade.Peer)
			heap.Push(ps.heapKClosestPeers, upgrade)
			ps.kClosestPeers[upgrade.Peer] = upgrade
		}
	} else if item, ok := ps.restOfPeers[p]; ok {
		// peer is not among the K closest, so remove it from the rest of peers.
		heap.Remove(ps.heapRestOfPeers, item.Index)
		delete(ps.restOfPeers, p)
	}
}
