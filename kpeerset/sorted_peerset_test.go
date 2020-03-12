package kpeerset

import (
	"testing"

	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/test"

	"github.com/libp2p/go-libp2p-kad-dht/kpeerset/peerheap"
	kb "github.com/libp2p/go-libp2p-kbucket"

	"github.com/stretchr/testify/require"
)

var noopCompare = func(i1 peerheap.Item, i2 peerheap.Item) bool {
	return true
}

func TestSortedPeerset(t *testing.T) {
	key := "test"
	sp := NewSortedPeerset(2, key)
	require.Empty(t, sp.UnqueriedFromKClosest(noopCompare))

	// -----------------Ordering between peers for the Test -----
	// peer0 < peer3 < peer1 < peer4 < peer2 < peer5 by distance from key
	// ----------------------------------------------------------
	peer2 := test.RandPeerIDFatal(t)

	// add peer 2 & assert
	require.True(t, sp.Add(peer2))
	require.Len(t, sp.UnqueriedFromKClosest(noopCompare), 1)
	require.True(t, sp.LenUnqueriedFromKClosest() == 1)
	require.Equal(t, sp.UnqueriedFromKClosest(noopCompare)[0], peer2)
	assertClosestKnownPeer(t, sp, peer2)

	// add peer4 & assert
	var peer4 peer.ID
	for {
		peer4 = test.RandPeerIDFatal(t)
		if kb.Closer(peer4, peer2, key) {
			break
		}
	}
	require.True(t, sp.Add(peer4))
	require.Len(t, sp.UnqueriedFromKClosest(noopCompare), 2)
	require.True(t, sp.LenUnqueriedFromKClosest() == 2)
	require.Contains(t, sp.UnqueriedFromKClosest(noopCompare), peer2)
	require.Contains(t, sp.UnqueriedFromKClosest(noopCompare), peer4)
	assertClosestKnownPeer(t, sp, peer4)

	// add peer1 which will displace peer2 in the kClosest
	var peer1 peer.ID
	for {
		peer1 = test.RandPeerIDFatal(t)
		if kb.Closer(peer1, peer4, key) {
			break
		}
	}
	require.True(t, sp.Add(peer1))
	require.Len(t, sp.UnqueriedFromKClosest(noopCompare), 2)
	require.Contains(t, sp.UnqueriedFromKClosest(noopCompare), peer1)
	require.Contains(t, sp.UnqueriedFromKClosest(noopCompare), peer4)
	assertClosestKnownPeer(t, sp, peer1)

	// add peer 3 which will displace peer4 in the kClosest
	var peer3 peer.ID
	for {
		peer3 = test.RandPeerIDFatal(t)
		if kb.Closer(peer3, peer1, key) {
			break
		}
	}
	require.True(t, sp.Add(peer3))
	require.Len(t, sp.UnqueriedFromKClosest(noopCompare), 2)
	require.Contains(t, sp.UnqueriedFromKClosest(noopCompare), peer1)
	require.Contains(t, sp.UnqueriedFromKClosest(noopCompare), peer3)
	assertClosestKnownPeer(t, sp, peer3)

	// removing peer1 moves peer4 to the KClosest
	sp.Remove(peer1)
	require.Len(t, sp.UnqueriedFromKClosest(noopCompare), 2)
	require.Contains(t, sp.UnqueriedFromKClosest(noopCompare), peer3)
	require.Contains(t, sp.UnqueriedFromKClosest(noopCompare), peer4)
	sp.lock.Lock()
	require.True(t, sp.heapRestOfPeers.Len() == 1)
	require.Contains(t, sp.heapRestOfPeers.Peers(), peer2)
	sp.lock.Unlock()

	// mark a peer as queried
	sp.MarkQueried(peer4)
	require.Len(t, sp.UnqueriedFromKClosest(noopCompare), 1)
	require.Contains(t, sp.UnqueriedFromKClosest(noopCompare), peer3)

	// removing peer3 moves peer2 to the kClosest.
	sp.Remove(peer3)
	require.Len(t, sp.UnqueriedFromKClosest(noopCompare), 1)
	require.Contains(t, sp.UnqueriedFromKClosest(noopCompare), peer2)
	sp.lock.Lock()
	require.Empty(t, sp.heapRestOfPeers.Peers())
	sp.lock.Unlock()

	// adding peer5 does not change the closest known peer
	var peer5 peer.ID
	for {
		peer5 = test.RandPeerIDFatal(t)
		if kb.Closer(peer2, peer5, key) {
			break
		}
	}
	require.False(t, sp.Add(peer5))
	assertClosestKnownPeer(t, sp, peer3)

	// adding peer0 changes the closest known peer
	var peer0 peer.ID
	for {
		peer0 = test.RandPeerIDFatal(t)
		if kb.Closer(peer0, peer3, key) {
			break
		}
	}
	require.True(t, sp.Add(peer0))
	assertClosestKnownPeer(t, sp, peer0)
}

func TestSortingUnqueriedFromKClosest(t *testing.T) {
	p1 := peer.ID("1")
	p2 := peer.ID("22")
	p3 := peer.ID("333")

	key := "test"
	sp := NewSortedPeerset(3, key)
	sp.Add(p1)
	sp.Add(p3)
	sp.Add(p2)

	ps := sp.UnqueriedFromKClosest(func(i1 peerheap.Item, i2 peerheap.Item) bool {
		return len(i1.Peer) > len(i2.Peer)
	})
	require.Len(t, ps, 3)
	require.Equal(t, p3, ps[0])
	require.Equal(t, p2, ps[1])
	require.Equal(t, p1, ps[2])

	// mark one as queried
	sp.MarkQueried(p3)
	ps = sp.UnqueriedFromKClosest(func(i1 peerheap.Item, i2 peerheap.Item) bool {
		return len(i1.Peer) > len(i2.Peer)
	})
	require.Len(t, ps, 2)
	require.Equal(t, p2, ps[0])
	require.Equal(t, p1, ps[1])
}

func assertClosestKnownPeer(t *testing.T, sp *SortedPeerset, p peer.ID) {
	sp.lock.Lock()
	defer sp.lock.Unlock()

	require.Equal(t, sp.closestKnownPeer, p)
}
