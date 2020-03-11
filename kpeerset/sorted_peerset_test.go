package kpeerset

import (
	"testing"

	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/test"

	kb "github.com/libp2p/go-libp2p-kbucket"

	"github.com/stretchr/testify/require"
)

func TestSortedPeerset(t *testing.T) {
	key := "test"
	sp := NewSortedPeerset(2, key)
	require.Empty(t, sp.UnqueriedFromKClosest())

	// peer3 < peer1 < peer4 < peer2 by distance from key
	peer2 := test.RandPeerIDFatal(t)

	// add peer 2 & assert
	require.True(t, sp.Add(peer2))
	require.Len(t, sp.UnqueriedFromKClosest(), 1)
	require.Equal(t, sp.UnqueriedFromKClosest()[0], peer2)

	// add peer4 & assert
	var peer4 peer.ID
	for {
		peer4 = test.RandPeerIDFatal(t)
		if kb.Closer(peer4, peer2, key) {
			break
		}
	}
	require.True(t, sp.Add(peer4))
	require.Len(t, sp.UnqueriedFromKClosest(), 2)
	require.Contains(t, sp.UnqueriedFromKClosest(), peer2)
	require.Contains(t, sp.UnqueriedFromKClosest(), peer4)

	// add peer1 which will displace peer2 in the kClosest
	var peer1 peer.ID
	for {
		peer1 = test.RandPeerIDFatal(t)
		if kb.Closer(peer1, peer4, key) {
			break
		}
	}
	require.True(t, sp.Add(peer1))
	require.Len(t, sp.UnqueriedFromKClosest(), 2)
	require.Contains(t, sp.UnqueriedFromKClosest(), peer1)
	require.Contains(t, sp.UnqueriedFromKClosest(), peer4)

	// add peer 3 which will displace peer4 in the kClosest
	var peer3 peer.ID
	for {
		peer3 = test.RandPeerIDFatal(t)
		if kb.Closer(peer3, peer1, key) {
			break
		}
	}
	require.True(t, sp.Add(peer3))
	require.Len(t, sp.UnqueriedFromKClosest(), 2)
	require.Contains(t, sp.UnqueriedFromKClosest(), peer1)
	require.Contains(t, sp.UnqueriedFromKClosest(), peer3)

	// removing peer1 moves peer4 to the KClosest
	sp.Remove(peer1)
	require.Len(t, sp.UnqueriedFromKClosest(), 2)
	require.Contains(t, sp.UnqueriedFromKClosest(), peer3)
	require.Contains(t, sp.UnqueriedFromKClosest(), peer4)
	sp.lock.Lock()
	require.True(t, sp.heapRestOfPeers.Len() == 1)
	require.Contains(t, sp.heapRestOfPeers.Peers(), peer2)
	sp.lock.Unlock()

	// mark a peer as queried
	sp.MarkQueried(peer4)
	require.Len(t, sp.UnqueriedFromKClosest(), 1)
	require.Contains(t, sp.UnqueriedFromKClosest(), peer3)

	// removing peer3 moves peer2 to the kClosest.
	sp.Remove(peer3)
	require.Len(t, sp.UnqueriedFromKClosest(), 1)
	require.Contains(t, sp.UnqueriedFromKClosest(), peer2)
	sp.lock.Lock()
	require.Empty(t, sp.heapRestOfPeers.Peers())
	sp.lock.Unlock()
}
