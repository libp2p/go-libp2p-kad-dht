package qpeerset

import (
	"testing"

	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/test"

	kb "github.com/libp2p/go-libp2p-kbucket"

	"github.com/stretchr/testify/require"
)

func TestQPeerSet(t *testing.T) {
	key := "test"
	qp := NewQueryPeerset(key)

	// -----------------Ordering between peers for the Test -----
	// KEY < peer3 < peer1 < peer4 < peer2
	// ----------------------------------------------------------
	peer2 := test.RandPeerIDFatal(t)
	var peer4 peer.ID
	for {
		peer4 = test.RandPeerIDFatal(t)
		if kb.Closer(peer4, peer2, key) {
			break
		}
	}

	var peer1 peer.ID
	for {
		peer1 = test.RandPeerIDFatal(t)
		if kb.Closer(peer1, peer4, key) {
			break
		}
	}

	var peer3 peer.ID
	for {
		peer3 = test.RandPeerIDFatal(t)
		if kb.Closer(peer3, peer1, key) {
			break
		}
	}

	oracle := test.RandPeerIDFatal(t)

	// find fails
	require.Equal(t, -1, qp.find(peer2))

	// add peer2,assert state & then another add fails
	require.True(t, qp.TryAdd(peer2, oracle))
	require.Equal(t, PeerHeard, qp.GetState(peer2))
	require.False(t, qp.TryAdd(peer2, oracle))
	require.Equal(t, 0, qp.NumWaiting())

	// add peer4
	require.True(t, qp.TryAdd(peer4, oracle))
	cl := qp.GetClosestNotUnreachable(2)
	require.Equal(t, []peer.ID{peer4, peer2}, cl)
	cl = qp.GetClosestNotUnreachable(3)
	require.Equal(t, []peer.ID{peer4, peer2}, cl)
	cl = qp.GetClosestNotUnreachable(1)
	require.Equal(t, []peer.ID{peer4}, cl)

	// mark as unreachable & try to get it
	qp.SetState(peer4, PeerUnreachable)
	cl = qp.GetClosestNotUnreachable(1)
	require.Equal(t, []peer.ID{peer2}, cl)

	// add peer1
	require.True(t, qp.TryAdd(peer1, oracle))
	cl = qp.GetClosestNotUnreachable(1)
	require.Equal(t, []peer.ID{peer1}, cl)
	cl = qp.GetClosestNotUnreachable(2)
	require.Equal(t, []peer.ID{peer1, peer2}, cl)

	// mark as waiting and assert
	qp.SetState(peer2, PeerWaiting)
	require.Equal(t, []peer.ID{peer2}, qp.GetWaitingPeers())

	require.Equal(t, []peer.ID{peer1}, qp.GetHeardPeers())
	require.True(t, qp.TryAdd(peer3, oracle))
	require.Equal(t, []peer.ID{peer3, peer1}, qp.GetSortedHeard())
	require.Equal(t, 2, qp.NumHeard())
}
