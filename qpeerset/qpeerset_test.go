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
	_, found := qp.states[peer2]
	require.False(t, found)

	// add peer2,assert state & then another add fails
	require.True(t, qp.TryAdd(peer2, oracle))
	require.Equal(t, PeerHeard, qp.GetState(peer2))
	require.False(t, qp.TryAdd(peer2, oracle))
	require.Equal(t, 0, qp.NumWaiting())

	// add peer4
	require.True(t, qp.TryAdd(peer4, oracle))
	cl := qp.GetClosestNInStates(2, PeerHeard, PeerWaiting, PeerQueried)
	require.Equal(t, []peer.ID{peer4, peer2}, cl)
	cl = qp.GetClosestNInStates(3, PeerHeard, PeerWaiting, PeerQueried)
	require.Equal(t, []peer.ID{peer4, peer2}, cl)
	cl = qp.GetClosestNInStates(1, PeerHeard, PeerWaiting, PeerQueried)
	require.Equal(t, []peer.ID{peer4}, cl)

	// mark as unreachable & try to get it
	qp.SetState(peer4, PeerUnreachable)
	cl = qp.GetClosestNInStates(1, PeerHeard, PeerWaiting, PeerQueried)
	require.Equal(t, []peer.ID{peer2}, cl)

	// add peer1
	require.True(t, qp.TryAdd(peer1, oracle))
	cl = qp.GetClosestNInStates(1, PeerHeard, PeerWaiting, PeerQueried)
	require.Equal(t, []peer.ID{peer1}, cl)
	cl = qp.GetClosestNInStates(2, PeerHeard, PeerWaiting, PeerQueried)
	require.Equal(t, []peer.ID{peer1, peer2}, cl)

	// mark as waiting and assert
	qp.SetState(peer2, PeerWaiting)
	require.Equal(t, []peer.ID{peer2}, qp.GetClosestInStates(PeerWaiting))

	require.Equal(t, []peer.ID{peer1}, qp.GetClosestInStates(PeerHeard))
	require.True(t, qp.TryAdd(peer3, oracle))
	require.Equal(t, []peer.ID{peer3, peer1}, qp.GetClosestInStates(PeerHeard))
	require.Equal(t, 2, qp.NumHeard())

	// sets for different keys cannot intersect
	otherqp := NewQueryPeerset("other key")
	require.True(t, otherqp.TryAdd(peer2, oracle))
	require.Len(t, qp.GetIntersection(otherqp), 0)

	// Initialize another query peer set and assert intersection
	otherqp = NewQueryPeerset(key)
	require.Len(t, qp.GetIntersection(otherqp), 0)
	require.True(t, otherqp.TryAdd(peer2, oracle))
	require.Len(t, qp.GetIntersection(otherqp), 1)
	require.Len(t, otherqp.GetIntersection(qp), 1)
	require.Equal(t, []peer.ID{peer2}, qp.GetIntersection(otherqp))

	// sets for different keys cannot intersect
	yetanotherqp := NewQueryPeerset(key)
	require.True(t, yetanotherqp.TryAdd(peer2, oracle))
	require.Len(t, yetanotherqp.GetIntersection(otherqp), 1)
	require.True(t, yetanotherqp.TryAdd(peer3, oracle))
	require.True(t, yetanotherqp.TryAdd(peer4, oracle))
	require.True(t, otherqp.TryAdd(peer4, oracle))
	require.Len(t, qp.GetIntersection(otherqp, yetanotherqp), 2)
	require.True(t, otherqp.TryAdd(peer3, oracle))
	require.Len(t, qp.GetIntersection(otherqp, yetanotherqp), 3)
}

func TestQueryPeerset_GetIntersectionNotInState(t *testing.T) {
	key := "test"
	qp1 := NewQueryPeerset(key)
	qp2 := NewQueryPeerset(key)
	qp3 := NewQueryPeerset(key)

	oracle := test.RandPeerIDFatal(t)
	peer1 := test.RandPeerIDFatal(t)
	peer2 := test.RandPeerIDFatal(t)

	require.True(t, qp1.TryAdd(peer1, oracle))
	require.True(t, qp2.TryAdd(peer1, oracle))

	require.Len(t, qp1.GetIntersectionNotInState(PeerHeard, qp2), 0)
	require.Len(t, qp1.GetIntersectionNotInState(PeerUnreachable, qp2), 1)
	require.Len(t, qp1.GetIntersectionNotInState(PeerUnreachable, qp2, qp3), 0)
	require.True(t, qp3.TryAdd(peer1, oracle))
	require.Len(t, qp1.GetIntersectionNotInState(PeerUnreachable, qp2, qp3), 1)
	require.True(t, qp1.TryAdd(peer2, oracle))
	require.Len(t, qp1.GetIntersectionNotInState(PeerUnreachable, qp2, qp3), 1)
	require.True(t, qp2.TryAdd(peer2, oracle))
	require.True(t, qp3.TryAdd(peer2, oracle))
	require.Len(t, qp1.GetIntersectionNotInState(PeerUnreachable, qp2, qp3), 2)
	require.Len(t, qp1.GetIntersectionNotInState(PeerHeard, qp2, qp3), 0)
}
