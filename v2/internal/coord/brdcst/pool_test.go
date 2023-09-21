package brdcst

import (
	"context"
	"testing"

	"github.com/plprobelab/go-kademlia/key"

	"github.com/libp2p/go-libp2p-kad-dht/v2/internal/coord/query"

	"github.com/libp2p/go-libp2p-kad-dht/v2/internal/coord/internal/tiny"
	"github.com/stretchr/testify/require"
)

func TestPoolStopWhenNoQueries(t *testing.T) {
	ctx := context.Background()
	cfg := DefaultPoolConfig()

	self := tiny.NewNode(0)
	qp, err := query.NewPool[tiny.Key, tiny.Node, tiny.Message](self, nil)
	require.NoError(t, err)

	p, err := NewPool[tiny.Key, tiny.Node, tiny.Message](qp, cfg)
	require.NoError(t, err)

	state := p.Advance(ctx, &EventPoolPoll{})
	require.IsType(t, &StatePoolIdle{}, state)
}

func TestPool_EventPoolAddBroadcast_FollowUp_happy_path(t *testing.T) {
	// This test attempts to cover the whole lifecycle of
	// a follow-up broadcast operation.
	ctx := context.Background()
	cfg := DefaultPoolConfig()

	self := tiny.NewNode(0)
	qp, err := query.NewPool[tiny.Key, tiny.Node, tiny.Message](self, nil)
	require.NoError(t, err)

	p, err := NewPool[tiny.Key, tiny.Node, tiny.Message](qp, cfg)
	require.NoError(t, err)

	msg := tiny.Message{Content: "store this"}
	target := tiny.Key(0b00000001)
	a := tiny.NewNode(0b00000100) // 4
	b := tiny.NewNode(0b00000010) // 3

	queryID := query.QueryID("test")

	state := p.Advance(ctx, &EventPoolAddBroadcast[tiny.Key, tiny.Node, tiny.Message]{
		QueryID:           queryID,
		Target:            target,
		Message:           msg,
		KnownClosestNodes: []tiny.Node{a},
		Strategy:          StrategyFollowUp,
	})

	// the query should attempt to contact the node it was given
	st, ok := state.(*StatePoolFindCloser[tiny.Key, tiny.Node])
	require.True(t, ok)

	require.Equal(t, queryID, st.QueryID)         // the query should be the one just added
	require.Equal(t, a, st.NodeID)                // the query should attempt to contact the node it was given
	require.True(t, key.Equal(target, st.Target)) // with the correct target

	// polling the state machine returns waiting
	state = p.Advance(ctx, &EventPoolPoll{})
	require.IsType(t, &StatePoolWaiting{}, state)

	// notify pool that the node was contacted successfully
	// with a single closer node.
	state = p.Advance(ctx, &EventPoolGetCloserNodesSuccess[tiny.Key, tiny.Node]{
		QueryID:     queryID,
		NodeID:      a,
		CloserNodes: []tiny.Node{a, b},
	})

	// the query should attempt to contact the single closer node it has found
	st, ok = state.(*StatePoolFindCloser[tiny.Key, tiny.Node])
	require.True(t, ok)

	require.Equal(t, queryID, st.QueryID)         // the query should be the same
	require.Equal(t, b, st.NodeID)                // the query should attempt to contact the newly discovered node
	require.True(t, key.Equal(target, st.Target)) // with the correct target

	// notify pool that the node was contacted successfully
	// with no new node.
	state = p.Advance(ctx, &EventPoolGetCloserNodesSuccess[tiny.Key, tiny.Node]{
		QueryID:     queryID,
		NodeID:      b,
		CloserNodes: []tiny.Node{a, b},
	})

	// This means we should start the follow-up phase
	// pool should respond that query has finished
	srState, ok := state.(*StatePoolStoreRecord[tiny.Key, tiny.Node, tiny.Message])
	require.True(t, ok)

	require.Equal(t, queryID, srState.QueryID)
	firstContactedNode := srState.NodeID
	require.True(t, a == srState.NodeID || b == srState.NodeID) // we should contact either node - there's no inherent order
	require.Equal(t, msg.Content, srState.Message.Content)

	// polling the state machine should trigger storing the record with
	// the second node
	state = p.Advance(ctx, &EventPoolPoll{})
	srState, ok = state.(*StatePoolStoreRecord[tiny.Key, tiny.Node, tiny.Message])
	require.True(t, ok)

	require.Equal(t, queryID, srState.QueryID)
	require.True(t, a == srState.NodeID || b == srState.NodeID) // we should contact either node - there's no inherent order
	require.NotEqual(t, firstContactedNode, srState.NodeID)     // should be the other one now
	require.Equal(t, msg.Content, srState.Message.Content)

	state = p.Advance(ctx, &EventPoolPoll{})
	require.IsType(t, &StatePoolWaiting{}, state)

	// first response from storing the record comes back
	state = p.Advance(ctx, &EventPoolStoreRecordSuccess[tiny.Key, tiny.Node, tiny.Message]{
		QueryID: queryID,
		NodeID:  a,
		Request: msg,
	})
	require.IsType(t, &StatePoolWaiting{}, state)

	// second response from storing the record comes back
	state = p.Advance(ctx, &EventPoolStoreRecordSuccess[tiny.Key, tiny.Node, tiny.Message]{
		QueryID: queryID,
		NodeID:  b,
		Request: msg,
	})
	finishState, ok := state.(*StatePoolBroadcastFinished)
	require.True(t, ok)

	require.Equal(t, queryID, finishState.QueryID)

	state = p.Advance(ctx, &EventPoolPoll{})
	require.IsType(t, &StatePoolIdle{}, state)
}
