package brdcst

import (
	"context"
	"fmt"
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

func TestPool_EventPoolAddBroadcast_FollowUp_lifecycle(t *testing.T) {
	// This test attempts to cover the whole lifecycle of
	// a follow-up broadcast operation.
	//
	// We have a network of three peers: a, b, and, c
	// First, we query all three while peer c fails to respond
	// Second, we store the record with the remaining a and b, while b fails to respond

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
	b := tiny.NewNode(0b00000011) // 3
	c := tiny.NewNode(0b00000010) // 2

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
		Target:      target,
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
		Target:      target,
		NodeID:      b,
		CloserNodes: []tiny.Node{b, c}, // returns additional node
	})

	// the query should attempt to contact the newly closer node it has found
	st, ok = state.(*StatePoolFindCloser[tiny.Key, tiny.Node])
	require.True(t, ok)

	require.Equal(t, queryID, st.QueryID)         // the query should be the same
	require.Equal(t, c, st.NodeID)                // the query should attempt to contact the newly discovered node
	require.True(t, key.Equal(target, st.Target)) // with the correct target

	// this last node times out -> start contacting the other two
	timeoutErr := fmt.Errorf("timeout")
	state = p.Advance(ctx, &EventPoolGetCloserNodesFailure[tiny.Key, tiny.Node]{
		QueryID: queryID,
		NodeID:  c,
		Target:  target,
		Error:   timeoutErr,
	})

	// This means we should start the follow-up phase
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

	// since we have two requests in-flight, polling should return a waiting state machine
	state = p.Advance(ctx, &EventPoolPoll{})
	require.IsType(t, &StatePoolWaiting{}, state)

	// first response from storing the record comes back
	state = p.Advance(ctx, &EventPoolStoreRecordSuccess[tiny.Key, tiny.Node, tiny.Message]{
		QueryID: queryID,
		NodeID:  a,
		Request: msg,
	})
	require.IsType(t, &StatePoolWaiting{}, state)

	// second response from storing the record comes back and it failed!
	state = p.Advance(ctx, &EventPoolStoreRecordFailure[tiny.Key, tiny.Node, tiny.Message]{
		QueryID: queryID,
		NodeID:  b,
		Request: msg,
		Error:   timeoutErr,
	})

	// since we have contacted all nodes we knew, the broadcast has finished
	finishState, ok := state.(*StatePoolBroadcastFinished[tiny.Key, tiny.Node])
	require.True(t, ok)

	require.Equal(t, queryID, finishState.QueryID)
	require.Len(t, finishState.Contacted, 2)
	require.Len(t, finishState.Errors, 1)
	require.Equal(t, finishState.Errors[b.String()].Node, b)
	require.Equal(t, finishState.Errors[b.String()].Err, timeoutErr)

	state = p.Advance(ctx, &EventPoolPoll{})
	require.IsType(t, &StatePoolIdle{}, state)
}
