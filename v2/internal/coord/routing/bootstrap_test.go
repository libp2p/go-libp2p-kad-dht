package routing

import (
	"context"
	"testing"

	"github.com/benbjohnson/clock"
	"github.com/plprobelab/go-kademlia/key"
	"github.com/stretchr/testify/require"

	"github.com/libp2p/go-libp2p-kad-dht/v2/internal/coord/coordt"
	"github.com/libp2p/go-libp2p-kad-dht/v2/internal/coord/internal/tiny"
)

func TestBootstrapConfigValidate(t *testing.T) {
	t.Run("default is valid", func(t *testing.T) {
		cfg := DefaultBootstrapConfig()
		require.NoError(t, cfg.Validate())
	})

	t.Run("clock is not nil", func(t *testing.T) {
		cfg := DefaultBootstrapConfig()
		cfg.Clock = nil
		require.Error(t, cfg.Validate())
	})

	t.Run("tracer is not nil", func(t *testing.T) {
		cfg := DefaultBootstrapConfig()
		cfg.Tracer = nil
		require.Error(t, cfg.Validate())
	})

	t.Run("meter is not nil", func(t *testing.T) {
		cfg := DefaultBootstrapConfig()
		cfg.Meter = nil
		require.Error(t, cfg.Validate())
	})

	t.Run("timeout positive", func(t *testing.T) {
		cfg := DefaultBootstrapConfig()
		cfg.Timeout = 0
		require.Error(t, cfg.Validate())
		cfg.Timeout = -1
		require.Error(t, cfg.Validate())
	})

	t.Run("request concurrency positive", func(t *testing.T) {
		cfg := DefaultBootstrapConfig()
		cfg.RequestConcurrency = 0
		require.Error(t, cfg.Validate())
		cfg.RequestConcurrency = -1
		require.Error(t, cfg.Validate())
	})

	t.Run("request timeout positive", func(t *testing.T) {
		cfg := DefaultBootstrapConfig()
		cfg.RequestTimeout = 0
		require.Error(t, cfg.Validate())
		cfg.RequestTimeout = -1
		require.Error(t, cfg.Validate())
	})
}

func TestBootstrapStartsIdle(t *testing.T) {
	ctx := context.Background()
	clk := clock.NewMock()
	cfg := DefaultBootstrapConfig()
	cfg.Clock = clk

	self := tiny.NewNode(0)
	bs, err := NewBootstrap[tiny.Key](self, cfg)
	require.NoError(t, err)

	state := bs.Advance(ctx, &EventBootstrapPoll{})
	require.IsType(t, &StateBootstrapIdle{}, state)
}

func TestBootstrapStart(t *testing.T) {
	ctx := context.Background()
	clk := clock.NewMock()
	cfg := DefaultBootstrapConfig()
	cfg.Clock = clk

	self := tiny.NewNode(0)
	bs, err := NewBootstrap[tiny.Key](self, cfg)
	require.NoError(t, err)

	a := tiny.NewNode(0b00000100) // 4

	// start the bootstrap
	state := bs.Advance(ctx, &EventBootstrapStart[tiny.Key, tiny.Node]{
		KnownClosestNodes: []tiny.Node{a},
	})
	require.IsType(t, &StateBootstrapFindCloser[tiny.Key, tiny.Node]{}, state)

	// the query should attempt to contact the node it was given
	st := state.(*StateBootstrapFindCloser[tiny.Key, tiny.Node])

	// the query should be the one just added
	require.Equal(t, coordt.QueryID("bootstrap"), st.QueryID)

	// the query should attempt to contact the node it was given
	require.Equal(t, a, st.NodeID)

	// with the correct key
	require.True(t, key.Equal(self.Key(), st.Target))

	// now the bootstrap reports that it is waiting
	state = bs.Advance(ctx, &EventBootstrapPoll{})
	require.IsType(t, &StateBootstrapWaiting{}, state)
}

func TestBootstrapMessageResponse(t *testing.T) {
	ctx := context.Background()
	clk := clock.NewMock()
	cfg := DefaultBootstrapConfig()
	cfg.Clock = clk

	self := tiny.NewNode(0)
	bs, err := NewBootstrap[tiny.Key](self, cfg)
	require.NoError(t, err)

	a := tiny.NewNode(0b00000100) // 4

	// start the bootstrap
	state := bs.Advance(ctx, &EventBootstrapStart[tiny.Key, tiny.Node]{
		KnownClosestNodes: []tiny.Node{a},
	})
	require.IsType(t, &StateBootstrapFindCloser[tiny.Key, tiny.Node]{}, state)

	// the bootstrap should attempt to contact the node it was given
	st := state.(*StateBootstrapFindCloser[tiny.Key, tiny.Node])
	require.Equal(t, coordt.QueryID("bootstrap"), st.QueryID)
	require.Equal(t, a, st.NodeID)

	// notify bootstrap that node was contacted successfully, but no closer nodes
	state = bs.Advance(ctx, &EventBootstrapFindCloserResponse[tiny.Key, tiny.Node]{
		NodeID: a,
	})

	// bootstrap should respond that its query has finished
	require.IsType(t, &StateBootstrapFinished{}, state)

	stf := state.(*StateBootstrapFinished)
	require.Equal(t, 1, stf.Stats.Requests)
	require.Equal(t, 1, stf.Stats.Success)
}

func TestBootstrapProgress(t *testing.T) {
	ctx := context.Background()
	clk := clock.NewMock()
	cfg := DefaultBootstrapConfig()
	cfg.Clock = clk
	cfg.RequestConcurrency = 3 // 1 less than the 4 nodes to be visited

	self := tiny.NewNode(0)
	bs, err := NewBootstrap[tiny.Key](self, cfg)
	require.NoError(t, err)

	a := tiny.NewNode(0b00000100) // 4
	b := tiny.NewNode(0b00001000) // 8
	c := tiny.NewNode(0b00010000) // 16
	d := tiny.NewNode(0b00100000) // 32

	// ensure the order of the known nodes
	require.True(t, self.Key().Xor(a.Key()).Compare(self.Key().Xor(b.Key())) == -1)
	require.True(t, self.Key().Xor(b.Key()).Compare(self.Key().Xor(c.Key())) == -1)
	require.True(t, self.Key().Xor(c.Key()).Compare(self.Key().Xor(d.Key())) == -1)

	// start the bootstrap
	state := bs.Advance(ctx, &EventBootstrapStart[tiny.Key, tiny.Node]{
		KnownClosestNodes: []tiny.Node{d, a, b, c},
	})

	// the bootstrap should attempt to contact the closest node it was given
	require.IsType(t, &StateBootstrapFindCloser[tiny.Key, tiny.Node]{}, state)
	st := state.(*StateBootstrapFindCloser[tiny.Key, tiny.Node])
	require.Equal(t, coordt.QueryID("bootstrap"), st.QueryID)
	require.Equal(t, a, st.NodeID)

	// next the bootstrap attempts to contact second nearest node
	state = bs.Advance(ctx, &EventBootstrapPoll{})
	require.IsType(t, &StateBootstrapFindCloser[tiny.Key, tiny.Node]{}, state)
	st = state.(*StateBootstrapFindCloser[tiny.Key, tiny.Node])
	require.Equal(t, b, st.NodeID)

	// next the bootstrap attempts to contact third nearest node
	state = bs.Advance(ctx, &EventBootstrapPoll{})
	require.IsType(t, &StateBootstrapFindCloser[tiny.Key, tiny.Node]{}, state)
	st = state.(*StateBootstrapFindCloser[tiny.Key, tiny.Node])
	require.Equal(t, c, st.NodeID)

	// now the bootstrap should be waiting since it is at request capacity
	state = bs.Advance(ctx, &EventBootstrapPoll{})
	require.IsType(t, &StateBootstrapWaiting{}, state)

	// notify bootstrap that node was contacted successfully, but no closer nodes
	state = bs.Advance(ctx, &EventBootstrapFindCloserResponse[tiny.Key, tiny.Node]{
		NodeID: a,
	})

	// now the bootstrap has capacity to contact fourth nearest node
	require.IsType(t, &StateBootstrapFindCloser[tiny.Key, tiny.Node]{}, state)
	st = state.(*StateBootstrapFindCloser[tiny.Key, tiny.Node])
	require.Equal(t, d, st.NodeID)

	// notify bootstrap that a node was contacted successfully
	state = bs.Advance(ctx, &EventBootstrapFindCloserResponse[tiny.Key, tiny.Node]{
		NodeID: b,
	})

	// bootstrap should respond that it is waiting for messages
	require.IsType(t, &StateBootstrapWaiting{}, state)

	// notify bootstrap that a node was contacted successfully
	state = bs.Advance(ctx, &EventBootstrapFindCloserResponse[tiny.Key, tiny.Node]{
		NodeID: c,
	})

	// bootstrap should respond that it is waiting for last message
	require.IsType(t, &StateBootstrapWaiting{}, state)

	// notify bootstrap that the final node was contacted successfully
	state = bs.Advance(ctx, &EventBootstrapFindCloserResponse[tiny.Key, tiny.Node]{
		NodeID: d,
	})

	// bootstrap should respond that its query has finished
	require.IsType(t, &StateBootstrapFinished{}, state)

	stf := state.(*StateBootstrapFinished)
	require.Equal(t, 4, stf.Stats.Requests)
	require.Equal(t, 4, stf.Stats.Success)
}
