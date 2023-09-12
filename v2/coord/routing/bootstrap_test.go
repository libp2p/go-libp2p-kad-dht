package routing

import (
	"context"
	"testing"

	"github.com/benbjohnson/clock"
	"github.com/plprobelab/go-kademlia/kad"
	"github.com/plprobelab/go-kademlia/key"
	"github.com/plprobelab/go-kademlia/network/address"
	"github.com/stretchr/testify/require"

	"github.com/libp2p/go-libp2p-kad-dht/v2/coord/internal/dtype"
	"github.com/libp2p/go-libp2p-kad-dht/v2/coord/query"
)

func TestBootstrapConfigValidate(t *testing.T) {
	t.Run("default is valid", func(t *testing.T) {
		cfg := DefaultBootstrapConfig[key.Key8]()
		require.NoError(t, cfg.Validate())
	})

	t.Run("clock is not nil", func(t *testing.T) {
		cfg := DefaultBootstrapConfig[key.Key8]()
		cfg.Clock = nil
		require.Error(t, cfg.Validate())
	})

	t.Run("timeout positive", func(t *testing.T) {
		cfg := DefaultBootstrapConfig[key.Key8]()
		cfg.Timeout = 0
		require.Error(t, cfg.Validate())
		cfg.Timeout = -1
		require.Error(t, cfg.Validate())
	})

	t.Run("request concurrency positive", func(t *testing.T) {
		cfg := DefaultBootstrapConfig[key.Key8]()
		cfg.RequestConcurrency = 0
		require.Error(t, cfg.Validate())
		cfg.RequestConcurrency = -1
		require.Error(t, cfg.Validate())
	})

	t.Run("request timeout positive", func(t *testing.T) {
		cfg := DefaultBootstrapConfig[key.Key8]()
		cfg.RequestTimeout = 0
		require.Error(t, cfg.Validate())
		cfg.RequestTimeout = -1
		require.Error(t, cfg.Validate())
	})
}

func TestBootstrapStartsIdle(t *testing.T) {
	ctx := context.Background()
	clk := clock.NewMock()
	cfg := DefaultBootstrapConfig[key.Key8]()
	cfg.Clock = clk

	self := dtype.NewID(key.Key8(0))
	bs, err := NewBootstrap[key.Key8](self, cfg)
	require.NoError(t, err)

	state := bs.Advance(ctx, &EventBootstrapPoll{})
	require.IsType(t, &StateBootstrapIdle{}, state)
}

func TestBootstrapStart(t *testing.T) {
	ctx := context.Background()
	clk := clock.NewMock()
	cfg := DefaultBootstrapConfig[key.Key8]()
	cfg.Clock = clk

	self := dtype.NewID(key.Key8(0))
	bs, err := NewBootstrap[key.Key8](self, cfg)
	require.NoError(t, err)

	a := dtype.NewID(key.Key8(0b00000100)) // 4

	protocolID := address.ProtocolID("testprotocol")

	// start the bootstrap
	state := bs.Advance(ctx, &EventBootstrapStart[key.Key8]{
		ProtocolID:        protocolID,
		KnownClosestNodes: []kad.NodeID[key.Key8]{a},
	})
	require.IsType(t, &StateBootstrapFindCloser[key.Key8]{}, state)

	// the query should attempt to contact the node it was given
	st := state.(*StateBootstrapFindCloser[key.Key8])

	// the query should be the one just added
	require.Equal(t, query.QueryID("bootstrap"), st.QueryID)

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
	cfg := DefaultBootstrapConfig[key.Key8]()
	cfg.Clock = clk

	self := dtype.NewID(key.Key8(0))
	bs, err := NewBootstrap[key.Key8](self, cfg)
	require.NoError(t, err)

	a := dtype.NewID(key.Key8(0b00000100)) // 4

	protocolID := address.ProtocolID("testprotocol")

	// start the bootstrap
	state := bs.Advance(ctx, &EventBootstrapStart[key.Key8]{
		ProtocolID:        protocolID,
		KnownClosestNodes: []kad.NodeID[key.Key8]{a},
	})
	require.IsType(t, &StateBootstrapFindCloser[key.Key8]{}, state)

	// the bootstrap should attempt to contact the node it was given
	st := state.(*StateBootstrapFindCloser[key.Key8])
	require.Equal(t, query.QueryID("bootstrap"), st.QueryID)
	require.Equal(t, a, st.NodeID)

	// notify bootstrap that node was contacted successfully, but no closer nodes
	state = bs.Advance(ctx, &EventBootstrapFindCloserResponse[key.Key8]{
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
	cfg := DefaultBootstrapConfig[key.Key8]()
	cfg.Clock = clk
	cfg.RequestConcurrency = 3 // 1 less than the 4 nodes to be visited

	self := dtype.NewID(key.Key8(0))
	bs, err := NewBootstrap[key.Key8](self, cfg)
	require.NoError(t, err)

	a := dtype.NewID(key.Key8(0b00000100)) // 4
	b := dtype.NewID(key.Key8(0b00001000)) // 8
	c := dtype.NewID(key.Key8(0b00010000)) // 16
	d := dtype.NewID(key.Key8(0b00100000)) // 32

	// ensure the order of the known nodes
	require.True(t, self.Key().Xor(a.Key()).Compare(self.Key().Xor(b.Key())) == -1)
	require.True(t, self.Key().Xor(b.Key()).Compare(self.Key().Xor(c.Key())) == -1)
	require.True(t, self.Key().Xor(c.Key()).Compare(self.Key().Xor(d.Key())) == -1)

	protocolID := address.ProtocolID("testprotocol")

	// start the bootstrap
	state := bs.Advance(ctx, &EventBootstrapStart[key.Key8]{
		ProtocolID:        protocolID,
		KnownClosestNodes: []kad.NodeID[key.Key8]{d, a, b, c},
	})

	// the bootstrap should attempt to contact the closest node it was given
	require.IsType(t, &StateBootstrapFindCloser[key.Key8]{}, state)
	st := state.(*StateBootstrapFindCloser[key.Key8])
	require.Equal(t, query.QueryID("bootstrap"), st.QueryID)
	require.Equal(t, a, st.NodeID)

	// next the bootstrap attempts to contact second nearest node
	state = bs.Advance(ctx, &EventBootstrapPoll{})
	require.IsType(t, &StateBootstrapFindCloser[key.Key8]{}, state)
	st = state.(*StateBootstrapFindCloser[key.Key8])
	require.Equal(t, b, st.NodeID)

	// next the bootstrap attempts to contact third nearest node
	state = bs.Advance(ctx, &EventBootstrapPoll{})
	require.IsType(t, &StateBootstrapFindCloser[key.Key8]{}, state)
	st = state.(*StateBootstrapFindCloser[key.Key8])
	require.Equal(t, c, st.NodeID)

	// now the bootstrap should be waiting since it is at request capacity
	state = bs.Advance(ctx, &EventBootstrapPoll{})
	require.IsType(t, &StateBootstrapWaiting{}, state)

	// notify bootstrap that node was contacted successfully, but no closer nodes
	state = bs.Advance(ctx, &EventBootstrapFindCloserResponse[key.Key8]{
		NodeID: a,
	})

	// now the bootstrap has capacity to contact fourth nearest node
	require.IsType(t, &StateBootstrapFindCloser[key.Key8]{}, state)
	st = state.(*StateBootstrapFindCloser[key.Key8])
	require.Equal(t, d, st.NodeID)

	// notify bootstrap that a node was contacted successfully
	state = bs.Advance(ctx, &EventBootstrapFindCloserResponse[key.Key8]{
		NodeID: b,
	})

	// bootstrap should respond that it is waiting for messages
	require.IsType(t, &StateBootstrapWaiting{}, state)

	// notify bootstrap that a node was contacted successfully
	state = bs.Advance(ctx, &EventBootstrapFindCloserResponse[key.Key8]{
		NodeID: c,
	})

	// bootstrap should respond that it is waiting for last message
	require.IsType(t, &StateBootstrapWaiting{}, state)

	// notify bootstrap that the final node was contacted successfully
	state = bs.Advance(ctx, &EventBootstrapFindCloserResponse[key.Key8]{
		NodeID: d,
	})

	// bootstrap should respond that its query has finished
	require.IsType(t, &StateBootstrapFinished{}, state)

	stf := state.(*StateBootstrapFinished)
	require.Equal(t, 4, stf.Stats.Requests)
	require.Equal(t, 4, stf.Stats.Success)
}
