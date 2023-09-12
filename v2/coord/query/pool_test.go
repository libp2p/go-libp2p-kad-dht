package query

import (
	"context"
	"testing"

	"github.com/benbjohnson/clock"
	"github.com/plprobelab/go-kademlia/kad"
	"github.com/plprobelab/go-kademlia/key"
	"github.com/stretchr/testify/require"

	"github.com/libp2p/go-libp2p-kad-dht/v2/coord/internal/dtype"
)

func TestPoolConfigValidate(t *testing.T) {
	t.Run("default is valid", func(t *testing.T) {
		cfg := DefaultPoolConfig()
		require.NoError(t, cfg.Validate())
	})

	t.Run("clock is not nil", func(t *testing.T) {
		cfg := DefaultPoolConfig()
		cfg.Clock = nil
		require.Error(t, cfg.Validate())
	})

	t.Run("concurrency positive", func(t *testing.T) {
		cfg := DefaultPoolConfig()
		cfg.Concurrency = 0
		require.Error(t, cfg.Validate())
		cfg.Concurrency = -1
		require.Error(t, cfg.Validate())
	})

	t.Run("timeout positive", func(t *testing.T) {
		cfg := DefaultPoolConfig()
		cfg.Timeout = 0
		require.Error(t, cfg.Validate())
		cfg.Timeout = -1
		require.Error(t, cfg.Validate())
	})

	t.Run("replication positive", func(t *testing.T) {
		cfg := DefaultPoolConfig()
		cfg.Replication = 0
		require.Error(t, cfg.Validate())
		cfg.Replication = -1
		require.Error(t, cfg.Validate())
	})

	t.Run("query concurrency positive", func(t *testing.T) {
		cfg := DefaultPoolConfig()
		cfg.QueryConcurrency = 0
		require.Error(t, cfg.Validate())
		cfg.QueryConcurrency = -1
		require.Error(t, cfg.Validate())
	})

	t.Run("request timeout positive", func(t *testing.T) {
		cfg := DefaultPoolConfig()
		cfg.RequestTimeout = 0
		require.Error(t, cfg.Validate())
		cfg.RequestTimeout = -1
		require.Error(t, cfg.Validate())
	})
}

func TestPoolStartsIdle(t *testing.T) {
	ctx := context.Background()
	clk := clock.NewMock()
	cfg := DefaultPoolConfig()
	cfg.Clock = clk

	self := dtype.NewID(key.Key8(0))
	p, err := NewPool[key.Key8](self, cfg)
	require.NoError(t, err)

	state := p.Advance(ctx, &EventPoolPoll{})
	require.IsType(t, &StatePoolIdle{}, state)
}

func TestPoolStopWhenNoQueries(t *testing.T) {
	ctx := context.Background()
	clk := clock.NewMock()
	cfg := DefaultPoolConfig()
	cfg.Clock = clk

	self := dtype.NewID(key.Key8(0))
	p, err := NewPool[key.Key8](self, cfg)
	require.NoError(t, err)

	state := p.Advance(ctx, &EventPoolPoll{})
	require.IsType(t, &StatePoolIdle{}, state)
}

func TestPoolAddQueryStartsIfCapacity(t *testing.T) {
	ctx := context.Background()
	clk := clock.NewMock()
	cfg := DefaultPoolConfig()
	cfg.Clock = clk

	self := dtype.NewID(key.Key8(0))
	p, err := NewPool[key.Key8](self, cfg)
	require.NoError(t, err)

	target := key.Key8(0b00000001)
	a := dtype.NewID(key.Key8(0b00000100)) // 4

	queryID := QueryID("test")

	// first thing the new pool should do is start the query
	state := p.Advance(ctx, &EventPoolAddQuery[key.Key8]{
		QueryID:           queryID,
		Target:            target,
		KnownClosestNodes: []kad.NodeID[key.Key8]{a},
	})
	require.IsType(t, &StatePoolQueryFindCloser[key.Key8]{}, state)

	// the query should attempt to contact the node it was given
	st := state.(*StatePoolQueryFindCloser[key.Key8])

	// the query should be the one just added
	require.Equal(t, queryID, st.QueryID)

	// the query should attempt to contact the node it was given
	require.Equal(t, a, st.NodeID)

	// with the correct target
	require.True(t, key.Equal(target, st.Target))

	// now the pool reports that it is waiting
	state = p.Advance(ctx, &EventPoolPoll{})
	require.IsType(t, &StatePoolWaitingWithCapacity{}, state)
}

func TestPoolMessageResponse(t *testing.T) {
	ctx := context.Background()
	clk := clock.NewMock()
	cfg := DefaultPoolConfig()
	cfg.Clock = clk

	self := dtype.NewID(key.Key8(0))
	p, err := NewPool[key.Key8](self, cfg)
	require.NoError(t, err)

	target := key.Key8(0b00000001)
	a := dtype.NewID(key.Key8(0b00000100)) // 4

	queryID := QueryID("test")

	// first thing the new pool should do is start the query
	state := p.Advance(ctx, &EventPoolAddQuery[key.Key8]{
		QueryID:           queryID,
		Target:            target,
		KnownClosestNodes: []kad.NodeID[key.Key8]{a},
	})
	require.IsType(t, &StatePoolQueryFindCloser[key.Key8]{}, state)

	// the query should attempt to contact the node it was given
	st := state.(*StatePoolQueryFindCloser[key.Key8])
	require.Equal(t, queryID, st.QueryID)
	require.Equal(t, a, st.NodeID)

	// notify query that node was contacted successfully, but no closer nodes
	state = p.Advance(ctx, &EventPoolMessageResponse[key.Key8]{
		QueryID: queryID,
		NodeID:  a,
	})

	// pool should respond that query has finished
	require.IsType(t, &StatePoolQueryFinished{}, state)

	stf := state.(*StatePoolQueryFinished)
	require.Equal(t, queryID, stf.QueryID)
	require.Equal(t, 1, stf.Stats.Requests)
	require.Equal(t, 1, stf.Stats.Success)
}

func TestPoolPrefersRunningQueriesOverNewOnes(t *testing.T) {
	ctx := context.Background()
	clk := clock.NewMock()
	cfg := DefaultPoolConfig()
	cfg.Clock = clk
	cfg.Concurrency = 2 // allow two queries to run concurrently

	self := dtype.NewID(key.Key8(0))
	p, err := NewPool[key.Key8](self, cfg)
	require.NoError(t, err)

	target := key.Key8(0b00000001)
	a := dtype.NewID(key.Key8(0b00000100)) // 4
	b := dtype.NewID(key.Key8(0b00001000)) // 8
	c := dtype.NewID(key.Key8(0b00010000)) // 16
	d := dtype.NewID(key.Key8(0b00100000)) // 32

	// Add the first query
	queryID1 := QueryID("1")
	state := p.Advance(ctx, &EventPoolAddQuery[key.Key8]{
		QueryID:           queryID1,
		Target:            target,
		KnownClosestNodes: []kad.NodeID[key.Key8]{a, b, c, d},
	})
	require.IsType(t, &StatePoolQueryFindCloser[key.Key8]{}, state)

	// the first query should attempt to contact the node it was given
	st := state.(*StatePoolQueryFindCloser[key.Key8])
	require.Equal(t, queryID1, st.QueryID)
	require.Equal(t, a, st.NodeID)

	// Add the second query
	queryID2 := QueryID("2")
	state = p.Advance(ctx, &EventPoolAddQuery[key.Key8]{
		QueryID:           queryID2,
		Target:            target,
		KnownClosestNodes: []kad.NodeID[key.Key8]{a, b, c, d},
	})

	// the first query should continue its operation in preference to starting the new query
	require.IsType(t, &StatePoolQueryFindCloser[key.Key8]{}, state)
	st = state.(*StatePoolQueryFindCloser[key.Key8])
	require.Equal(t, queryID1, st.QueryID)
	require.Equal(t, b, st.NodeID)

	// advance the pool again, the first query should continue its operation in preference to starting the new query
	state = p.Advance(ctx, &EventPoolPoll{})
	require.IsType(t, &StatePoolQueryFindCloser[key.Key8]{}, state)
	st = state.(*StatePoolQueryFindCloser[key.Key8])
	require.Equal(t, queryID1, st.QueryID)
	require.Equal(t, c, st.NodeID)

	// advance the pool again, the first query is at capacity so the second query can start
	state = p.Advance(ctx, &EventPoolPoll{})
	require.IsType(t, &StatePoolQueryFindCloser[key.Key8]{}, state)
	st = state.(*StatePoolQueryFindCloser[key.Key8])
	require.Equal(t, queryID2, st.QueryID)
	require.Equal(t, a, st.NodeID)

	// notify first query that node was contacted successfully, but no closer nodes
	state = p.Advance(ctx, &EventPoolMessageResponse[key.Key8]{
		QueryID: queryID1,
		NodeID:  a,
	})

	// first query starts a new message request
	require.IsType(t, &StatePoolQueryFindCloser[key.Key8]{}, state)
	st = state.(*StatePoolQueryFindCloser[key.Key8])
	require.Equal(t, queryID1, st.QueryID)
	require.Equal(t, d, st.NodeID)

	// notify first query that next node was contacted successfully, but no closer nodes
	state = p.Advance(ctx, &EventPoolMessageResponse[key.Key8]{
		QueryID: queryID1,
		NodeID:  b,
	})

	// first query is out of nodes to try so second query can proceed
	require.IsType(t, &StatePoolQueryFindCloser[key.Key8]{}, state)
	st = state.(*StatePoolQueryFindCloser[key.Key8])
	require.Equal(t, queryID2, st.QueryID)
	require.Equal(t, b, st.NodeID)
}

func TestPoolRespectsConcurrency(t *testing.T) {
	ctx := context.Background()
	clk := clock.NewMock()
	cfg := DefaultPoolConfig()
	cfg.Clock = clk
	cfg.Concurrency = 2      // allow two queries to run concurrently
	cfg.QueryConcurrency = 1 // allow each query to have a single request in flight

	self := dtype.NewID(key.Key8(0))
	p, err := NewPool[key.Key8](self, cfg)
	require.NoError(t, err)

	target := key.Key8(0b00000001)
	a := dtype.NewID(key.Key8(0b00000100)) // 4

	// Add the first query
	queryID1 := QueryID("1")
	state := p.Advance(ctx, &EventPoolAddQuery[key.Key8]{
		QueryID:           queryID1,
		Target:            target,
		KnownClosestNodes: []kad.NodeID[key.Key8]{a},
	})
	require.IsType(t, &StatePoolQueryFindCloser[key.Key8]{}, state)

	// the first query should attempt to contact the node it was given
	st := state.(*StatePoolQueryFindCloser[key.Key8])
	require.Equal(t, queryID1, st.QueryID)
	require.Equal(t, a, st.NodeID)

	// Add the second query
	queryID2 := QueryID("2")
	state = p.Advance(ctx, &EventPoolAddQuery[key.Key8]{
		QueryID:           queryID2,
		Target:            target,
		KnownClosestNodes: []kad.NodeID[key.Key8]{a},
	})

	// the second query should start since the first query has a request in flight
	require.IsType(t, &StatePoolQueryFindCloser[key.Key8]{}, state)
	st = state.(*StatePoolQueryFindCloser[key.Key8])
	require.Equal(t, queryID2, st.QueryID)
	require.Equal(t, a, st.NodeID)

	// Add a third query
	queryID3 := QueryID("3")
	state = p.Advance(ctx, &EventPoolAddQuery[key.Key8]{
		QueryID:           queryID3,
		Target:            target,
		KnownClosestNodes: []kad.NodeID[key.Key8]{a},
	})

	// the third query should wait since the pool has reached maximum concurrency
	require.IsType(t, &StatePoolWaitingAtCapacity{}, state)

	// notify first query that next node was contacted successfully, but no closer nodes
	state = p.Advance(ctx, &EventPoolMessageResponse[key.Key8]{
		QueryID: queryID1,
		NodeID:  a,
	})

	// first query is out of nodes so it has finished
	require.IsType(t, &StatePoolQueryFinished{}, state)
	stf := state.(*StatePoolQueryFinished)
	require.Equal(t, queryID1, stf.QueryID)

	// advancing pool again allows query 3 to start
	state = p.Advance(ctx, &EventPoolPoll{})
	require.IsType(t, &StatePoolQueryFindCloser[key.Key8]{}, state)
	st = state.(*StatePoolQueryFindCloser[key.Key8])
	require.Equal(t, queryID3, st.QueryID)
	require.Equal(t, a, st.NodeID)
}
