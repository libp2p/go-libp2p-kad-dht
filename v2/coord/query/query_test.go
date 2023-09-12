package query

import (
	"context"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/plprobelab/go-kademlia/kad"
	"github.com/plprobelab/go-kademlia/key"
	"github.com/stretchr/testify/require"

	"github.com/libp2p/go-libp2p-kad-dht/v2/coord/internal/dtype"
)

func TestQueryConfigValidate(t *testing.T) {
	t.Run("default is valid", func(t *testing.T) {
		cfg := DefaultQueryConfig[key.Key8]()
		require.NoError(t, cfg.Validate())
	})

	t.Run("clock is not nil", func(t *testing.T) {
		cfg := DefaultQueryConfig[key.Key8]()
		cfg.Clock = nil
		require.Error(t, cfg.Validate())
	})

	t.Run("request timeout positive", func(t *testing.T) {
		cfg := DefaultQueryConfig[key.Key8]()
		cfg.RequestTimeout = 0
		require.Error(t, cfg.Validate())
		cfg.RequestTimeout = -1
		require.Error(t, cfg.Validate())
	})

	t.Run("concurrency positive", func(t *testing.T) {
		cfg := DefaultQueryConfig[key.Key8]()
		cfg.Concurrency = 0
		require.Error(t, cfg.Validate())
		cfg.Concurrency = -1
		require.Error(t, cfg.Validate())
	})

	t.Run("num results positive", func(t *testing.T) {
		cfg := DefaultQueryConfig[key.Key8]()
		cfg.NumResults = 0
		require.Error(t, cfg.Validate())
		cfg.NumResults = -1
		require.Error(t, cfg.Validate())
	})
}

func TestQueryMessagesNode(t *testing.T) {
	ctx := context.Background()

	target := key.Key8(0b00000001)
	a := dtype.NewID(key.Key8(0b00000100)) // 4

	// one known node to start with
	knownNodes := []kad.NodeID[key.Key8]{a}

	clk := clock.NewMock()

	iter := NewClosestNodesIter(target)

	cfg := DefaultQueryConfig[key.Key8]()
	cfg.Clock = clk

	queryID := QueryID("test")

	self := dtype.NewID(key.Key8(0))
	qry, err := NewQuery[key.Key8](self, queryID, target, iter, knownNodes, cfg)
	require.NoError(t, err)

	// first thing the new query should do is request to send a message to the node
	state := qry.Advance(ctx, nil)
	require.IsType(t, &StateQueryWaitingFindCloser[key.Key8]{}, state)

	// check that we are messaging the correct node with the right message
	st := state.(*StateQueryWaitingFindCloser[key.Key8])
	require.Equal(t, queryID, st.QueryID)
	require.Equal(t, a, st.NodeID)
	require.True(t, key.Equal(target, st.Target))
	require.Equal(t, clk.Now(), st.Stats.Start)
	require.Equal(t, 1, st.Stats.Requests)
	require.Equal(t, 0, st.Stats.Success)

	// advancing now reports that the query is waiting for a response but its underlying query still has capacity
	state = qry.Advance(ctx, nil)
	require.IsType(t, &StateQueryWaitingWithCapacity{}, state)
	stw := state.(*StateQueryWaitingWithCapacity)
	require.Equal(t, 1, stw.Stats.Requests)
	require.Equal(t, 0, st.Stats.Success)
}

func TestQueryMessagesNearest(t *testing.T) {
	ctx := context.Background()

	target := key.Key8(0b00000011)
	far := dtype.NewID(key.Key8(0b11011011))
	near := dtype.NewID(key.Key8(0b00000110))

	// ensure near is nearer to target than far is
	require.Less(t, target.Xor(near.Key()), target.Xor(far.Key()))

	// knownNodes are in "random" order with furthest before nearest
	knownNodes := []kad.NodeID[key.Key8]{
		far,
		near,
	}
	clk := clock.NewMock()

	iter := NewClosestNodesIter(target)

	cfg := DefaultQueryConfig[key.Key8]()
	cfg.Clock = clk

	queryID := QueryID("test")

	self := dtype.NewID(key.Key8(0))
	qry, err := NewQuery[key.Key8](self, queryID, target, iter, knownNodes, cfg)
	require.NoError(t, err)

	// first thing the new query should do is message the nearest node
	state := qry.Advance(ctx, nil)
	require.IsType(t, &StateQueryWaitingFindCloser[key.Key8]{}, state)

	// check that we are contacting the nearest node first
	st := state.(*StateQueryWaitingFindCloser[key.Key8])
	require.Equal(t, near, st.NodeID)
}

func TestQueryCancelFinishesQuery(t *testing.T) {
	ctx := context.Background()

	target := key.Key8(0b00000001)
	a := dtype.NewID(key.Key8(0b00000100)) // 4

	// one known node to start with
	knownNodes := []kad.NodeID[key.Key8]{a}

	clk := clock.NewMock()

	iter := NewClosestNodesIter(target)

	cfg := DefaultQueryConfig[key.Key8]()
	cfg.Clock = clk

	queryID := QueryID("test")

	self := dtype.NewID(key.Key8(0))
	qry, err := NewQuery[key.Key8](self, queryID, target, iter, knownNodes, cfg)
	require.NoError(t, err)

	// first thing the new query should do is request to send a message to the node
	state := qry.Advance(ctx, nil)
	require.IsType(t, &StateQueryWaitingFindCloser[key.Key8]{}, state)

	clk.Add(time.Second)

	// cancel the query
	state = qry.Advance(ctx, &EventQueryCancel{})
	require.IsType(t, &StateQueryFinished{}, state)

	stf := state.(*StateQueryFinished)
	require.Equal(t, 1, stf.Stats.Requests)

	// no successful responses were received before query was cancelled
	require.Equal(t, 0, stf.Stats.Success)

	// no failed responses were received before query was cancelled
	require.Equal(t, 0, stf.Stats.Failure)

	// query should have an end time
	require.Equal(t, clk.Now(), stf.Stats.End)
}

func TestQueryNoClosest(t *testing.T) {
	ctx := context.Background()

	target := key.Key8(0b00000011)

	// no known nodes to start with
	knownNodes := []kad.NodeID[key.Key8]{}

	iter := NewClosestNodesIter(target)

	clk := clock.NewMock()
	cfg := DefaultQueryConfig[key.Key8]()
	cfg.Clock = clk

	queryID := QueryID("test")

	self := dtype.NewID(key.Key8(0))
	qry, err := NewQuery[key.Key8](self, queryID, target, iter, knownNodes, cfg)
	require.NoError(t, err)

	// query is finished because there were no nodes to contat
	state := qry.Advance(ctx, nil)
	require.IsType(t, &StateQueryFinished{}, state)

	stf := state.(*StateQueryFinished)

	// no requests were made
	require.Equal(t, 0, stf.Stats.Requests)

	// no successful responses were received before query was cancelled
	require.Equal(t, 0, stf.Stats.Success)

	// no failed responses were received before query was cancelled
	require.Equal(t, 0, stf.Stats.Failure)

	// query should have an end time
	require.Equal(t, clk.Now(), stf.Stats.End)
}

func TestQueryWaitsAtCapacity(t *testing.T) {
	ctx := context.Background()

	target := key.Key8(0b00000001)
	a := dtype.NewID(key.Key8(0b00000100)) // 4
	b := dtype.NewID(key.Key8(0b00001000)) // 8
	c := dtype.NewID(key.Key8(0b00010000)) // 16

	// one known node to start with
	knownNodes := []kad.NodeID[key.Key8]{a, b, c}

	clk := clock.NewMock()

	iter := NewClosestNodesIter(target)

	cfg := DefaultQueryConfig[key.Key8]()
	cfg.Clock = clk
	cfg.Concurrency = 2

	queryID := QueryID("test")

	self := dtype.NewID(key.Key8(0))
	qry, err := NewQuery[key.Key8](self, queryID, target, iter, knownNodes, cfg)
	require.NoError(t, err)

	// first thing the new query should do is request to send a message to the node
	state := qry.Advance(ctx, nil)
	require.IsType(t, &StateQueryWaitingFindCloser[key.Key8]{}, state)
	st := state.(*StateQueryWaitingFindCloser[key.Key8])
	require.Equal(t, a, st.NodeID)
	require.Equal(t, 1, st.Stats.Requests)

	// advancing sends the message to the next node
	state = qry.Advance(ctx, nil)
	require.IsType(t, &StateQueryWaitingFindCloser[key.Key8]{}, state)
	st = state.(*StateQueryWaitingFindCloser[key.Key8])
	require.Equal(t, b, st.NodeID)
	require.Equal(t, 2, st.Stats.Requests)

	// advancing now reports that the query is waiting at capacity since there are 2 messages in flight
	state = qry.Advance(ctx, nil)
	require.IsType(t, &StateQueryWaitingAtCapacity{}, state)

	stw := state.(*StateQueryWaitingAtCapacity)
	require.Equal(t, 2, stw.Stats.Requests)
}

func TestQueryTimedOutNodeMakesCapacity(t *testing.T) {
	ctx := context.Background()

	target := key.Key8(0b00000001)
	a := dtype.NewID(key.Key8(0b00000100)) // 4
	b := dtype.NewID(key.Key8(0b00001000)) // 8
	c := dtype.NewID(key.Key8(0b00010000)) // 16
	d := dtype.NewID(key.Key8(0b00100000)) // 32

	// ensure the order of the known nodes
	require.True(t, target.Xor(a.Key()).Compare(target.Xor(b.Key())) == -1)
	require.True(t, target.Xor(b.Key()).Compare(target.Xor(c.Key())) == -1)
	require.True(t, target.Xor(c.Key()).Compare(target.Xor(d.Key())) == -1)

	// knownNodes are in "random" order
	knownNodes := []kad.NodeID[key.Key8]{b, c, a, d}

	clk := clock.NewMock()

	iter := NewClosestNodesIter(target)

	cfg := DefaultQueryConfig[key.Key8]()
	cfg.Clock = clk
	cfg.RequestTimeout = 3 * time.Minute
	cfg.Concurrency = len(knownNodes) - 1 // one less than the number of initial nodes

	queryID := QueryID("test")

	self := dtype.NewID(key.Key8(0))
	qry, err := NewQuery[key.Key8](self, queryID, target, iter, knownNodes, cfg)
	require.NoError(t, err)

	// first thing the new query should do is contact the nearest node
	state := qry.Advance(ctx, nil)
	require.IsType(t, &StateQueryWaitingFindCloser[key.Key8]{}, state)
	st := state.(*StateQueryWaitingFindCloser[key.Key8])
	require.Equal(t, a, st.NodeID)
	stwm := state.(*StateQueryWaitingFindCloser[key.Key8])
	require.Equal(t, 1, stwm.Stats.Requests)
	require.Equal(t, 0, stwm.Stats.Success)
	require.Equal(t, 0, stwm.Stats.Failure)

	// advance time by one minute
	clk.Add(time.Minute)

	// while the query has capacity the query should contact the next nearest node
	state = qry.Advance(ctx, nil)
	require.IsType(t, &StateQueryWaitingFindCloser[key.Key8]{}, state)
	st = state.(*StateQueryWaitingFindCloser[key.Key8])
	require.Equal(t, b, st.NodeID)
	stwm = state.(*StateQueryWaitingFindCloser[key.Key8])
	require.Equal(t, 2, stwm.Stats.Requests)
	require.Equal(t, 0, stwm.Stats.Success)
	require.Equal(t, 0, stwm.Stats.Failure)

	// advance time by one minute
	clk.Add(time.Minute)

	// while the query has capacity the query should contact the second nearest node
	state = qry.Advance(ctx, nil)
	require.IsType(t, &StateQueryWaitingFindCloser[key.Key8]{}, state)
	st = state.(*StateQueryWaitingFindCloser[key.Key8])
	require.Equal(t, c, st.NodeID)
	stwm = state.(*StateQueryWaitingFindCloser[key.Key8])
	require.Equal(t, 3, stwm.Stats.Requests)
	require.Equal(t, 0, stwm.Stats.Success)
	require.Equal(t, 0, stwm.Stats.Failure)

	// advance time by one minute
	clk.Add(time.Minute)

	// the query should be at capacity
	state = qry.Advance(ctx, nil)
	require.IsType(t, &StateQueryWaitingAtCapacity{}, state)
	stwa := state.(*StateQueryWaitingAtCapacity)
	require.Equal(t, 3, stwa.Stats.Requests)
	require.Equal(t, 0, stwa.Stats.Success)
	require.Equal(t, 0, stwa.Stats.Failure)

	// advance time by another minute, now at 4 minutes, first node connection attempt should now time out
	clk.Add(time.Minute)

	// the first node request should have timed out, making capacity for the last node to attempt connection
	state = qry.Advance(ctx, nil)
	require.IsType(t, &StateQueryWaitingFindCloser[key.Key8]{}, state)
	st = state.(*StateQueryWaitingFindCloser[key.Key8])
	require.Equal(t, d, st.NodeID)

	stwm = state.(*StateQueryWaitingFindCloser[key.Key8])
	require.Equal(t, 4, stwm.Stats.Requests)
	require.Equal(t, 0, stwm.Stats.Success)
	require.Equal(t, 1, stwm.Stats.Failure)

	// advance time by another minute, now at 5 minutes, second node connection attempt should now time out
	clk.Add(time.Minute)

	// advancing now makes more capacity
	state = qry.Advance(ctx, nil)
	require.IsType(t, &StateQueryWaitingWithCapacity{}, state)

	stww := state.(*StateQueryWaitingWithCapacity)
	require.Equal(t, 4, stww.Stats.Requests)
	require.Equal(t, 0, stww.Stats.Success)
	require.Equal(t, 2, stww.Stats.Failure)
}

func TestQueryMessageResponseMakesCapacity(t *testing.T) {
	ctx := context.Background()

	target := key.Key8(0b00000001)
	a := dtype.NewID(key.Key8(0b00000100)) // 4
	b := dtype.NewID(key.Key8(0b00001000)) // 8
	c := dtype.NewID(key.Key8(0b00010000)) // 16
	d := dtype.NewID(key.Key8(0b00100000)) // 32

	// ensure the order of the known nodes
	require.True(t, target.Xor(a.Key()).Compare(target.Xor(b.Key())) == -1)
	require.True(t, target.Xor(b.Key()).Compare(target.Xor(c.Key())) == -1)
	require.True(t, target.Xor(c.Key()).Compare(target.Xor(d.Key())) == -1)

	// knownNodes are in "random" order
	knownNodes := []kad.NodeID[key.Key8]{b, c, a, d}

	clk := clock.NewMock()

	iter := NewClosestNodesIter(target)

	cfg := DefaultQueryConfig[key.Key8]()
	cfg.Clock = clk
	cfg.Concurrency = len(knownNodes) - 1 // one less than the number of initial nodes

	queryID := QueryID("test")

	self := dtype.NewID(key.Key8(0))
	qry, err := NewQuery[key.Key8](self, queryID, target, iter, knownNodes, cfg)
	require.NoError(t, err)

	// first thing the new query should do is contact the nearest node
	state := qry.Advance(ctx, nil)
	require.IsType(t, &StateQueryWaitingFindCloser[key.Key8]{}, state)
	st := state.(*StateQueryWaitingFindCloser[key.Key8])
	require.Equal(t, a, st.NodeID)
	stwm := state.(*StateQueryWaitingFindCloser[key.Key8])
	require.Equal(t, 1, stwm.Stats.Requests)
	require.Equal(t, 0, stwm.Stats.Success)
	require.Equal(t, 0, stwm.Stats.Failure)

	// while the query has capacity the query should contact the next nearest node
	state = qry.Advance(ctx, nil)
	require.IsType(t, &StateQueryWaitingFindCloser[key.Key8]{}, state)
	st = state.(*StateQueryWaitingFindCloser[key.Key8])
	require.Equal(t, b, st.NodeID)
	stwm = state.(*StateQueryWaitingFindCloser[key.Key8])
	require.Equal(t, 2, stwm.Stats.Requests)
	require.Equal(t, 0, stwm.Stats.Success)
	require.Equal(t, 0, stwm.Stats.Failure)

	// while the query has capacity the query should contact the second nearest node
	state = qry.Advance(ctx, nil)
	require.IsType(t, &StateQueryWaitingFindCloser[key.Key8]{}, state)
	st = state.(*StateQueryWaitingFindCloser[key.Key8])
	require.Equal(t, c, st.NodeID)
	stwm = state.(*StateQueryWaitingFindCloser[key.Key8])
	require.Equal(t, 3, stwm.Stats.Requests)
	require.Equal(t, 0, stwm.Stats.Success)
	require.Equal(t, 0, stwm.Stats.Failure)

	// the query should be at capacity
	state = qry.Advance(ctx, nil)
	require.IsType(t, &StateQueryWaitingAtCapacity{}, state)

	// notify query that first node was contacted successfully, now node d can be contacted
	state = qry.Advance(ctx, &EventQueryMessageResponse[key.Key8]{NodeID: a})
	require.IsType(t, &StateQueryWaitingFindCloser[key.Key8]{}, state)
	st = state.(*StateQueryWaitingFindCloser[key.Key8])
	require.Equal(t, d, st.NodeID)
	stwm = state.(*StateQueryWaitingFindCloser[key.Key8])
	require.Equal(t, 4, stwm.Stats.Requests)
	require.Equal(t, 1, stwm.Stats.Success)
	require.Equal(t, 0, stwm.Stats.Failure)

	// the query should be at capacity again
	state = qry.Advance(ctx, nil)
	require.IsType(t, &StateQueryWaitingAtCapacity{}, state)
	stwa := state.(*StateQueryWaitingAtCapacity)
	require.Equal(t, 4, stwa.Stats.Requests)
	require.Equal(t, 1, stwa.Stats.Success)
	require.Equal(t, 0, stwa.Stats.Failure)
}

func TestQueryCloserNodesAreAddedToIteration(t *testing.T) {
	ctx := context.Background()

	target := key.Key8(0b00000001)
	a := dtype.NewID(key.Key8(0b00000100)) // 4
	b := dtype.NewID(key.Key8(0b00001000)) // 8
	c := dtype.NewID(key.Key8(0b00010000)) // 16
	d := dtype.NewID(key.Key8(0b00100000)) // 32

	// ensure the order of the known nodes
	require.True(t, target.Xor(a.Key()).Compare(target.Xor(b.Key())) == -1)
	require.True(t, target.Xor(b.Key()).Compare(target.Xor(c.Key())) == -1)
	require.True(t, target.Xor(c.Key()).Compare(target.Xor(d.Key())) == -1)

	// one known node to start with
	knownNodes := []kad.NodeID[key.Key8]{d}

	clk := clock.NewMock()

	iter := NewClosestNodesIter(target)

	cfg := DefaultQueryConfig[key.Key8]()
	cfg.Clock = clk
	cfg.Concurrency = 2

	queryID := QueryID("test")

	self := dtype.NewID(key.Key8(0))
	qry, err := NewQuery[key.Key8](self, queryID, target, iter, knownNodes, cfg)
	require.NoError(t, err)

	// first thing the new query should do is contact the first node
	state := qry.Advance(ctx, nil)
	require.IsType(t, &StateQueryWaitingFindCloser[key.Key8]{}, state)
	st := state.(*StateQueryWaitingFindCloser[key.Key8])
	require.Equal(t, d, st.NodeID)

	// advancing reports query has capacity
	state = qry.Advance(ctx, nil)
	require.IsType(t, &StateQueryWaitingWithCapacity{}, state)

	// notify query that first node was contacted successfully, with closer nodes
	state = qry.Advance(ctx, &EventQueryMessageResponse[key.Key8]{
		NodeID: d,
		CloserNodes: []kad.NodeID[key.Key8]{
			b,
			a,
		},
	})
	require.IsType(t, &StateQueryWaitingFindCloser[key.Key8]{}, state)

	// query should contact the next nearest uncontacted node
	st = state.(*StateQueryWaitingFindCloser[key.Key8])
	require.Equal(t, a, st.NodeID)
}

func TestQueryCloserNodesIgnoresDuplicates(t *testing.T) {
	ctx := context.Background()

	target := key.Key8(0b00000001)
	a := dtype.NewID(key.Key8(0b00000100)) // 4
	b := dtype.NewID(key.Key8(0b00001000)) // 8
	c := dtype.NewID(key.Key8(0b00010000)) // 16
	d := dtype.NewID(key.Key8(0b00100000)) // 32

	// ensure the order of the known nodes
	require.True(t, target.Xor(a.Key()).Compare(target.Xor(b.Key())) == -1)
	require.True(t, target.Xor(b.Key()).Compare(target.Xor(c.Key())) == -1)
	require.True(t, target.Xor(c.Key()).Compare(target.Xor(d.Key())) == -1)

	// one known node to start with
	knownNodes := []kad.NodeID[key.Key8]{d, a}

	clk := clock.NewMock()

	iter := NewClosestNodesIter(target)

	cfg := DefaultQueryConfig[key.Key8]()
	cfg.Clock = clk
	cfg.Concurrency = 2

	queryID := QueryID("test")

	self := dtype.NewID(key.Key8(0))
	qry, err := NewQuery[key.Key8](self, queryID, target, iter, knownNodes, cfg)
	require.NoError(t, err)

	// first thing the new query should do is contact the first node
	state := qry.Advance(ctx, nil)
	require.IsType(t, &StateQueryWaitingFindCloser[key.Key8]{}, state)
	st := state.(*StateQueryWaitingFindCloser[key.Key8])
	require.Equal(t, a, st.NodeID)

	// next the query attempts to contact second nearest node
	state = qry.Advance(ctx, nil)
	require.IsType(t, &StateQueryWaitingFindCloser[key.Key8]{}, state)
	st = state.(*StateQueryWaitingFindCloser[key.Key8])
	require.Equal(t, d, st.NodeID)

	// advancing reports query has no capacity
	state = qry.Advance(ctx, nil)
	require.IsType(t, &StateQueryWaitingAtCapacity{}, state)

	// notify query that second node was contacted successfully, with closer nodes
	state = qry.Advance(ctx, &EventQueryMessageResponse[key.Key8]{
		NodeID: d,
		CloserNodes: []kad.NodeID[key.Key8]{
			b,
			a,
		},
	})
	require.IsType(t, &StateQueryWaitingFindCloser[key.Key8]{}, state)

	// query should contact the next nearest uncontacted node, which is b
	st = state.(*StateQueryWaitingFindCloser[key.Key8])
	require.Equal(t, b, st.NodeID)
}

func TestQueryCancelFinishesIteration(t *testing.T) {
	ctx := context.Background()

	target := key.Key8(0b00000001)
	a := dtype.NewID(key.Key8(0b00000100)) // 4

	// one known node to start with
	knownNodes := []kad.NodeID[key.Key8]{a}

	clk := clock.NewMock()

	iter := NewClosestNodesIter(target)

	cfg := DefaultQueryConfig[key.Key8]()
	cfg.Clock = clk
	cfg.Concurrency = 2

	queryID := QueryID("test")

	self := dtype.NewID(key.Key8(0))
	qry, err := NewQuery[key.Key8](self, queryID, target, iter, knownNodes, cfg)
	require.NoError(t, err)

	// first thing the new query should do is contact the first node
	state := qry.Advance(ctx, nil)
	require.IsType(t, &StateQueryWaitingFindCloser[key.Key8]{}, state)
	st := state.(*StateQueryWaitingFindCloser[key.Key8])
	require.Equal(t, a, st.NodeID)

	// cancel the query so it is now finished
	state = qry.Advance(ctx, &EventQueryCancel{})
	require.IsType(t, &StateQueryFinished{}, state)

	stf := state.(*StateQueryFinished)
	require.Equal(t, 0, stf.Stats.Success)
}

func TestQueryFinishedIgnoresLaterEvents(t *testing.T) {
	ctx := context.Background()

	target := key.Key8(0b00000001)
	a := dtype.NewID(key.Key8(0b00000100)) // 4
	b := dtype.NewID(key.Key8(0b00001000)) // 8

	// one known node to start with
	knownNodes := []kad.NodeID[key.Key8]{b}

	clk := clock.NewMock()

	iter := NewClosestNodesIter(target)

	cfg := DefaultQueryConfig[key.Key8]()
	cfg.Clock = clk
	cfg.Concurrency = 2

	queryID := QueryID("test")

	self := dtype.NewID(key.Key8(0))
	qry, err := NewQuery[key.Key8](self, queryID, target, iter, knownNodes, cfg)
	require.NoError(t, err)

	// first thing the new query should do is contact the first node
	state := qry.Advance(ctx, nil)
	require.IsType(t, &StateQueryWaitingFindCloser[key.Key8]{}, state)
	st := state.(*StateQueryWaitingFindCloser[key.Key8])
	require.Equal(t, b, st.NodeID)

	// cancel the query so it is now finished
	state = qry.Advance(ctx, &EventQueryCancel{})
	require.IsType(t, &StateQueryFinished{}, state)

	// no successes
	stf := state.(*StateQueryFinished)
	require.Equal(t, 1, stf.Stats.Requests)
	require.Equal(t, 0, stf.Stats.Success)
	require.Equal(t, 0, stf.Stats.Failure)

	// notify query that second node was contacted successfully, with closer nodes
	state = qry.Advance(ctx, &EventQueryMessageResponse[key.Key8]{
		NodeID:      b,
		CloserNodes: []kad.NodeID[key.Key8]{a},
	})

	// query remains finished
	require.IsType(t, &StateQueryFinished{}, state)

	// still no successes since contact message was after query had been cancelled
	stf = state.(*StateQueryFinished)
	require.Equal(t, 1, stf.Stats.Requests)
	require.Equal(t, 0, stf.Stats.Success)
	require.Equal(t, 0, stf.Stats.Failure)
}

func TestQueryWithCloserIterIgnoresMessagesFromUnknownNodes(t *testing.T) {
	ctx := context.Background()

	target := key.Key8(0b00000001)
	a := dtype.NewID(key.Key8(0b00000100)) // 4
	b := dtype.NewID(key.Key8(0b00001000)) // 8
	c := dtype.NewID(key.Key8(0b00010000)) // 16

	// one known node to start with
	knownNodes := []kad.NodeID[key.Key8]{c}

	clk := clock.NewMock()

	iter := NewClosestNodesIter(target)

	cfg := DefaultQueryConfig[key.Key8]()
	cfg.Clock = clk
	cfg.Concurrency = 2

	queryID := QueryID("test")

	self := dtype.NewID(key.Key8(0))
	qry, err := NewQuery[key.Key8](self, queryID, target, iter, knownNodes, cfg)
	require.NoError(t, err)

	// first thing the new query should do is contact the first node
	state := qry.Advance(ctx, nil)
	require.IsType(t, &StateQueryWaitingFindCloser[key.Key8]{}, state)
	st := state.(*StateQueryWaitingFindCloser[key.Key8])
	require.Equal(t, c, st.NodeID)
	stwm := state.(*StateQueryWaitingFindCloser[key.Key8])
	require.Equal(t, 1, stwm.Stats.Requests)
	require.Equal(t, 0, stwm.Stats.Success)
	require.Equal(t, 0, stwm.Stats.Failure)

	// notify query that second node was contacted successfully, with closer nodes
	state = qry.Advance(ctx, &EventQueryMessageResponse[key.Key8]{
		NodeID:      b,
		CloserNodes: []kad.NodeID[key.Key8]{a},
	})

	// query ignores message from unknown node
	require.IsType(t, &StateQueryWaitingWithCapacity{}, state)

	stwc := state.(*StateQueryWaitingWithCapacity)
	require.Equal(t, 1, stwc.Stats.Requests)
	require.Equal(t, 0, stwc.Stats.Success)
	require.Equal(t, 0, stwc.Stats.Failure)
}

func TestQueryWithCloserIterFinishesWhenNumResultsReached(t *testing.T) {
	ctx := context.Background()

	target := key.Key8(0b00000001)
	a := dtype.NewID(key.Key8(0b00000100)) // 4
	b := dtype.NewID(key.Key8(0b00001000)) // 8
	c := dtype.NewID(key.Key8(0b00010000)) // 16
	d := dtype.NewID(key.Key8(0b00100000)) // 32

	// one known node to start with
	knownNodes := []kad.NodeID[key.Key8]{a, b, c, d}

	clk := clock.NewMock()

	iter := NewClosestNodesIter(target)

	cfg := DefaultQueryConfig[key.Key8]()
	cfg.Clock = clk
	cfg.Concurrency = 4
	cfg.NumResults = 2

	queryID := QueryID("test")

	self := dtype.NewID(key.Key8(0))
	qry, err := NewQuery[key.Key8](self, queryID, target, iter, knownNodes, cfg)
	require.NoError(t, err)

	// contact first node
	state := qry.Advance(ctx, nil)
	require.IsType(t, &StateQueryWaitingFindCloser[key.Key8]{}, state)
	st := state.(*StateQueryWaitingFindCloser[key.Key8])
	require.Equal(t, a, st.NodeID)

	// contact second node
	state = qry.Advance(ctx, nil)
	require.IsType(t, &StateQueryWaitingFindCloser[key.Key8]{}, state)
	st = state.(*StateQueryWaitingFindCloser[key.Key8])
	require.Equal(t, b, st.NodeID)

	// notify query that first node was contacted successfully
	state = qry.Advance(ctx, &EventQueryMessageResponse[key.Key8]{
		NodeID: a,
	})

	// query attempts to contact third node
	require.IsType(t, &StateQueryWaitingFindCloser[key.Key8]{}, state)
	st = state.(*StateQueryWaitingFindCloser[key.Key8])
	require.Equal(t, c, st.NodeID)

	// notify query that second node was contacted successfully
	state = qry.Advance(ctx, &EventQueryMessageResponse[key.Key8]{
		NodeID: b,
	})

	// query has finished since it contacted the NumResults closest nodes
	require.IsType(t, &StateQueryFinished{}, state)
}

func TestQueryWithCloserIterContinuesUntilNumResultsReached(t *testing.T) {
	ctx := context.Background()

	target := key.Key8(0b00000001)
	a := dtype.NewID(key.Key8(0b00000100)) // 4
	b := dtype.NewID(key.Key8(0b00001000)) // 8
	c := dtype.NewID(key.Key8(0b00010000)) // 16

	// one known node to start with, the furthesr
	knownNodes := []kad.NodeID[key.Key8]{c}

	clk := clock.NewMock()

	iter := NewClosestNodesIter(target)

	cfg := DefaultQueryConfig[key.Key8]()
	cfg.Clock = clk
	cfg.Concurrency = 4
	cfg.NumResults = 2

	queryID := QueryID("test")

	self := dtype.NewID(key.Key8(0))
	qry, err := NewQuery[key.Key8](self, queryID, target, iter, knownNodes, cfg)
	require.NoError(t, err)

	// contact first node
	state := qry.Advance(ctx, nil)
	require.IsType(t, &StateQueryWaitingFindCloser[key.Key8]{}, state)
	st := state.(*StateQueryWaitingFindCloser[key.Key8])
	require.Equal(t, c, st.NodeID)

	// notify query that node was contacted successfully and tell it about
	// a closer one
	state = qry.Advance(ctx, &EventQueryMessageResponse[key.Key8]{
		NodeID:      c,
		CloserNodes: []kad.NodeID[key.Key8]{b},
	})

	// query attempts to contact second node
	require.IsType(t, &StateQueryWaitingFindCloser[key.Key8]{}, state)
	st = state.(*StateQueryWaitingFindCloser[key.Key8])
	require.Equal(t, b, st.NodeID)

	// notify query that node was contacted successfully and tell it about
	// a closer one
	state = qry.Advance(ctx, &EventQueryMessageResponse[key.Key8]{
		NodeID:      b,
		CloserNodes: []kad.NodeID[key.Key8]{a},
	})

	// query has seen enough successful contacts but there are still
	// closer nodes that have not been contacted, so query attempts
	// to contact third node
	require.IsType(t, &StateQueryWaitingFindCloser[key.Key8]{}, state)
	st = state.(*StateQueryWaitingFindCloser[key.Key8])
	require.Equal(t, a, st.NodeID)

	// notify query that second node was contacted successfully
	state = qry.Advance(ctx, &EventQueryMessageResponse[key.Key8]{
		NodeID: a,
	})

	// query has finished since it contacted the NumResults closest nodes
	require.IsType(t, &StateQueryFinished{}, state)

	stf := state.(*StateQueryFinished)
	require.Equal(t, 3, stf.Stats.Success)
}

func TestQueryNotContactedMakesCapacity(t *testing.T) {
	ctx := context.Background()

	target := key.Key8(0b00000001)
	a := dtype.NewID(key.Key8(0b00000100)) // 4
	b := dtype.NewID(key.Key8(0b00001000)) // 8
	c := dtype.NewID(key.Key8(0b00010000)) // 16
	d := dtype.NewID(key.Key8(0b00100000)) // 32

	// ensure the order of the known nodes
	require.True(t, target.Xor(a.Key()).Compare(target.Xor(b.Key())) == -1)
	require.True(t, target.Xor(b.Key()).Compare(target.Xor(c.Key())) == -1)
	require.True(t, target.Xor(c.Key()).Compare(target.Xor(d.Key())) == -1)

	knownNodes := []kad.NodeID[key.Key8]{a, b, c, d}
	iter := NewSequentialIter[key.Key8]()

	clk := clock.NewMock()
	cfg := DefaultQueryConfig[key.Key8]()
	cfg.Clock = clk
	cfg.Concurrency = len(knownNodes) - 1 // one less than the number of initial nodes

	queryID := QueryID("test")

	self := dtype.NewID(key.Key8(0))
	qry, err := NewQuery[key.Key8](self, queryID, target, iter, knownNodes, cfg)
	require.NoError(t, err)

	// first thing the new query should do is contact the nearest node
	state := qry.Advance(ctx, nil)
	require.IsType(t, &StateQueryWaitingFindCloser[key.Key8]{}, state)
	st := state.(*StateQueryWaitingFindCloser[key.Key8])
	require.Equal(t, a, st.NodeID)

	// while the query has capacity the query should contact the next nearest node
	state = qry.Advance(ctx, nil)
	require.IsType(t, &StateQueryWaitingFindCloser[key.Key8]{}, state)
	st = state.(*StateQueryWaitingFindCloser[key.Key8])
	require.Equal(t, b, st.NodeID)

	// while the query has capacity the query should contact the second nearest node
	state = qry.Advance(ctx, nil)
	require.IsType(t, &StateQueryWaitingFindCloser[key.Key8]{}, state)
	st = state.(*StateQueryWaitingFindCloser[key.Key8])
	require.Equal(t, c, st.NodeID)

	// the query should be at capacity
	state = qry.Advance(ctx, nil)
	require.IsType(t, &StateQueryWaitingAtCapacity{}, state)

	// notify query that first node was not contacted, now node d can be contacted
	state = qry.Advance(ctx, &EventQueryMessageFailure[key.Key8]{NodeID: a})
	require.IsType(t, &StateQueryWaitingFindCloser[key.Key8]{}, state)
	st = state.(*StateQueryWaitingFindCloser[key.Key8])
	require.Equal(t, d, st.NodeID)

	// the query should be at capacity again
	state = qry.Advance(ctx, nil)
	require.IsType(t, &StateQueryWaitingAtCapacity{}, state)
}

func TestQueryAllNotContactedFinishes(t *testing.T) {
	ctx := context.Background()

	target := key.Key8(0b00000001)
	a := dtype.NewID(key.Key8(0b00000100)) // 4
	b := dtype.NewID(key.Key8(0b00001000)) // 8
	c := dtype.NewID(key.Key8(0b00010000)) // 16

	// knownNodes are in "random" order
	knownNodes := []kad.NodeID[key.Key8]{a, b, c}

	clk := clock.NewMock()

	iter := NewSequentialIter[key.Key8]()

	cfg := DefaultQueryConfig[key.Key8]()
	cfg.Clock = clk
	cfg.Concurrency = len(knownNodes) // allow all to be contacted at once

	queryID := QueryID("test")

	self := dtype.NewID(key.Key8(0))
	qry, err := NewQuery[key.Key8](self, queryID, target, iter, knownNodes, cfg)
	require.NoError(t, err)

	// first thing the new query should do is contact the nearest node
	state := qry.Advance(ctx, nil)
	require.IsType(t, &StateQueryWaitingFindCloser[key.Key8]{}, state)

	// while the query has capacity the query should contact the next nearest node
	state = qry.Advance(ctx, nil)
	require.IsType(t, &StateQueryWaitingFindCloser[key.Key8]{}, state)

	// while the query has capacity the query should contact the third nearest node
	state = qry.Advance(ctx, nil)
	require.IsType(t, &StateQueryWaitingFindCloser[key.Key8]{}, state)

	// the query should be at capacity
	state = qry.Advance(ctx, nil)
	require.IsType(t, &StateQueryWaitingAtCapacity{}, state)

	// notify query that first node was not contacted
	state = qry.Advance(ctx, &EventQueryMessageFailure[key.Key8]{NodeID: a})
	require.IsType(t, &StateQueryWaitingWithCapacity{}, state)

	// notify query that second node was not contacted
	state = qry.Advance(ctx, &EventQueryMessageFailure[key.Key8]{NodeID: b})
	require.IsType(t, &StateQueryWaitingWithCapacity{}, state)

	// notify query that third node was not contacted
	state = qry.Advance(ctx, &EventQueryMessageFailure[key.Key8]{NodeID: c})

	// query has finished since it contacted all possible nodes
	require.IsType(t, &StateQueryFinished{}, state)

	stf := state.(*StateQueryFinished)
	require.Equal(t, 0, stf.Stats.Success)
}

func TestQueryAllContactedFinishes(t *testing.T) {
	ctx := context.Background()

	target := key.Key8(0b00000001)
	a := dtype.NewID(key.Key8(0b00000100)) // 4
	b := dtype.NewID(key.Key8(0b00001000)) // 8
	c := dtype.NewID(key.Key8(0b00010000)) // 16

	knownNodes := []kad.NodeID[key.Key8]{a, b, c}

	clk := clock.NewMock()

	iter := NewSequentialIter[key.Key8]()

	cfg := DefaultQueryConfig[key.Key8]()
	cfg.Clock = clk
	cfg.Concurrency = len(knownNodes)    // allow all to be contacted at once
	cfg.NumResults = len(knownNodes) + 1 // one more than the size of the network

	queryID := QueryID("test")

	self := dtype.NewID(key.Key8(0))
	qry, err := NewQuery[key.Key8](self, queryID, target, iter, knownNodes, cfg)
	require.NoError(t, err)

	// first thing the new query should do is contact the nearest node
	state := qry.Advance(ctx, nil)
	require.IsType(t, &StateQueryWaitingFindCloser[key.Key8]{}, state)

	// while the query has capacity the query should contact the next nearest node
	state = qry.Advance(ctx, nil)
	require.IsType(t, &StateQueryWaitingFindCloser[key.Key8]{}, state)

	// while the query has capacity the query should contact the third nearest node
	state = qry.Advance(ctx, nil)
	require.IsType(t, &StateQueryWaitingFindCloser[key.Key8]{}, state)

	// the query should be at capacity
	state = qry.Advance(ctx, nil)
	require.IsType(t, &StateQueryWaitingAtCapacity{}, state)

	// notify query that first node was contacted successfully, but no closer nodes
	state = qry.Advance(ctx, &EventQueryMessageResponse[key.Key8]{NodeID: a})
	require.IsType(t, &StateQueryWaitingWithCapacity{}, state)

	// notify query that second node was contacted successfully, but no closer nodes
	state = qry.Advance(ctx, &EventQueryMessageResponse[key.Key8]{NodeID: b})
	require.IsType(t, &StateQueryWaitingWithCapacity{}, state)

	// notify query that third node was contacted successfully, but no closer nodes
	state = qry.Advance(ctx, &EventQueryMessageResponse[key.Key8]{NodeID: c})

	// query has finished since it contacted all possible nodes, even though it didn't
	// reach the desired NumResults
	require.IsType(t, &StateQueryFinished{}, state)

	stf := state.(*StateQueryFinished)
	require.Equal(t, 3, stf.Stats.Success)
}

func TestQueryNeverMessagesSelf(t *testing.T) {
	ctx := context.Background()

	target := key.Key8(0b00000001)
	a := dtype.NewID(key.Key8(0b00000100)) // 4
	b := dtype.NewID(key.Key8(0b00001000)) // 8

	// one known node to start with
	knownNodes := []kad.NodeID[key.Key8]{b}

	clk := clock.NewMock()

	iter := NewClosestNodesIter(target)

	cfg := DefaultQueryConfig[key.Key8]()
	cfg.Clock = clk
	cfg.Concurrency = 2

	queryID := QueryID("test")

	self := a
	qry, err := NewQuery[key.Key8](self, queryID, target, iter, knownNodes, cfg)
	require.NoError(t, err)

	// first thing the new query should do is contact the first node
	state := qry.Advance(ctx, nil)
	require.IsType(t, &StateQueryWaitingFindCloser[key.Key8]{}, state)
	st := state.(*StateQueryWaitingFindCloser[key.Key8])
	require.Equal(t, b, st.NodeID)

	// notify query that first node was contacted successfully, with closer nodes
	state = qry.Advance(ctx, &EventQueryMessageResponse[key.Key8]{
		NodeID:      b,
		CloserNodes: []kad.NodeID[key.Key8]{a},
	})

	// query is finished since it can't contact self
	require.IsType(t, &StateQueryFinished{}, state)

	// one successful message
	stf := state.(*StateQueryFinished)
	require.Equal(t, 1, stf.Stats.Requests)
	require.Equal(t, 1, stf.Stats.Success)
	require.Equal(t, 0, stf.Stats.Failure)
}
