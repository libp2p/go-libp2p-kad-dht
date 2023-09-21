package routing

import (
	"context"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/plprobelab/go-kademlia/key"
	"github.com/plprobelab/go-kademlia/routing/simplert"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/libp2p/go-libp2p-kad-dht/v2/internal/coord/internal/tiny"
)

func TestExploreConfigValidate(t *testing.T) {
	t.Run("default is valid", func(t *testing.T) {
		cfg := DefaultExploreConfig()
		require.NoError(t, cfg.Validate())
	})

	t.Run("clock is not nil", func(t *testing.T) {
		cfg := DefaultExploreConfig()
		cfg.Clock = nil
		require.Error(t, cfg.Validate())
	})

	t.Run("timeout positive", func(t *testing.T) {
		cfg := DefaultExploreConfig()
		cfg.Timeout = 0
		require.Error(t, cfg.Validate())
		cfg.Timeout = -1
		require.Error(t, cfg.Validate())
	})
}

func DefaultDynamicSchedule(t *testing.T, clk clock.Clock) *DynamicExploreSchedule {
	t.Helper()
	// maxCpl is 7 since we are using tiny 8-bit keys
	s, err := NewDynamicExploreSchedule(7, clk.Now(), time.Hour, 1, 0)
	require.NoError(t, err)
	return s
}

func TestDynamicExploreSchedule(t *testing.T) {
	testCases := []struct {
		interval   time.Duration
		multiplier float64
	}{
		{
			interval:   time.Hour,
			multiplier: 1,
		},
		{
			interval:   time.Hour,
			multiplier: 1.1,
		},
		{
			interval:   time.Hour,
			multiplier: 2,
		},
	}

	// test invariants
	for _, tc := range testCases {
		clk := clock.NewMock()
		maxCpl := 20

		s, err := NewDynamicExploreSchedule(maxCpl, clk.Now(), tc.interval, tc.multiplier, 0)
		require.NoError(t, err)

		intervals := make([]time.Duration, 0, maxCpl+1)
		cpl := maxCpl
		for cpl >= 0 {
			intervals = append(intervals, s.cplInterval(cpl))
			cpl--
		}

		// higher cpls must have a shorter interval than lower cpls
		assert.IsIncreasing(t, intervals)

		// intervals increase by at least one base interval for each cpl
		// interval for cpl[x-1] is at twice the interval of than cpl[x]
		// and cpl[x-2] is at least three times larger than cpl[x]
		for i := 1; i < len(intervals); i++ {
			assert.GreaterOrEqual(t, intervals[i], intervals[0]*time.Duration(i+1))
		}

	}
}

func TestExploreStartsIdle(t *testing.T) {
	ctx := context.Background()
	clk := clock.NewMock()
	cfg := DefaultExploreConfig()
	cfg.Clock = clk

	self := tiny.NewNode(128)
	rt := simplert.New[tiny.Key, tiny.Node](self, 5)
	schedule := DefaultDynamicSchedule(t, clk)
	ex, err := NewExplore[tiny.Key, tiny.Node](self, rt, tiny.NodeWithCpl, schedule, cfg)
	require.NoError(t, err)

	state := ex.Advance(ctx, &EventExplorePoll{})
	require.IsType(t, &StateExploreIdle{}, state)
}

func TestExploreFirstQueriesForMaximumCpl(t *testing.T) {
	ctx := context.Background()
	clk := clock.NewMock()
	cfg := DefaultExploreConfig()
	cfg.Clock = clk

	self := tiny.NewNode(128)
	rt := simplert.New[tiny.Key, tiny.Node](self, 5)

	// populate the routing table with at least one node
	a := tiny.NewNode(4)
	rt.AddNode(a)

	schedule := DefaultDynamicSchedule(t, clk)
	ex, err := NewExplore[tiny.Key, tiny.Node](self, rt, tiny.NodeWithCpl, schedule, cfg)
	require.NoError(t, err)

	state := ex.Advance(ctx, &EventExplorePoll{})
	require.IsType(t, &StateExploreIdle{}, state)

	// advance the clock to the due time of the first explore that should be started
	clk.Add(schedule.cplInterval(schedule.maxCpl))

	// explore should now start the explore query
	state = ex.Advance(ctx, &EventExplorePoll{})
	require.IsType(t, &StateExploreFindCloser[tiny.Key, tiny.Node]{}, state)

	// the query should attempt to contact the node it was given
	st := state.(*StateExploreFindCloser[tiny.Key, tiny.Node])

	// the query should have the correct ID
	require.Equal(t, ExploreQueryID, st.QueryID)

	// with the correct cpl
	require.Equal(t, schedule.maxCpl, st.Cpl)

	// the query should attempt to look for nodes near a key with the maximum cpl
	require.True(t, key.Equal(self.Key(), st.Target))

	// the query should be contacting the nearest known node
	require.Equal(t, a, st.NodeID)

	// now the explore reports that it is waiting
	state = ex.Advance(ctx, &EventExplorePoll{})
	require.IsType(t, &StateExploreWaiting{}, state)
}

func TestExploreFindCloserResponse(t *testing.T) {
	ctx := context.Background()
	clk := clock.NewMock()

	cfg := DefaultExploreConfig()
	cfg.Clock = clk

	self := tiny.NewNode(128)
	rt := simplert.New[tiny.Key, tiny.Node](self, 5)

	// populate the routing table with at least one node
	a := tiny.NewNode(4)
	rt.AddNode(a)

	start := clk.Now()

	schedule := DefaultDynamicSchedule(t, clk)
	ex, err := NewExplore[tiny.Key, tiny.Node](self, rt, tiny.NodeWithCpl, schedule, cfg)
	require.NoError(t, err)

	state := ex.Advance(ctx, &EventExplorePoll{})
	require.IsType(t, &StateExploreIdle{}, state)

	// advance the clock to the due time of the first explore that should be started
	interval1 := schedule.cplInterval(schedule.maxCpl)
	clk.Set(start.Add(interval1))

	// explore should now start the explore query
	state = ex.Advance(ctx, &EventExplorePoll{})
	require.IsType(t, &StateExploreFindCloser[tiny.Key, tiny.Node]{}, state)

	// now the explore reports that it is waiting
	state = ex.Advance(ctx, &EventExplorePoll{})
	require.IsType(t, &StateExploreWaiting{}, state)

	// notify explore that node was contacted successfully, but no closer nodes
	state = ex.Advance(ctx, &EventExploreFindCloserResponse[tiny.Key, tiny.Node]{
		NodeID: a,
	})
	require.IsType(t, &StateExploreQueryFinished{}, state)
}

func TestExploreFindCloserFailure(t *testing.T) {
	ctx := context.Background()
	clk := clock.NewMock()

	cfg := DefaultExploreConfig()
	cfg.Clock = clk

	self := tiny.NewNode(128)
	rt := simplert.New[tiny.Key, tiny.Node](self, 5)

	// populate the routing table with at least one node
	a := tiny.NewNode(4)
	rt.AddNode(a)

	start := clk.Now()

	schedule := DefaultDynamicSchedule(t, clk)
	ex, err := NewExplore[tiny.Key, tiny.Node](self, rt, tiny.NodeWithCpl, schedule, cfg)
	require.NoError(t, err)

	state := ex.Advance(ctx, &EventExplorePoll{})
	require.IsType(t, &StateExploreIdle{}, state)

	// advance the clock to the due time of the first explore that should be started
	interval1 := schedule.cplInterval(schedule.maxCpl)
	clk.Set(start.Add(interval1))

	// explore should now start the explore query
	state = ex.Advance(ctx, &EventExplorePoll{})
	require.IsType(t, &StateExploreFindCloser[tiny.Key, tiny.Node]{}, state)

	// now the explore reports that it is waiting
	state = ex.Advance(ctx, &EventExplorePoll{})
	require.IsType(t, &StateExploreWaiting{}, state)

	// notify explore that node was not
	state = ex.Advance(ctx, &EventExploreFindCloserFailure[tiny.Key, tiny.Node]{
		NodeID: a,
	})
	require.IsType(t, &StateExploreQueryFinished{}, state)
}

func TestExploreProgress(t *testing.T) {
	ctx := context.Background()
	clk := clock.NewMock()

	cfg := DefaultExploreConfig()
	cfg.Clock = clk

	self := tiny.NewNode(128)
	rt := simplert.New[tiny.Key, tiny.Node](self, 5)

	a := tiny.NewNode(4)  // 4
	b := tiny.NewNode(8)  // 8
	c := tiny.NewNode(16) // 16

	// ensure the order of the known nodes
	require.True(t, self.Key().Xor(a.Key()).Compare(self.Key().Xor(b.Key())) == -1)
	require.True(t, self.Key().Xor(b.Key()).Compare(self.Key().Xor(c.Key())) == -1)

	// populate the routing table with at least one node
	rt.AddNode(a)

	start := clk.Now()

	schedule := DefaultDynamicSchedule(t, clk)
	ex, err := NewExplore[tiny.Key, tiny.Node](self, rt, tiny.NodeWithCpl, schedule, cfg)
	require.NoError(t, err)

	state := ex.Advance(ctx, &EventExplorePoll{})
	require.IsType(t, &StateExploreIdle{}, state)

	// advance the clock to the due time of the first explore that should be started
	interval1 := schedule.cplInterval(schedule.maxCpl)
	clk.Set(start.Add(interval1))

	// explore should now start the explore query
	state = ex.Advance(ctx, &EventExplorePoll{})
	require.IsType(t, &StateExploreFindCloser[tiny.Key, tiny.Node]{}, state)

	// the query should attempt to contact the node it was given
	st := state.(*StateExploreFindCloser[tiny.Key, tiny.Node])
	require.Equal(t, a, st.NodeID)

	// now the explore reports that it is waiting
	state = ex.Advance(ctx, &EventExplorePoll{})
	require.IsType(t, &StateExploreWaiting{}, state)

	// notify explore that node was contacted successfully, with a closer node
	state = ex.Advance(ctx, &EventExploreFindCloserResponse[tiny.Key, tiny.Node]{
		NodeID:      a,
		CloserNodes: []tiny.Node{b},
	})

	// explore tries to contact nearer node
	require.IsType(t, &StateExploreFindCloser[tiny.Key, tiny.Node]{}, state)

	// notify explore that node was contacted successfully, with a closer node
	state = ex.Advance(ctx, &EventExploreFindCloserResponse[tiny.Key, tiny.Node]{
		NodeID:      b,
		CloserNodes: []tiny.Node{c},
	})

	// explore tries to contact nearer node
	require.IsType(t, &StateExploreFindCloser[tiny.Key, tiny.Node]{}, state)

	// the query should attempt to contact the node it was given
	st = state.(*StateExploreFindCloser[tiny.Key, tiny.Node])
	require.Equal(t, c, st.NodeID)

	// notify explore that node was contacted successfully, but no closer nodes
	state = ex.Advance(ctx, &EventExploreFindCloserResponse[tiny.Key, tiny.Node]{
		NodeID: c,
	})
	require.IsType(t, &StateExploreQueryFinished{}, state)
}

func TestExploreQueriesNextHighestCpl(t *testing.T) {
	ctx := context.Background()
	clk := clock.NewMock()

	cfg := DefaultExploreConfig()
	cfg.Clock = clk

	self := tiny.NewNode(128)
	rt := simplert.New[tiny.Key, tiny.Node](self, 5)

	// populate the routing table with at least one node
	a := tiny.NewNode(4)
	rt.AddNode(a)

	start := clk.Now()

	schedule := DefaultDynamicSchedule(t, clk)
	ex, err := NewExplore[tiny.Key, tiny.Node](self, rt, tiny.NodeWithCpl, schedule, cfg)
	require.NoError(t, err)

	state := ex.Advance(ctx, &EventExplorePoll{})
	require.IsType(t, &StateExploreIdle{}, state)

	// advance the clock to the due time of the first explore that should be started
	interval1 := schedule.cplInterval(schedule.maxCpl)
	clk.Set(start.Add(interval1))

	// explore should now start the explore query
	state = ex.Advance(ctx, &EventExplorePoll{})
	require.IsType(t, &StateExploreFindCloser[tiny.Key, tiny.Node]{}, state)
	st := state.(*StateExploreFindCloser[tiny.Key, tiny.Node])

	// the query should have the correct ID
	require.Equal(t, ExploreQueryID, st.QueryID)

	// with the correct cpl
	require.Equal(t, schedule.maxCpl, st.Cpl)

	// the query should attempt to look for nodes near a key with the maximum cpl
	require.True(t, key.Equal(self.Key(), st.Target))

	// the query should be contacting the nearest known node
	require.Equal(t, a, st.NodeID)

	// now the explore reports that it is waiting
	state = ex.Advance(ctx, &EventExplorePoll{})
	require.IsType(t, &StateExploreWaiting{}, state)

	// notify explore that node was contacted successfully, but no closer nodes
	state = ex.Advance(ctx, &EventExploreFindCloserResponse[tiny.Key, tiny.Node]{
		NodeID: a,
	})
	require.IsType(t, &StateExploreQueryFinished{}, state)

	// advance the clock to the due time of the second cpl explore that should be started
	interval2 := schedule.cplInterval(schedule.maxCpl - 1)
	clk.Set(start.Add(interval2))

	// explore should now start another explore query
	state = ex.Advance(ctx, &EventExplorePoll{})
	require.IsType(t, &StateExploreFindCloser[tiny.Key, tiny.Node]{}, state)
	st = state.(*StateExploreFindCloser[tiny.Key, tiny.Node])

	// with the correct cpl
	require.Equal(t, schedule.maxCpl-1, st.Cpl)

	// the query should attempt to look for nodes near a key with the maximum cpl
	require.True(t, key.Equal(self.Key(), st.Target))

	// the query should be contacting the nearest known node
	require.Equal(t, a, st.NodeID)
}
