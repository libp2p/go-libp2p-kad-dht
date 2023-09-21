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

	t.Run("interval positive", func(t *testing.T) {
		cfg := DefaultExploreConfig()
		cfg.Interval = 0
		require.Error(t, cfg.Validate())
		cfg.Interval = -1
		require.Error(t, cfg.Validate())
	})

	t.Run("interval multiplier greater or equal one", func(t *testing.T) {
		cfg := DefaultExploreConfig()
		cfg.IntervalMultiplier = 0
		require.Error(t, cfg.Validate())
		cfg.IntervalMultiplier = -1
		require.Error(t, cfg.Validate())
		cfg.IntervalMultiplier = 0.99
		require.Error(t, cfg.Validate())
	})

	t.Run("interval jitter between 0 and 0.05", func(t *testing.T) {
		cfg := DefaultExploreConfig()
		cfg.IntervalJitter = 0
		require.NoError(t, cfg.Validate())
		cfg.IntervalJitter = -1
		require.Error(t, cfg.Validate())
		cfg.IntervalJitter = 0.06
		require.Error(t, cfg.Validate())
	})
}

func TestExploreStartsIdle(t *testing.T) {
	ctx := context.Background()
	clk := clock.NewMock()
	cfg := DefaultExploreConfig()
	cfg.Clock = clk
	cfg.MaximumCpl = 7 // since we are using tiny 8-bit keys

	self := tiny.NewNode(128)
	rt := simplert.New[tiny.Key, tiny.Node](self, 5)
	ex, err := NewExplore[tiny.Key, tiny.Node](self, rt, tiny.NodeWithCpl, cfg)
	require.NoError(t, err)

	state := ex.Advance(ctx, &EventExplorePoll{})
	require.IsType(t, &StateExploreIdle{}, state)
}

func TestExploreIntervalCalc(t *testing.T) {
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
		cfg := DefaultExploreConfig()
		cfg.Interval = tc.interval
		cfg.IntervalMultiplier = tc.multiplier
		cfg.MaximumCpl = 20

		self := tiny.NewNode(128)
		rt := simplert.New[tiny.Key, tiny.Node](self, 5)
		ex, err := NewExplore[tiny.Key, tiny.Node](self, rt, tiny.NodeWithCpl, cfg)
		require.NoError(t, err)

		intervals := make([]time.Duration, 0, cfg.MaximumCpl+1)
		cpl := cfg.MaximumCpl
		for cpl >= 0 {
			intervals = append(intervals, ex.interval(cpl))
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

func TestExploreFirstQueriesForMaximumCpl(t *testing.T) {
	ctx := context.Background()
	clk := clock.NewMock()
	cfg := DefaultExploreConfig()
	cfg.Clock = clk
	cfg.MaximumCpl = 7 // since we are using tiny 8-bit keys

	self := tiny.NewNode(128)
	rt := simplert.New[tiny.Key, tiny.Node](self, 5)

	// populate the routing table with at least one node
	a := tiny.NewNode(4)
	rt.AddNode(a)

	ex, err := NewExplore[tiny.Key, tiny.Node](self, rt, tiny.NodeWithCpl, cfg)
	require.NoError(t, err)

	state := ex.Advance(ctx, &EventExplorePoll{})
	require.IsType(t, &StateExploreIdle{}, state)

	// advance the clock to just past the due time of the first explore that should be started
	interval := ex.interval(cfg.MaximumCpl) + time.Millisecond
	clk.Add(interval)

	// explore should now start the explore query
	state = ex.Advance(ctx, &EventExplorePoll{})
	require.IsType(t, &StateExploreFindCloser[tiny.Key, tiny.Node]{}, state)

	// the query should attempt to contact the node it was given
	st := state.(*StateExploreFindCloser[tiny.Key, tiny.Node])

	// the query should have the correct ID
	require.Equal(t, ExploreQueryID, st.QueryID)

	// with the correct cpl
	require.Equal(t, cfg.MaximumCpl, st.Cpl)

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
	cfg.MaximumCpl = 7 // since we are using tiny 8-bit keys

	self := tiny.NewNode(128)
	rt := simplert.New[tiny.Key, tiny.Node](self, 5)

	// populate the routing table with at least one node
	a := tiny.NewNode(4)
	rt.AddNode(a)

	start := clk.Now()

	ex, err := NewExplore[tiny.Key, tiny.Node](self, rt, tiny.NodeWithCpl, cfg)
	require.NoError(t, err)

	state := ex.Advance(ctx, &EventExplorePoll{})
	require.IsType(t, &StateExploreIdle{}, state)

	// advance the clock to just past the due time of the first explore that should be started
	interval1 := ex.interval(cfg.MaximumCpl)
	clk.Set(start.Add(interval1 + time.Millisecond))

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
	cfg.MaximumCpl = 7 // since we are using tiny 8-bit keys

	self := tiny.NewNode(128)
	rt := simplert.New[tiny.Key, tiny.Node](self, 5)

	// populate the routing table with at least one node
	a := tiny.NewNode(4)
	rt.AddNode(a)

	start := clk.Now()

	ex, err := NewExplore[tiny.Key, tiny.Node](self, rt, tiny.NodeWithCpl, cfg)
	require.NoError(t, err)

	state := ex.Advance(ctx, &EventExplorePoll{})
	require.IsType(t, &StateExploreIdle{}, state)

	// advance the clock to just past the due time of the first explore that should be started
	interval1 := ex.interval(cfg.MaximumCpl)
	clk.Set(start.Add(interval1 + time.Millisecond))

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
	cfg.MaximumCpl = 7 // since we are using tiny 8-bit keys

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

	ex, err := NewExplore[tiny.Key, tiny.Node](self, rt, tiny.NodeWithCpl, cfg)
	require.NoError(t, err)

	state := ex.Advance(ctx, &EventExplorePoll{})
	require.IsType(t, &StateExploreIdle{}, state)

	// advance the clock to just past the due time of the first explore that should be started
	interval1 := ex.interval(cfg.MaximumCpl)
	clk.Set(start.Add(interval1 + time.Millisecond))

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
	cfg.MaximumCpl = 7 // since we are using tiny 8-bit keys

	self := tiny.NewNode(128)
	rt := simplert.New[tiny.Key, tiny.Node](self, 5)

	// populate the routing table with at least one node
	a := tiny.NewNode(4)
	rt.AddNode(a)

	start := clk.Now()

	ex, err := NewExplore[tiny.Key, tiny.Node](self, rt, tiny.NodeWithCpl, cfg)
	require.NoError(t, err)

	state := ex.Advance(ctx, &EventExplorePoll{})
	require.IsType(t, &StateExploreIdle{}, state)

	// advance the clock to just past the due time of the first explore that should be started
	interval1 := ex.interval(cfg.MaximumCpl)
	clk.Set(start.Add(interval1 + time.Millisecond))

	// explore should now start the explore query
	state = ex.Advance(ctx, &EventExplorePoll{})
	require.IsType(t, &StateExploreFindCloser[tiny.Key, tiny.Node]{}, state)
	st := state.(*StateExploreFindCloser[tiny.Key, tiny.Node])

	// the query should have the correct ID
	require.Equal(t, ExploreQueryID, st.QueryID)

	// with the correct cpl
	require.Equal(t, cfg.MaximumCpl, st.Cpl)

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

	// advance the clock to just past the due time of the second cpl explore that should be started
	interval2 := ex.interval(cfg.MaximumCpl - 1)
	clk.Set(start.Add(interval2 + time.Millisecond))

	// explore should now start another explore query
	state = ex.Advance(ctx, &EventExplorePoll{})
	require.IsType(t, &StateExploreFindCloser[tiny.Key, tiny.Node]{}, state)
	st = state.(*StateExploreFindCloser[tiny.Key, tiny.Node])

	// with the correct cpl
	require.Equal(t, cfg.MaximumCpl-1, st.Cpl)

	// the query should attempt to look for nodes near a key with the maximum cpl
	require.True(t, key.Equal(self.Key(), st.Target))

	// the query should be contacting the nearest known node
	require.Equal(t, a, st.NodeID)
}

func TestExploreQueriesFrequencyDistribution(t *testing.T) {
	ctx := context.Background()
	clk := clock.NewMock()

	cfg := DefaultExploreConfig()
	cfg.Clock = clk
	cfg.MaximumCpl = 7 // since we are using tiny 8-bit keys

	// make sure schedule is fully deterministic
	cfg.Interval = time.Hour
	cfg.IntervalMultiplier = 1
	cfg.IntervalJitter = 0

	self := tiny.NewNode(128)
	rt := simplert.New[tiny.Key, tiny.Node](self, 5)

	// populate the routing table with at least one node
	a := tiny.NewNode(4)
	rt.AddNode(a)

	ex, err := NewExplore[tiny.Key, tiny.Node](self, rt, tiny.NodeWithCpl, cfg)
	require.NoError(t, err)

	cplDistribution := make(map[int]float64) // float64 for fractional comparison later

	// advance the clock a little so advances don't fall on exact interval boundaries
	clk.Add(time.Minute)

	for i := 0; i < 24*4; i++ {
		// advance the clock in 15 minute steps
		clk.Add(15 * time.Minute)

		// explore should now start an explore query
		state := ex.Advance(ctx, &EventExplorePoll{})
		switch st := state.(type) {
		case *StateExploreFindCloser[tiny.Key, tiny.Node]:
			cplDistribution[st.Cpl]++
			// notify explore that node was contacted successfully, but no closer nodes
			state = ex.Advance(ctx, &EventExploreFindCloserResponse[tiny.Key, tiny.Node]{
				NodeID: a,
			})
			require.IsType(t, &StateExploreQueryFinished{}, state)
		}

	}

	// ensure every cpl was explored at least once in the time period
	for cpl := cfg.MaximumCpl; cpl >= 0; cpl-- {
		assert.GreaterOrEqual(t, cplDistribution[cpl], 1.0)
	}

	// cpl 6 should be explored roughly half as many times as cpl 7
	assert.InDelta(t, cplDistribution[7]/2, cplDistribution[6], 1.5)

	// cpl 5 should be explored roughly a third as many times as cpl 7
	assert.InDelta(t, cplDistribution[7]/3, cplDistribution[5], 1.5)

	// cpl 4 should be explored roughly a fourth as many times as cpl 7
	assert.InDelta(t, cplDistribution[7]/4, cplDistribution[4], 1.5)

	// cpl 3 should be explored roughly a fifth as many times as cpl 7
	assert.InDelta(t, cplDistribution[7]/5, cplDistribution[4], 1.5)

	// accuracy for smaller cpls is too low given the number of samples
	// simulating for a longer time period would improve accuracy, but the mock clock is very slow at increasing time
}
