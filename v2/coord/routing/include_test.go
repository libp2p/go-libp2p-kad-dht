package routing

import (
	"context"
	"testing"

	"github.com/benbjohnson/clock"
	"github.com/plprobelab/go-kademlia/kad"
	"github.com/plprobelab/go-kademlia/key"
	"github.com/plprobelab/go-kademlia/routing/simplert"
	"github.com/stretchr/testify/require"

	"github.com/libp2p/go-libp2p-kad-dht/v2/coord/internal/dtype"
)

func TestIncludeConfigValidate(t *testing.T) {
	t.Run("default is valid", func(t *testing.T) {
		cfg := DefaultIncludeConfig()
		require.NoError(t, cfg.Validate())
	})

	t.Run("clock is not nil", func(t *testing.T) {
		cfg := DefaultIncludeConfig()
		cfg.Clock = nil
		require.Error(t, cfg.Validate())
	})

	t.Run("timeout positive", func(t *testing.T) {
		cfg := DefaultIncludeConfig()
		cfg.Timeout = 0
		require.Error(t, cfg.Validate())
		cfg.Timeout = -1
		require.Error(t, cfg.Validate())
	})

	t.Run("request concurrency positive", func(t *testing.T) {
		cfg := DefaultIncludeConfig()
		cfg.Concurrency = 0
		require.Error(t, cfg.Validate())
		cfg.Concurrency = -1
		require.Error(t, cfg.Validate())
	})

	t.Run("queue size positive", func(t *testing.T) {
		cfg := DefaultIncludeConfig()
		cfg.QueueCapacity = 0
		require.Error(t, cfg.Validate())
		cfg.QueueCapacity = -1
		require.Error(t, cfg.Validate())
	})
}

func TestIncludeStartsIdle(t *testing.T) {
	ctx := context.Background()
	clk := clock.NewMock()
	cfg := DefaultIncludeConfig()
	cfg.Clock = clk

	rt := simplert.New[key.Key8, kad.NodeID[key.Key8]](dtype.NewID(key.Key8(128)), 5)

	bs, err := NewInclude[key.Key8](rt, cfg)
	require.NoError(t, err)

	state := bs.Advance(ctx, &EventIncludePoll{})
	require.IsType(t, &StateIncludeIdle{}, state)
}

func TestIncludeAddCandidateStartsCheckIfCapacity(t *testing.T) {
	ctx := context.Background()
	clk := clock.NewMock()
	cfg := DefaultIncludeConfig()
	cfg.Clock = clk
	cfg.Concurrency = 1

	rt := simplert.New[key.Key8, kad.NodeID[key.Key8]](dtype.NewID(key.Key8(128)), 5)

	p, err := NewInclude[key.Key8](rt, cfg)
	require.NoError(t, err)

	candidate := dtype.NewID(key.Key8(0b00000100))

	// add a candidate
	state := p.Advance(ctx, &EventIncludeAddCandidate[key.Key8]{
		NodeID: candidate,
	})
	// the state machine should attempt to send a message
	require.IsType(t, &StateIncludeFindNodeMessage[key.Key8]{}, state)

	st := state.(*StateIncludeFindNodeMessage[key.Key8])

	// the message should be sent to the candidate node
	require.Equal(t, candidate, st.NodeID)

	// the message should be looking for the candidate node
	require.Equal(t, candidate, st.NodeID)

	// now the include reports that it is waiting since concurrency is 1
	state = p.Advance(ctx, &EventIncludePoll{})
	require.IsType(t, &StateIncludeWaitingAtCapacity{}, state)
}

func TestIncludeAddCandidateReportsCapacity(t *testing.T) {
	ctx := context.Background()
	clk := clock.NewMock()
	cfg := DefaultIncludeConfig()
	cfg.Clock = clk
	cfg.Concurrency = 2

	rt := simplert.New[key.Key8, kad.NodeID[key.Key8]](dtype.NewID(key.Key8(128)), 5)
	p, err := NewInclude[key.Key8](rt, cfg)
	require.NoError(t, err)

	candidate := dtype.NewID(key.Key8(0b00000100))

	// add a candidate
	state := p.Advance(ctx, &EventIncludeAddCandidate[key.Key8]{
		NodeID: candidate,
	})
	require.IsType(t, &StateIncludeFindNodeMessage[key.Key8]{}, state)

	// now the state machine reports that it is waiting with capacity since concurrency
	// is greater than the number of checks in flight
	state = p.Advance(ctx, &EventIncludePoll{})
	require.IsType(t, &StateIncludeWaitingWithCapacity{}, state)
}

func TestIncludeAddCandidateOverQueueLength(t *testing.T) {
	ctx := context.Background()
	clk := clock.NewMock()
	cfg := DefaultIncludeConfig()
	cfg.Clock = clk
	cfg.QueueCapacity = 2 // only allow two candidates in the queue
	cfg.Concurrency = 3

	rt := simplert.New[key.Key8, kad.NodeID[key.Key8]](dtype.NewID(key.Key8(128)), 5)

	p, err := NewInclude[key.Key8](rt, cfg)
	require.NoError(t, err)

	// add a candidate
	state := p.Advance(ctx, &EventIncludeAddCandidate[key.Key8]{
		NodeID: dtype.NewID(key.Key8(0b00000100)),
	})
	require.IsType(t, &StateIncludeFindNodeMessage[key.Key8]{}, state)

	// include reports that it is waiting and has capacity for more
	state = p.Advance(ctx, &EventIncludePoll{})
	require.IsType(t, &StateIncludeWaitingWithCapacity{}, state)

	// add second candidate
	state = p.Advance(ctx, &EventIncludeAddCandidate[key.Key8]{
		NodeID: dtype.NewID(key.Key8(0b00000010)),
	})
	// sends a message to the candidate
	require.IsType(t, &StateIncludeFindNodeMessage[key.Key8]{}, state)

	// include reports that it is waiting and has capacity for more
	state = p.Advance(ctx, &EventIncludePoll{})
	// sends a message to the candidate
	require.IsType(t, &StateIncludeWaitingWithCapacity{}, state)

	// add third candidate
	state = p.Advance(ctx, &EventIncludeAddCandidate[key.Key8]{
		NodeID: dtype.NewID(key.Key8(0b00000011)),
	})
	// sends a message to the candidate
	require.IsType(t, &StateIncludeFindNodeMessage[key.Key8]{}, state)

	// include reports that it is waiting at capacity since 3 messages are in flight
	state = p.Advance(ctx, &EventIncludePoll{})
	require.IsType(t, &StateIncludeWaitingAtCapacity{}, state)

	// add fourth candidate
	state = p.Advance(ctx, &EventIncludeAddCandidate[key.Key8]{
		NodeID: dtype.NewID(key.Key8(0b00000101)),
	})

	// include reports that it is waiting at capacity since 3 messages are already in flight
	require.IsType(t, &StateIncludeWaitingAtCapacity{}, state)

	// add fifth candidate
	state = p.Advance(ctx, &EventIncludeAddCandidate[key.Key8]{
		NodeID: dtype.NewID(key.Key8(0b00000110)),
	})

	// include reports that it is waiting and the candidate queue is full since it
	// is configured to have 3 concurrent checks and 2 queued
	require.IsType(t, &StateIncludeWaitingFull{}, state)

	// add sixth candidate
	state = p.Advance(ctx, &EventIncludeAddCandidate[key.Key8]{
		NodeID: dtype.NewID(key.Key8(0b00000111)),
	})

	// include reports that it is still waiting and the candidate queue is full since it
	// is configured to have 3 concurrent checks and 2 queued
	require.IsType(t, &StateIncludeWaitingFull{}, state)
}

func TestIncludeMessageResponse(t *testing.T) {
	ctx := context.Background()
	clk := clock.NewMock()
	cfg := DefaultIncludeConfig()
	cfg.Clock = clk
	cfg.Concurrency = 2

	rt := simplert.New[key.Key8, kad.NodeID[key.Key8]](dtype.NewID(key.Key8(128)), 5)

	p, err := NewInclude[key.Key8](rt, cfg)
	require.NoError(t, err)

	// add a candidate
	state := p.Advance(ctx, &EventIncludeAddCandidate[key.Key8]{
		NodeID: dtype.NewID(key.Key8(0b00000100)),
	})
	require.IsType(t, &StateIncludeFindNodeMessage[key.Key8]{}, state)

	// notify that node was contacted successfully, with no closer nodes
	state = p.Advance(ctx, &EventIncludeMessageResponse[key.Key8]{
		NodeID: dtype.NewID(key.Key8(0b00000100)),
		CloserNodes: []kad.NodeID[key.Key8]{
			dtype.NewID(key.Key8(4)),
			dtype.NewID(key.Key8(6)),
		},
	})

	// should respond that the routing table was updated
	require.IsType(t, &StateIncludeRoutingUpdated[key.Key8]{}, state)

	st := state.(*StateIncludeRoutingUpdated[key.Key8])

	// the update is for the correct node
	require.Equal(t, dtype.NewID(key.Key8(4)), st.NodeID)

	// the routing table should contain the node
	foundNode, found := rt.GetNode(key.Key8(4))
	require.True(t, found)
	require.NotNil(t, foundNode)

	require.True(t, key.Equal(foundNode.Key(), key.Key8(4)))

	// advancing again should reports that it is idle
	state = p.Advance(ctx, &EventIncludePoll{})
	require.IsType(t, &StateIncludeIdle{}, state)
}

func TestIncludeMessageResponseInvalid(t *testing.T) {
	ctx := context.Background()
	clk := clock.NewMock()
	cfg := DefaultIncludeConfig()
	cfg.Clock = clk
	cfg.Concurrency = 2

	rt := simplert.New[key.Key8, kad.NodeID[key.Key8]](dtype.NewID(key.Key8(128)), 5)

	p, err := NewInclude[key.Key8](rt, cfg)
	require.NoError(t, err)

	// add a candidate
	state := p.Advance(ctx, &EventIncludeAddCandidate[key.Key8]{
		NodeID: dtype.NewID(key.Key8(0b00000100)),
	})
	require.IsType(t, &StateIncludeFindNodeMessage[key.Key8]{}, state)

	// notify that node was contacted successfully, but no closer nodes
	state = p.Advance(ctx, &EventIncludeMessageResponse[key.Key8]{
		NodeID: dtype.NewID(key.Key8(0b00000100)),
	})
	// should respond that state machine is idle
	require.IsType(t, &StateIncludeIdle{}, state)

	// the routing table should not contain the node
	foundNode, found := rt.GetNode(key.Key8(4))
	require.False(t, found)
	require.Nil(t, foundNode)
}

func TestIncludeMessageFailure(t *testing.T) {
	ctx := context.Background()
	clk := clock.NewMock()
	cfg := DefaultIncludeConfig()
	cfg.Clock = clk
	cfg.Concurrency = 2

	rt := simplert.New[key.Key8, kad.NodeID[key.Key8]](dtype.NewID(key.Key8(128)), 5)

	p, err := NewInclude[key.Key8](rt, cfg)
	require.NoError(t, err)

	// add a candidate
	state := p.Advance(ctx, &EventIncludeAddCandidate[key.Key8]{
		NodeID: dtype.NewID(key.Key8(0b00000100)),
	})
	require.IsType(t, &StateIncludeFindNodeMessage[key.Key8]{}, state)

	// notify that node was not contacted successfully
	state = p.Advance(ctx, &EventIncludeMessageFailure[key.Key8]{
		NodeID: dtype.NewID(key.Key8(0b00000100)),
	})

	// should respond that state machine is idle
	require.IsType(t, &StateIncludeIdle{}, state)

	// the routing table should not contain the node
	foundNode, found := rt.GetNode(key.Key8(4))
	require.False(t, found)
	require.Nil(t, foundNode)
}
