package routing

import (
	"context"
	"testing"

	"github.com/benbjohnson/clock"
	"github.com/libp2p/go-libp2p-kad-dht/v2/coord/internal/tiny"
	"github.com/plprobelab/go-kademlia/key"
	"github.com/plprobelab/go-kademlia/routing/simplert"
	"github.com/stretchr/testify/require"
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

	rt := simplert.New[tiny.Key, tiny.Node](tiny.NewNode(tiny.Key(128)), 5)

	bs, err := NewInclude[tiny.Key, tiny.Node](rt, cfg)
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

	rt := simplert.New[tiny.Key, tiny.Node](tiny.NewNode(tiny.Key(128)), 5)

	p, err := NewInclude[tiny.Key, tiny.Node](rt, cfg)
	require.NoError(t, err)

	candidate := tiny.NewNode(tiny.Key(0b00000100))

	// add a candidate
	state := p.Advance(ctx, &EventIncludeAddCandidate[tiny.Key, tiny.Node]{
		NodeID: candidate,
	})
	// the state machine should attempt to send a message
	require.IsType(t, &StateIncludeConnectivityCheck[tiny.Key]{}, state)

	st := state.(*StateIncludeConnectivityCheck[tiny.Key])

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

	rt := simplert.New[tiny.Key, tiny.Node](tiny.NewNode(tiny.Key(128)), 5)
	p, err := NewInclude[tiny.Key, tiny.Node](rt, cfg)
	require.NoError(t, err)

	candidate := tiny.NewNode(tiny.Key(0b00000100))

	// add a candidate
	state := p.Advance(ctx, &EventIncludeAddCandidate[tiny.Key, tiny.Node]{
		NodeID: candidate,
	})
	require.IsType(t, &StateIncludeConnectivityCheck[tiny.Key]{}, state)

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

	rt := simplert.New[tiny.Key, tiny.Node](tiny.NewNode(tiny.Key(128)), 5)

	p, err := NewInclude[tiny.Key, tiny.Node](rt, cfg)
	require.NoError(t, err)

	// add a candidate
	state := p.Advance(ctx, &EventIncludeAddCandidate[tiny.Key, tiny.Node]{
		NodeID: tiny.NewNode(tiny.Key(0b00000100)),
	})
	require.IsType(t, &StateIncludeConnectivityCheck[tiny.Key]{}, state)

	// include reports that it is waiting and has capacity for more
	state = p.Advance(ctx, &EventIncludePoll{})
	require.IsType(t, &StateIncludeWaitingWithCapacity{}, state)

	// add second candidate
	state = p.Advance(ctx, &EventIncludeAddCandidate[tiny.Key, tiny.Node]{
		NodeID: tiny.NewNode(tiny.Key(0b00000010)),
	})
	// sends a message to the candidate
	require.IsType(t, &StateIncludeConnectivityCheck[tiny.Key]{}, state)

	// include reports that it is waiting and has capacity for more
	state = p.Advance(ctx, &EventIncludePoll{})
	// sends a message to the candidate
	require.IsType(t, &StateIncludeWaitingWithCapacity{}, state)

	// add third candidate
	state = p.Advance(ctx, &EventIncludeAddCandidate[tiny.Key, tiny.Node]{
		NodeID: tiny.NewNode(tiny.Key(0b00000011)),
	})
	// sends a message to the candidate
	require.IsType(t, &StateIncludeConnectivityCheck[tiny.Key]{}, state)

	// include reports that it is waiting at capacity since 3 messages are in flight
	state = p.Advance(ctx, &EventIncludePoll{})
	require.IsType(t, &StateIncludeWaitingAtCapacity{}, state)

	// add fourth candidate
	state = p.Advance(ctx, &EventIncludeAddCandidate[tiny.Key, tiny.Node]{
		NodeID: tiny.NewNode(tiny.Key(0b00000101)),
	})

	// include reports that it is waiting at capacity since 3 messages are already in flight
	require.IsType(t, &StateIncludeWaitingAtCapacity{}, state)

	// add fifth candidate
	state = p.Advance(ctx, &EventIncludeAddCandidate[tiny.Key, tiny.Node]{
		NodeID: tiny.NewNode(tiny.Key(0b00000110)),
	})

	// include reports that it is waiting and the candidate queue is full since it
	// is configured to have 3 concurrent checks and 2 queued
	require.IsType(t, &StateIncludeWaitingFull{}, state)

	// add sixth candidate
	state = p.Advance(ctx, &EventIncludeAddCandidate[tiny.Key, tiny.Node]{
		NodeID: tiny.NewNode(tiny.Key(0b00000111)),
	})

	// include reports that it is still waiting and the candidate queue is full since it
	// is configured to have 3 concurrent checks and 2 queued
	require.IsType(t, &StateIncludeWaitingFull{}, state)
}

func TestIncludeConnectivityCheckSuccess(t *testing.T) {
	ctx := context.Background()
	clk := clock.NewMock()
	cfg := DefaultIncludeConfig()
	cfg.Clock = clk
	cfg.Concurrency = 2

	rt := simplert.New[tiny.Key, tiny.Node](tiny.NewNode(tiny.Key(128)), 5)

	p, err := NewInclude[tiny.Key, tiny.Node](rt, cfg)
	require.NoError(t, err)

	// add a candidate
	state := p.Advance(ctx, &EventIncludeAddCandidate[tiny.Key, tiny.Node]{
		NodeID: tiny.NewNode(tiny.Key(0b00000100)),
	})
	require.IsType(t, &StateIncludeConnectivityCheck[tiny.Key]{}, state)

	// notify that node was contacted successfully, with no closer nodes
	state = p.Advance(ctx, &EventIncludeConnectivityCheckSuccess[tiny.Key, tiny.Node]{
		NodeID: tiny.NewNode(tiny.Key(0b00000100)),
	})

	// should respond that the routing table was updated
	require.IsType(t, &StateIncludeRoutingUpdated[tiny.Key]{}, state)

	st := state.(*StateIncludeRoutingUpdated[tiny.Key])

	// the update is for the correct node
	require.Equal(t, tiny.NewNode(tiny.Key(4)), st.NodeID)

	// the routing table should contain the node
	foundNode, found := rt.GetNode(tiny.Key(4))
	require.True(t, found)
	require.NotNil(t, foundNode)

	require.True(t, key.Equal(foundNode.Key(), tiny.Key(4)))

	// advancing again should reports that it is idle
	state = p.Advance(ctx, &EventIncludePoll{})
	require.IsType(t, &StateIncludeIdle{}, state)
}

func TestIncludeConnectivityCheckFailure(t *testing.T) {
	ctx := context.Background()
	clk := clock.NewMock()
	cfg := DefaultIncludeConfig()
	cfg.Clock = clk
	cfg.Concurrency = 2

	rt := simplert.New[tiny.Key, tiny.Node](tiny.NewNode(tiny.Key(128)), 5)

	p, err := NewInclude[tiny.Key, tiny.Node](rt, cfg)
	require.NoError(t, err)

	// add a candidate
	state := p.Advance(ctx, &EventIncludeAddCandidate[tiny.Key, tiny.Node]{
		NodeID: tiny.NewNode(tiny.Key(0b00000100)),
	})
	require.IsType(t, &StateIncludeConnectivityCheck[tiny.Key]{}, state)

	// notify that node was not contacted successfully
	state = p.Advance(ctx, &EventIncludeConnectivityCheckFailure[tiny.Key, tiny.Node]{
		NodeID: tiny.NewNode(tiny.Key(0b00000100)),
	})

	// should respond that state machine is idle
	require.IsType(t, &StateIncludeIdle{}, state)

	// the routing table should not contain the node
	foundNode, found := rt.GetNode(tiny.Key(4))
	require.False(t, found)
	require.Zero(t, foundNode)
}
