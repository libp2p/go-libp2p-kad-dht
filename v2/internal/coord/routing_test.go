package coord

import (
	"errors"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"

	"github.com/libp2p/go-libp2p-kad-dht/v2/internal/coord/coordt"
	"github.com/libp2p/go-libp2p-kad-dht/v2/internal/coord/cplutil"
	"github.com/libp2p/go-libp2p-kad-dht/v2/internal/coord/internal/nettest"
	"github.com/libp2p/go-libp2p-kad-dht/v2/internal/coord/routing"
	"github.com/libp2p/go-libp2p-kad-dht/v2/internal/kadtest"
	"github.com/libp2p/go-libp2p-kad-dht/v2/kadt"
)

// idleBootstrap returns a bootstrap state machine that is always idle
func idleBootstrap() *RecordingSM[routing.BootstrapEvent, routing.BootstrapState] {
	return NewRecordingSM[routing.BootstrapEvent, routing.BootstrapState](&routing.StateBootstrapIdle{})
}

// idleInclude returns an include state machine that is always idle
func idleInclude() *RecordingSM[routing.IncludeEvent, routing.IncludeState] {
	return NewRecordingSM[routing.IncludeEvent, routing.IncludeState](&routing.StateIncludeIdle{})
}

// idleProbe returns a probe state machine that is always idle
func idleProbe() *RecordingSM[routing.ProbeEvent, routing.ProbeState] {
	return NewRecordingSM[routing.ProbeEvent, routing.ProbeState](&routing.StateProbeIdle{})
}

// idleExplore returns an explore state machine that is always idle
func idleExplore() *RecordingSM[routing.ExploreEvent, routing.ExploreState] {
	return NewRecordingSM[routing.ExploreEvent, routing.ExploreState](&routing.StateExploreIdle{})
}

func TestRoutingConfigValidate(t *testing.T) {
	t.Run("default is valid", func(t *testing.T) {
		cfg := DefaultRoutingConfig()

		require.NoError(t, cfg.Validate())
	})

	t.Run("clock is not nil", func(t *testing.T) {
		cfg := DefaultRoutingConfig()

		cfg.Clock = nil
		require.Error(t, cfg.Validate())
	})

	t.Run("logger not nil", func(t *testing.T) {
		cfg := DefaultRoutingConfig()
		cfg.Logger = nil
		require.Error(t, cfg.Validate())
	})

	t.Run("tracer not nil", func(t *testing.T) {
		cfg := DefaultRoutingConfig()
		cfg.Tracer = nil
		require.Error(t, cfg.Validate())
	})

	t.Run("bootstrap timeout positive", func(t *testing.T) {
		cfg := DefaultRoutingConfig()
		cfg.BootstrapTimeout = 0
		require.Error(t, cfg.Validate())
		cfg.BootstrapTimeout = -1
		require.Error(t, cfg.Validate())
	})

	t.Run("bootstrap request concurrency positive", func(t *testing.T) {
		cfg := DefaultRoutingConfig()
		cfg.BootstrapRequestConcurrency = 0
		require.Error(t, cfg.Validate())
		cfg.BootstrapRequestConcurrency = -1
		require.Error(t, cfg.Validate())
	})

	t.Run("bootstrap request timeout positive", func(t *testing.T) {
		cfg := DefaultRoutingConfig()
		cfg.BootstrapRequestTimeout = 0
		require.Error(t, cfg.Validate())
		cfg.BootstrapRequestTimeout = -1
		require.Error(t, cfg.Validate())
	})

	t.Run("connectivity check timeout positive", func(t *testing.T) {
		cfg := DefaultRoutingConfig()
		cfg.ConnectivityCheckTimeout = 0
		require.Error(t, cfg.Validate())
		cfg.ConnectivityCheckTimeout = -1
		require.Error(t, cfg.Validate())
	})

	t.Run("probe request concurrency positive", func(t *testing.T) {
		cfg := DefaultRoutingConfig()

		cfg.ProbeRequestConcurrency = 0
		require.Error(t, cfg.Validate())
		cfg.ProbeRequestConcurrency = -1
		require.Error(t, cfg.Validate())
	})

	t.Run("probe check interval positive", func(t *testing.T) {
		cfg := DefaultRoutingConfig()
		cfg.ProbeCheckInterval = 0
		require.Error(t, cfg.Validate())
		cfg.ProbeCheckInterval = -1
		require.Error(t, cfg.Validate())
	})

	t.Run("include request concurrency positive", func(t *testing.T) {
		cfg := DefaultRoutingConfig()

		cfg.IncludeRequestConcurrency = 0
		require.Error(t, cfg.Validate())
		cfg.IncludeRequestConcurrency = -1
		require.Error(t, cfg.Validate())
	})

	t.Run("include queue capacity positive", func(t *testing.T) {
		cfg := DefaultRoutingConfig()

		cfg.IncludeQueueCapacity = 0
		require.Error(t, cfg.Validate())
		cfg.IncludeQueueCapacity = -1
		require.Error(t, cfg.Validate())
	})

	t.Run("explore timeout positive", func(t *testing.T) {
		cfg := DefaultRoutingConfig()

		cfg.ExploreTimeout = 0
		require.Error(t, cfg.Validate())
		cfg.ExploreTimeout = -1
		require.Error(t, cfg.Validate())
	})

	t.Run("explore request concurrency positive", func(t *testing.T) {
		cfg := DefaultRoutingConfig()

		cfg.ExploreRequestConcurrency = 0
		require.Error(t, cfg.Validate())
		cfg.ExploreRequestConcurrency = -1
		require.Error(t, cfg.Validate())
	})

	t.Run("explore request timeout positive", func(t *testing.T) {
		cfg := DefaultRoutingConfig()

		cfg.ExploreRequestTimeout = 0
		require.Error(t, cfg.Validate())
		cfg.ExploreRequestTimeout = -1
		require.Error(t, cfg.Validate())
	})

	t.Run("explore maximum cpl positive", func(t *testing.T) {
		cfg := DefaultRoutingConfig()

		cfg.ExploreMaximumCpl = 0
		require.Error(t, cfg.Validate())
		cfg.ExploreMaximumCpl = -1
		require.Error(t, cfg.Validate())
	})

	t.Run("explore maximum 15 or less", func(t *testing.T) {
		cfg := DefaultRoutingConfig()

		cfg.ExploreMaximumCpl = 16
		require.Error(t, cfg.Validate())
	})

	t.Run("explore interval positive", func(t *testing.T) {
		cfg := DefaultRoutingConfig()

		cfg.ExploreInterval = 0
		require.Error(t, cfg.Validate())
		cfg.ExploreInterval = -1
		require.Error(t, cfg.Validate())
	})

	t.Run("explore interval multiplier at least 1", func(t *testing.T) {
		cfg := DefaultRoutingConfig()

		cfg.ExploreIntervalMultiplier = 0
		require.Error(t, cfg.Validate())
		cfg.ExploreIntervalMultiplier = 0.9
		require.Error(t, cfg.Validate())
		cfg.ExploreIntervalMultiplier = -1
		require.Error(t, cfg.Validate())
	})

	t.Run("explore interval between 0 and 0.05", func(t *testing.T) {
		cfg := DefaultRoutingConfig()

		cfg.ExploreIntervalJitter = 0.1
		require.Error(t, cfg.Validate())
		cfg.ExploreIntervalJitter = 0.05001
		require.Error(t, cfg.Validate())
		cfg.ExploreIntervalJitter = -0.1
		require.Error(t, cfg.Validate())
	})
}

func TestRoutingStartBootstrapSendsEvent(t *testing.T) {
	ctx := kadtest.CtxShort(t)

	clk := clock.NewMock()
	_, nodes, err := nettest.LinearTopology(4, clk)
	require.NoError(t, err)

	self := nodes[0].NodeID

	// records the event passed to bootstrap
	bootstrap := NewRecordingSM[routing.BootstrapEvent, routing.BootstrapState](&routing.StateBootstrapIdle{})

	cfg := DefaultRoutingConfig()
	cfg.Clock = clk
	routingBehaviour, err := ComposeRoutingBehaviour(self, bootstrap, idleInclude(), idleProbe(), idleExplore(), cfg)
	require.NoError(t, err)

	ev := &EventStartBootstrap{
		SeedNodes: []kadt.PeerID{nodes[1].NodeID},
	}

	routingBehaviour.Notify(ctx, ev)

	// the event that should be passed to the bootstrap state machine
	expected := &routing.EventBootstrapStart[kadt.Key, kadt.PeerID]{
		KnownClosestNodes: ev.SeedNodes,
	}
	require.Equal(t, expected, bootstrap.Received)
}

func TestRoutingBootstrapGetClosestNodesSuccess(t *testing.T) {
	ctx := kadtest.CtxShort(t)

	clk := clock.NewMock()
	_, nodes, err := nettest.LinearTopology(4, clk)
	require.NoError(t, err)

	self := nodes[0].NodeID

	// records the event passed to bootstrap
	bootstrap := NewRecordingSM[routing.BootstrapEvent, routing.BootstrapState](&routing.StateBootstrapIdle{})

	cfg := DefaultRoutingConfig()
	cfg.Clock = clk
	routingBehaviour, err := ComposeRoutingBehaviour(self, bootstrap, idleInclude(), idleProbe(), idleExplore(), cfg)
	require.NoError(t, err)

	ev := &EventGetCloserNodesSuccess{
		QueryID:     routing.BootstrapQueryID,
		To:          nodes[1].NodeID,
		Target:      nodes[0].NodeID.Key(),
		CloserNodes: []kadt.PeerID{nodes[2].NodeID},
	}

	routingBehaviour.Notify(ctx, ev)

	// bootstrap should receive message response event
	require.IsType(t, &routing.EventBootstrapFindCloserResponse[kadt.Key, kadt.PeerID]{}, bootstrap.Received)

	rev := bootstrap.Received.(*routing.EventBootstrapFindCloserResponse[kadt.Key, kadt.PeerID])
	require.True(t, nodes[1].NodeID.Equal(rev.NodeID))
	require.Equal(t, ev.CloserNodes, rev.CloserNodes)
}

func TestRoutingBootstrapGetClosestNodesFailure(t *testing.T) {
	ctx := kadtest.CtxShort(t)

	clk := clock.NewMock()
	_, nodes, err := nettest.LinearTopology(4, clk)
	require.NoError(t, err)

	self := nodes[0].NodeID

	// records the event passed to bootstrap
	bootstrap := NewRecordingSM[routing.BootstrapEvent, routing.BootstrapState](&routing.StateBootstrapIdle{})

	cfg := DefaultRoutingConfig()
	cfg.Clock = clk
	routingBehaviour, err := ComposeRoutingBehaviour(self, bootstrap, idleInclude(), idleProbe(), idleExplore(), cfg)
	require.NoError(t, err)

	failure := errors.New("failed")
	ev := &EventGetCloserNodesFailure{
		QueryID: routing.BootstrapQueryID,
		To:      nodes[1].NodeID,
		Target:  nodes[0].NodeID.Key(),
		Err:     failure,
	}

	routingBehaviour.Notify(ctx, ev)

	// bootstrap should receive message response event
	require.IsType(t, &routing.EventBootstrapFindCloserFailure[kadt.Key, kadt.PeerID]{}, bootstrap.Received)

	rev := bootstrap.Received.(*routing.EventBootstrapFindCloserFailure[kadt.Key, kadt.PeerID])
	require.Equal(t, peer.ID(nodes[1].NodeID), peer.ID(rev.NodeID))
	require.Equal(t, failure, rev.Error)
}

func TestRoutingAddNodeInfoSendsEvent(t *testing.T) {
	ctx := kadtest.CtxShort(t)

	clk := clock.NewMock()
	_, nodes, err := nettest.LinearTopology(4, clk)
	require.NoError(t, err)

	self := nodes[0].NodeID

	// records the event passed to include
	include := NewRecordingSM[routing.IncludeEvent, routing.IncludeState](&routing.StateIncludeIdle{})

	cfg := DefaultRoutingConfig()
	cfg.Clock = clk
	routingBehaviour, err := ComposeRoutingBehaviour(self, idleBootstrap(), include, idleProbe(), idleExplore(), cfg)
	require.NoError(t, err)

	ev := &EventAddNode{
		NodeID: nodes[2].NodeID,
	}

	routingBehaviour.Notify(ctx, ev)

	// the event that should be passed to the include state machine
	expected := &routing.EventIncludeAddCandidate[kadt.Key, kadt.PeerID]{
		NodeID: ev.NodeID,
	}
	require.Equal(t, expected, include.Received)
}

func TestRoutingIncludeGetClosestNodesSuccess(t *testing.T) {
	ctx := kadtest.CtxShort(t)

	clk := clock.NewMock()
	_, nodes, err := nettest.LinearTopology(4, clk)
	require.NoError(t, err)

	self := nodes[0].NodeID

	// records the event passed to include
	include := NewRecordingSM[routing.IncludeEvent, routing.IncludeState](&routing.StateIncludeIdle{})

	cfg := DefaultRoutingConfig()
	cfg.Clock = clk
	routingBehaviour, err := ComposeRoutingBehaviour(self, idleBootstrap(), include, idleProbe(), idleExplore(), cfg)
	require.NoError(t, err)

	ev := &EventGetCloserNodesSuccess{
		QueryID:     coordt.QueryID("include"),
		To:          nodes[1].NodeID,
		Target:      nodes[0].NodeID.Key(),
		CloserNodes: []kadt.PeerID{nodes[2].NodeID},
	}

	routingBehaviour.Notify(ctx, ev)

	// include should receive message response event
	require.IsType(t, &routing.EventIncludeConnectivityCheckSuccess[kadt.Key, kadt.PeerID]{}, include.Received)

	rev := include.Received.(*routing.EventIncludeConnectivityCheckSuccess[kadt.Key, kadt.PeerID])
	require.Equal(t, peer.ID(nodes[1].NodeID), peer.ID(rev.NodeID))
}

func TestRoutingIncludeGetClosestNodesFailure(t *testing.T) {
	ctx := kadtest.CtxShort(t)

	clk := clock.NewMock()
	_, nodes, err := nettest.LinearTopology(4, clk)
	require.NoError(t, err)

	self := nodes[0].NodeID

	// records the event passed to include
	include := NewRecordingSM[routing.IncludeEvent, routing.IncludeState](&routing.StateIncludeIdle{})

	cfg := DefaultRoutingConfig()
	cfg.Clock = clk
	routingBehaviour, err := ComposeRoutingBehaviour(self, idleBootstrap(), include, idleProbe(), idleExplore(), cfg)
	require.NoError(t, err)

	failure := errors.New("failed")
	ev := &EventGetCloserNodesFailure{
		QueryID: coordt.QueryID("include"),
		To:      nodes[1].NodeID,
		Target:  nodes[0].NodeID.Key(),
		Err:     failure,
	}

	routingBehaviour.Notify(ctx, ev)

	// include should receive message response event
	require.IsType(t, &routing.EventIncludeConnectivityCheckFailure[kadt.Key, kadt.PeerID]{}, include.Received)

	rev := include.Received.(*routing.EventIncludeConnectivityCheckFailure[kadt.Key, kadt.PeerID])
	require.Equal(t, peer.ID(nodes[1].NodeID), peer.ID(rev.NodeID))
	require.Equal(t, failure, rev.Error)
}

func TestRoutingIncludedNodeAddToProbeList(t *testing.T) {
	ctx := kadtest.CtxShort(t)

	clk := clock.NewMock()
	_, nodes, err := nettest.LinearTopology(4, clk)
	require.NoError(t, err)

	self := nodes[0].NodeID
	rt := nodes[0].RoutingTable

	includeCfg := routing.DefaultIncludeConfig()
	includeCfg.Clock = clk
	include, err := routing.NewInclude[kadt.Key, kadt.PeerID](rt, includeCfg)
	require.NoError(t, err)

	probeCfg := routing.DefaultProbeConfig()
	probeCfg.Clock = clk
	probeCfg.CheckInterval = 5 * time.Minute
	probe, err := routing.NewProbe[kadt.Key](rt, probeCfg)
	require.NoError(t, err)

	cfg := DefaultRoutingConfig()
	cfg.Clock = clk
	routingBehaviour, err := ComposeRoutingBehaviour(self, idleBootstrap(), include, probe, idleExplore(), cfg)
	require.NoError(t, err)

	// a new node to be included
	candidate := nodes[len(nodes)-1].NodeID

	// the routing table should not contain the node yet
	_, intable := rt.GetNode(candidate.Key())
	require.False(t, intable)

	// notify that there is a new node to be included
	routingBehaviour.Notify(ctx, &EventAddNode{
		NodeID: candidate,
	})

	// collect the result of the notify
	dev, ok := routingBehaviour.Perform(ctx)
	require.True(t, ok)

	// include should be asking to send a message to the node
	require.IsType(t, &EventOutboundGetCloserNodes{}, dev)

	oev := dev.(*EventOutboundGetCloserNodes)

	// advance time a little
	clk.Add(time.Second)

	// notify a successful response back (best to use the notify included in the event even though it will be the behaviour's Notify method)
	oev.Notify.Notify(ctx, &EventGetCloserNodesSuccess{
		QueryID:     oev.QueryID,
		To:          oev.To,
		Target:      oev.Target,
		CloserNodes: []kadt.PeerID{nodes[1].NodeID}, // must include one for include check to pass
	})

	// the routing table should now contain the node
	_, intable = rt.GetNode(candidate.Key())
	require.True(t, intable)

	// routing update event should be emitted from the include state machine
	dev, ok = routingBehaviour.Perform(ctx)
	require.True(t, ok)
	require.IsType(t, &EventRoutingUpdated{}, dev)

	// advance time past the probe check interval
	clk.Add(probeCfg.CheckInterval)

	// routing update event should be emitted from the include state machine
	dev, ok = routingBehaviour.Perform(ctx)
	require.True(t, ok)
	require.IsType(t, &EventOutboundGetCloserNodes{}, dev)

	// confirm that the message is for the correct node
	oev = dev.(*EventOutboundGetCloserNodes)
	require.Equal(t, coordt.QueryID("probe"), oev.QueryID)
	require.Equal(t, candidate, oev.To)
}

func TestRoutingExploreSendsEvent(t *testing.T) {
	ctx := kadtest.CtxShort(t)

	clk := clock.NewMock()
	_, nodes, err := nettest.LinearTopology(4, clk)
	require.NoError(t, err)

	self := nodes[0].NodeID
	rt := nodes[0].RoutingTable

	exploreCfg := routing.DefaultExploreConfig()
	exploreCfg.Clock = clk

	// make sure the explore starts as soon as the explore state machine is polled
	schedule := routing.NewNoWaitExploreSchedule(14)

	explore, err := routing.NewExplore[kadt.Key](self, rt, cplutil.GenRandPeerID, schedule, exploreCfg)
	require.NoError(t, err)

	cfg := DefaultRoutingConfig()
	cfg.Clock = clk
	routingBehaviour, err := ComposeRoutingBehaviour(self, idleBootstrap(), idleInclude(), idleProbe(), explore, cfg)
	require.NoError(t, err)

	routingBehaviour.Notify(ctx, &EventRoutingPoll{})

	// collect the result of the notify
	dev, ok := routingBehaviour.Perform(ctx)
	require.True(t, ok)

	// include should be asking to send a message to the node
	require.IsType(t, &EventOutboundGetCloserNodes{}, dev)
	gcl := dev.(*EventOutboundGetCloserNodes)

	require.Equal(t, routing.ExploreQueryID, gcl.QueryID)

	// the message should be looking for nodes closer to a key that occupies cpl 14
	require.Equal(t, 14, self.Key().CommonPrefixLength(gcl.Target))
}

func TestRoutingExploreGetClosestNodesSuccess(t *testing.T) {
	ctx := kadtest.CtxShort(t)

	clk := clock.NewMock()
	_, nodes, err := nettest.LinearTopology(4, clk)
	require.NoError(t, err)

	self := nodes[0].NodeID

	// records the event passed to explore
	explore := NewRecordingSM[routing.ExploreEvent, routing.ExploreState](&routing.StateExploreIdle{})

	cfg := DefaultRoutingConfig()
	cfg.Clock = clk
	routingBehaviour, err := ComposeRoutingBehaviour(self, idleBootstrap(), idleInclude(), idleProbe(), explore, cfg)
	require.NoError(t, err)

	ev := &EventGetCloserNodesSuccess{
		QueryID:     routing.ExploreQueryID,
		To:          nodes[1].NodeID,
		Target:      nodes[0].NodeID.Key(),
		CloserNodes: []kadt.PeerID{nodes[2].NodeID},
	}
	routingBehaviour.Notify(ctx, ev)

	// explore should receive message response event
	require.IsType(t, &routing.EventExploreFindCloserResponse[kadt.Key, kadt.PeerID]{}, explore.Received)

	rev := explore.Received.(*routing.EventExploreFindCloserResponse[kadt.Key, kadt.PeerID])
	require.True(t, nodes[1].NodeID.Equal(rev.NodeID))
	require.Equal(t, ev.CloserNodes, rev.CloserNodes)
}

func TestRoutingExploreGetClosestNodesFailure(t *testing.T) {
	ctx := kadtest.CtxShort(t)

	clk := clock.NewMock()
	_, nodes, err := nettest.LinearTopology(4, clk)
	require.NoError(t, err)

	self := nodes[0].NodeID

	// records the event passed to explore
	explore := NewRecordingSM[routing.ExploreEvent, routing.ExploreState](&routing.StateExploreIdle{})

	cfg := DefaultRoutingConfig()
	cfg.Clock = clk
	routingBehaviour, err := ComposeRoutingBehaviour(self, idleBootstrap(), idleInclude(), idleProbe(), explore, cfg)
	require.NoError(t, err)

	failure := errors.New("failed")
	ev := &EventGetCloserNodesFailure{
		QueryID: routing.ExploreQueryID,
		To:      nodes[1].NodeID,
		Target:  nodes[0].NodeID.Key(),
		Err:     failure,
	}

	routingBehaviour.Notify(ctx, ev)

	// bootstrap should receive message response event
	require.IsType(t, &routing.EventExploreFindCloserFailure[kadt.Key, kadt.PeerID]{}, explore.Received)

	rev := explore.Received.(*routing.EventExploreFindCloserFailure[kadt.Key, kadt.PeerID])
	require.Equal(t, peer.ID(nodes[1].NodeID), peer.ID(rev.NodeID))
	require.Equal(t, failure, rev.Error)
}
