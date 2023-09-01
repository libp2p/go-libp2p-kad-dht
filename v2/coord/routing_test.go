package kademlia

import (
	"errors"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/plprobelab/go-kademlia/kad"
	"github.com/plprobelab/go-kademlia/key"
	"github.com/plprobelab/go-kademlia/network/address"
	"github.com/plprobelab/go-kademlia/query"
	"github.com/plprobelab/go-kademlia/routing"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slog"

	"github.com/libp2p/go-libp2p-kad-dht/v2/coord/internal/kadtest"
	"github.com/libp2p/go-libp2p-kad-dht/v2/coord/internal/nettest"
)

func TestRoutingStartBootstrapSendsEvent(t *testing.T) {
	ctx, cancel := kadtest.CtxShort(t)
	defer cancel()

	clk := clock.NewMock()
	_, nodes := nettest.LinearTopology(4, clk)

	self := nodes[0].NodeInfo.ID()

	// records the event passed to bootstrap
	bootstrap := NewRecordingSM[routing.BootstrapEvent, routing.BootstrapState](&routing.StateBootstrapIdle{})
	include := new(NullSM[routing.IncludeEvent, routing.IncludeState])
	probe := new(NullSM[routing.ProbeEvent, routing.ProbeState])

	routingBehaviour := NewRoutingBehaviour[key.Key8, kadtest.StrAddr](self, bootstrap, include, probe, slog.Default())

	ev := &EventDhtStartBootstrap[key.Key8, kadtest.StrAddr]{
		ProtocolID: address.ProtocolID("test"),
		Message:    kadtest.NewRequest("1", self.Key()),
		SeedNodes:  []kad.NodeID[key.Key8]{nodes[1].NodeInfo.ID()},
	}

	routingBehaviour.Notify(ctx, ev)

	// the event that should be passed to the bootstrap state machine
	expected := &routing.EventBootstrapStart[key.Key8, kadtest.StrAddr]{
		ProtocolID:        ev.ProtocolID,
		Message:           ev.Message,
		KnownClosestNodes: ev.SeedNodes,
	}
	require.Equal(t, expected, bootstrap.Received)
}

func TestRoutingBootstrapGetClosestNodesSuccess(t *testing.T) {
	ctx, cancel := kadtest.CtxShort(t)
	defer cancel()

	clk := clock.NewMock()
	_, nodes := nettest.LinearTopology(4, clk)

	self := nodes[0].NodeInfo.ID()

	// records the event passed to bootstrap
	bootstrap := NewRecordingSM[routing.BootstrapEvent, routing.BootstrapState](&routing.StateBootstrapIdle{})
	include := new(NullSM[routing.IncludeEvent, routing.IncludeState])
	probe := new(NullSM[routing.ProbeEvent, routing.ProbeState])

	routingBehaviour := NewRoutingBehaviour[key.Key8, kadtest.StrAddr](self, bootstrap, include, probe, slog.Default())

	ev := &EventGetClosestNodesSuccess[key.Key8, kadtest.StrAddr]{
		QueryID:      query.QueryID("bootstrap"),
		To:           nodes[1].NodeInfo,
		Target:       nodes[0].NodeInfo.ID().Key(),
		ClosestNodes: []kad.NodeInfo[key.Key8, kadtest.StrAddr]{nodes[2].NodeInfo},
	}

	routingBehaviour.Notify(ctx, ev)

	// bootstrap should receive message response event
	require.IsType(t, &routing.EventBootstrapMessageResponse[key.Key8, kadtest.StrAddr]{}, bootstrap.Received)

	rev := bootstrap.Received.(*routing.EventBootstrapMessageResponse[key.Key8, kadtest.StrAddr])
	require.Equal(t, nodes[1].NodeInfo.ID(), rev.NodeID)
	require.Equal(t, ev.ClosestNodes, rev.Response.CloserNodes())
}

func TestRoutingBootstrapGetClosestNodesFailure(t *testing.T) {
	ctx, cancel := kadtest.CtxShort(t)
	defer cancel()

	clk := clock.NewMock()
	_, nodes := nettest.LinearTopology(4, clk)

	self := nodes[0].NodeInfo.ID()

	// records the event passed to bootstrap
	bootstrap := NewRecordingSM[routing.BootstrapEvent, routing.BootstrapState](&routing.StateBootstrapIdle{})
	include := new(NullSM[routing.IncludeEvent, routing.IncludeState])
	probe := new(NullSM[routing.ProbeEvent, routing.ProbeState])

	routingBehaviour := NewRoutingBehaviour[key.Key8, kadtest.StrAddr](self, bootstrap, include, probe, slog.Default())

	failure := errors.New("failed")
	ev := &EventGetClosestNodesFailure[key.Key8, kadtest.StrAddr]{
		QueryID: query.QueryID("bootstrap"),
		To:      nodes[1].NodeInfo,
		Target:  nodes[0].NodeInfo.ID().Key(),
		Err:     failure,
	}

	routingBehaviour.Notify(ctx, ev)

	// bootstrap should receive message response event
	require.IsType(t, &routing.EventBootstrapMessageFailure[key.Key8]{}, bootstrap.Received)

	rev := bootstrap.Received.(*routing.EventBootstrapMessageFailure[key.Key8])
	require.Equal(t, nodes[1].NodeInfo.ID(), rev.NodeID)
	require.Equal(t, failure, rev.Error)
}

func TestRoutingAddNodeInfoSendsEvent(t *testing.T) {
	ctx, cancel := kadtest.CtxShort(t)
	defer cancel()

	clk := clock.NewMock()
	_, nodes := nettest.LinearTopology(4, clk)

	self := nodes[0].NodeInfo.ID()

	// records the event passed to include
	include := NewRecordingSM[routing.IncludeEvent, routing.IncludeState](&routing.StateIncludeIdle{})

	bootstrap := new(NullSM[routing.BootstrapEvent, routing.BootstrapState])
	probe := new(NullSM[routing.ProbeEvent, routing.ProbeState])

	routingBehaviour := NewRoutingBehaviour[key.Key8, kadtest.StrAddr](self, bootstrap, include, probe, slog.Default())

	ev := &EventDhtAddNodeInfo[key.Key8, kadtest.StrAddr]{
		NodeInfo: nodes[2].NodeInfo,
	}

	routingBehaviour.Notify(ctx, ev)

	// the event that should be passed to the include state machine
	expected := &routing.EventIncludeAddCandidate[key.Key8, kadtest.StrAddr]{
		NodeInfo: ev.NodeInfo,
	}
	require.Equal(t, expected, include.Received)
}

func TestRoutingIncludeGetClosestNodesSuccess(t *testing.T) {
	ctx, cancel := kadtest.CtxShort(t)
	defer cancel()

	clk := clock.NewMock()
	_, nodes := nettest.LinearTopology(4, clk)

	self := nodes[0].NodeInfo.ID()

	// records the event passed to include
	include := NewRecordingSM[routing.IncludeEvent, routing.IncludeState](&routing.StateIncludeIdle{})

	bootstrap := new(NullSM[routing.BootstrapEvent, routing.BootstrapState])
	probe := new(NullSM[routing.ProbeEvent, routing.ProbeState])

	routingBehaviour := NewRoutingBehaviour[key.Key8, kadtest.StrAddr](self, bootstrap, include, probe, slog.Default())

	ev := &EventGetClosestNodesSuccess[key.Key8, kadtest.StrAddr]{
		QueryID:      query.QueryID("include"),
		To:           nodes[1].NodeInfo,
		Target:       nodes[0].NodeInfo.ID().Key(),
		ClosestNodes: []kad.NodeInfo[key.Key8, kadtest.StrAddr]{nodes[2].NodeInfo},
	}

	routingBehaviour.Notify(ctx, ev)

	// include should receive message response event
	require.IsType(t, &routing.EventIncludeMessageResponse[key.Key8, kadtest.StrAddr]{}, include.Received)

	rev := include.Received.(*routing.EventIncludeMessageResponse[key.Key8, kadtest.StrAddr])
	require.Equal(t, nodes[1].NodeInfo, rev.NodeInfo)
	require.Equal(t, ev.ClosestNodes, rev.Response.CloserNodes())
}

func TestRoutingIncludeGetClosestNodesFailure(t *testing.T) {
	ctx, cancel := kadtest.CtxShort(t)
	defer cancel()

	clk := clock.NewMock()
	_, nodes := nettest.LinearTopology(4, clk)

	self := nodes[0].NodeInfo.ID()

	// records the event passed to include
	include := NewRecordingSM[routing.IncludeEvent, routing.IncludeState](&routing.StateIncludeIdle{})

	bootstrap := new(NullSM[routing.BootstrapEvent, routing.BootstrapState])
	probe := new(NullSM[routing.ProbeEvent, routing.ProbeState])

	routingBehaviour := NewRoutingBehaviour[key.Key8, kadtest.StrAddr](self, bootstrap, include, probe, slog.Default())

	failure := errors.New("failed")
	ev := &EventGetClosestNodesFailure[key.Key8, kadtest.StrAddr]{
		QueryID: query.QueryID("include"),
		To:      nodes[1].NodeInfo,
		Target:  nodes[0].NodeInfo.ID().Key(),
		Err:     failure,
	}

	routingBehaviour.Notify(ctx, ev)

	// include should receive message response event
	require.IsType(t, &routing.EventIncludeMessageFailure[key.Key8, kadtest.StrAddr]{}, include.Received)

	rev := include.Received.(*routing.EventIncludeMessageFailure[key.Key8, kadtest.StrAddr])
	require.Equal(t, nodes[1].NodeInfo, rev.NodeInfo)
	require.Equal(t, failure, rev.Error)
}

func TestRoutingIncludedNodeAddToProbeList(t *testing.T) {
	ctx, cancel := kadtest.CtxShort(t)
	defer cancel()

	clk := clock.NewMock()
	_, nodes := nettest.LinearTopology(4, clk)

	self := nodes[0].NodeInfo.ID()
	rt := nodes[0].RoutingTable

	includeCfg := routing.DefaultIncludeConfig()
	includeCfg.Clock = clk
	include, err := routing.NewInclude[key.Key8, kadtest.StrAddr](rt, includeCfg)
	require.NoError(t, err)

	probeCfg := routing.DefaultProbeConfig()
	probeCfg.Clock = clk
	probeCfg.CheckInterval = 5 * time.Minute
	probe, err := routing.NewProbe[key.Key8, kadtest.StrAddr](rt, probeCfg)
	require.NoError(t, err)

	// ensure bootstrap is always idle
	bootstrap := NewRecordingSM[routing.BootstrapEvent, routing.BootstrapState](&routing.StateBootstrapIdle{})

	routingBehaviour := NewRoutingBehaviour[key.Key8, kadtest.StrAddr](self, bootstrap, include, probe, slog.Default())

	// a new node to be included
	candidate := nodes[len(nodes)-1].NodeInfo

	// the routing table should not contain the node yet
	_, intable := rt.GetNode(candidate.ID().Key())
	require.False(t, intable)

	// notify that there is a new node to be included
	routingBehaviour.Notify(ctx, &EventDhtAddNodeInfo[key.Key8, kadtest.StrAddr]{
		NodeInfo: candidate,
	})

	// collect the result of the notify
	dev, ok := routingBehaviour.Perform(ctx)
	require.True(t, ok)

	// include should be asking to send a message to the node
	require.IsType(t, &EventOutboundGetClosestNodes[key.Key8, kadtest.StrAddr]{}, dev)

	oev := dev.(*EventOutboundGetClosestNodes[key.Key8, kadtest.StrAddr])

	// advance time a little
	clk.Add(time.Second)

	// notify a successful response back (best to use the notify included in the event even though it will be the behaviour's Notify method)
	oev.Notify.Notify(ctx, &EventGetClosestNodesSuccess[key.Key8, kadtest.StrAddr]{
		QueryID:      oev.QueryID,
		To:           oev.To,
		Target:       oev.Target,
		ClosestNodes: []kad.NodeInfo[key.Key8, kadtest.StrAddr]{nodes[1].NodeInfo}, // must include one for include check to pass
	})

	// the routing table should now contain the node
	_, intable = rt.GetNode(candidate.ID().Key())
	require.True(t, intable)

	// routing update event should be emitted from the include state machine
	dev, ok = routingBehaviour.Perform(ctx)
	require.True(t, ok)
	require.IsType(t, &EventRoutingUpdated[key.Key8, kadtest.StrAddr]{}, dev)

	// advance time past the probe check interval
	clk.Add(probeCfg.CheckInterval)

	// routing update event should be emitted from the include state machine
	dev, ok = routingBehaviour.Perform(ctx)
	require.True(t, ok)
	require.IsType(t, &EventOutboundGetClosestNodes[key.Key8, kadtest.StrAddr]{}, dev)

	// confirm that the message is for the correct node
	oev = dev.(*EventOutboundGetClosestNodes[key.Key8, kadtest.StrAddr])
	require.Equal(t, query.QueryID("probe"), oev.QueryID)
	require.Equal(t, candidate.ID(), oev.To.ID())
	require.Equal(t, candidate.ID().Key(), oev.Target)
}
