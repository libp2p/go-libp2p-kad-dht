package coord

import (
	"errors"
	"testing"
	"time"

	"go.opentelemetry.io/otel"

	"github.com/benbjohnson/clock"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/plprobelab/go-kademlia/network/address"
	"github.com/plprobelab/go-kademlia/query"
	"github.com/plprobelab/go-kademlia/routing"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slog"

	"github.com/libp2p/go-libp2p-kad-dht/v2/coord/internal/nettest"
	"github.com/libp2p/go-libp2p-kad-dht/v2/internal/kadtest"
	"github.com/libp2p/go-libp2p-kad-dht/v2/kadt"
	"github.com/libp2p/go-libp2p-kad-dht/v2/pb"
)

func TestRoutingStartBootstrapSendsEvent(t *testing.T) {
	ctx, cancel := kadtest.CtxShort(t)
	defer cancel()

	clk := clock.NewMock()
	_, nodes, err := nettest.LinearTopology(4, clk)
	require.NoError(t, err)

	self := nodes[0].NodeInfo.ID

	// records the event passed to bootstrap
	bootstrap := NewRecordingSM[routing.BootstrapEvent, routing.BootstrapState](&routing.StateBootstrapIdle{})
	include := new(NullSM[routing.IncludeEvent, routing.IncludeState])
	probe := new(NullSM[routing.ProbeEvent, routing.ProbeState])

	routingBehaviour := NewRoutingBehaviour(self, bootstrap, include, probe, slog.Default(), otel.Tracer("test"))

	req := &pb.Message{
		Type: pb.Message_FIND_NODE,
		Key:  []byte(self),
	}

	ev := &EventStartBootstrap{
		ProtocolID: address.ProtocolID("test"),
		Message:    req,
		SeedNodes:  []peer.ID{nodes[1].NodeInfo.ID},
	}

	routingBehaviour.Notify(ctx, ev)

	// the event that should be passed to the bootstrap state machine
	expected := &routing.EventBootstrapStart[KadKey, ma.Multiaddr]{
		ProtocolID:        ev.ProtocolID,
		Message:           ev.Message,
		KnownClosestNodes: SliceOfPeerIDToSliceOfNodeID(ev.SeedNodes),
	}
	require.Equal(t, expected, bootstrap.Received)
}

func TestRoutingBootstrapGetClosestNodesSuccess(t *testing.T) {
	ctx, cancel := kadtest.CtxShort(t)
	defer cancel()

	clk := clock.NewMock()
	_, nodes, err := nettest.LinearTopology(4, clk)
	require.NoError(t, err)

	self := nodes[0].NodeInfo.ID

	// records the event passed to bootstrap
	bootstrap := NewRecordingSM[routing.BootstrapEvent, routing.BootstrapState](&routing.StateBootstrapIdle{})
	include := new(NullSM[routing.IncludeEvent, routing.IncludeState])
	probe := new(NullSM[routing.ProbeEvent, routing.ProbeState])

	routingBehaviour := NewRoutingBehaviour(self, bootstrap, include, probe, slog.Default(), otel.Tracer("test"))

	ev := &EventGetCloserNodesSuccess{
		QueryID:     query.QueryID("bootstrap"),
		To:          nodes[1].NodeInfo,
		Target:      kadt.PeerID(nodes[0].NodeInfo.ID).Key(),
		CloserNodes: []peer.AddrInfo{nodes[2].NodeInfo},
	}

	routingBehaviour.Notify(ctx, ev)

	// bootstrap should receive message response event
	require.IsType(t, &routing.EventBootstrapMessageResponse[KadKey, ma.Multiaddr]{}, bootstrap.Received)

	rev := bootstrap.Received.(*routing.EventBootstrapMessageResponse[KadKey, ma.Multiaddr])
	require.Equal(t, nodes[1].NodeInfo.ID, NodeIDToPeerID(rev.NodeID))
	require.Equal(t, ev.CloserNodes, SliceOfNodeInfoToSliceOfAddrInfo(rev.Response.CloserNodes()))
}

func TestRoutingBootstrapGetClosestNodesFailure(t *testing.T) {
	ctx, cancel := kadtest.CtxShort(t)
	defer cancel()

	clk := clock.NewMock()
	_, nodes, err := nettest.LinearTopology(4, clk)
	require.NoError(t, err)

	self := nodes[0].NodeInfo.ID

	// records the event passed to bootstrap
	bootstrap := NewRecordingSM[routing.BootstrapEvent, routing.BootstrapState](&routing.StateBootstrapIdle{})
	include := new(NullSM[routing.IncludeEvent, routing.IncludeState])
	probe := new(NullSM[routing.ProbeEvent, routing.ProbeState])

	routingBehaviour := NewRoutingBehaviour(self, bootstrap, include, probe, slog.Default(), otel.Tracer("test"))

	failure := errors.New("failed")
	ev := &EventGetCloserNodesFailure{
		QueryID: query.QueryID("bootstrap"),
		To:      nodes[1].NodeInfo,
		Target:  kadt.PeerID(nodes[0].NodeInfo.ID).Key(),
		Err:     failure,
	}

	routingBehaviour.Notify(ctx, ev)

	// bootstrap should receive message response event
	require.IsType(t, &routing.EventBootstrapMessageFailure[KadKey]{}, bootstrap.Received)

	rev := bootstrap.Received.(*routing.EventBootstrapMessageFailure[KadKey])
	require.Equal(t, nodes[1].NodeInfo.ID, NodeIDToPeerID(rev.NodeID))
	require.Equal(t, failure, rev.Error)
}

func TestRoutingAddNodeInfoSendsEvent(t *testing.T) {
	ctx, cancel := kadtest.CtxShort(t)
	defer cancel()

	clk := clock.NewMock()
	_, nodes, err := nettest.LinearTopology(4, clk)
	require.NoError(t, err)

	self := nodes[0].NodeInfo.ID

	// records the event passed to include
	include := NewRecordingSM[routing.IncludeEvent, routing.IncludeState](&routing.StateIncludeIdle{})

	bootstrap := new(NullSM[routing.BootstrapEvent, routing.BootstrapState])
	probe := new(NullSM[routing.ProbeEvent, routing.ProbeState])

	routingBehaviour := NewRoutingBehaviour(self, bootstrap, include, probe, slog.Default(), otel.Tracer("test"))

	ev := &EventAddAddrInfo{
		NodeInfo: nodes[2].NodeInfo,
	}

	routingBehaviour.Notify(ctx, ev)

	// the event that should be passed to the include state machine
	expected := &routing.EventIncludeAddCandidate[KadKey, ma.Multiaddr]{
		NodeInfo: kadt.AddrInfo{Info: ev.NodeInfo},
	}
	require.Equal(t, expected, include.Received)
}

func TestRoutingIncludeGetClosestNodesSuccess(t *testing.T) {
	ctx, cancel := kadtest.CtxShort(t)
	defer cancel()

	clk := clock.NewMock()
	_, nodes, err := nettest.LinearTopology(4, clk)
	require.NoError(t, err)

	self := nodes[0].NodeInfo.ID

	// records the event passed to include
	include := NewRecordingSM[routing.IncludeEvent, routing.IncludeState](&routing.StateIncludeIdle{})

	bootstrap := new(NullSM[routing.BootstrapEvent, routing.BootstrapState])
	probe := new(NullSM[routing.ProbeEvent, routing.ProbeState])

	routingBehaviour := NewRoutingBehaviour(self, bootstrap, include, probe, slog.Default(), otel.Tracer("test"))

	ev := &EventGetCloserNodesSuccess{
		QueryID:     query.QueryID("include"),
		To:          nodes[1].NodeInfo,
		Target:      kadt.PeerID(nodes[0].NodeInfo.ID).Key(),
		CloserNodes: []peer.AddrInfo{nodes[2].NodeInfo},
	}

	routingBehaviour.Notify(ctx, ev)

	// include should receive message response event
	require.IsType(t, &routing.EventIncludeMessageResponse[KadKey, ma.Multiaddr]{}, include.Received)

	rev := include.Received.(*routing.EventIncludeMessageResponse[KadKey, ma.Multiaddr])
	require.Equal(t, nodes[1].NodeInfo, NodeInfoToAddrInfo(rev.NodeInfo))
	require.Equal(t, ev.CloserNodes, SliceOfNodeInfoToSliceOfAddrInfo(rev.Response.CloserNodes()))
}

func TestRoutingIncludeGetClosestNodesFailure(t *testing.T) {
	ctx, cancel := kadtest.CtxShort(t)
	defer cancel()

	clk := clock.NewMock()
	_, nodes, err := nettest.LinearTopology(4, clk)
	require.NoError(t, err)

	self := nodes[0].NodeInfo.ID

	// records the event passed to include
	include := NewRecordingSM[routing.IncludeEvent, routing.IncludeState](&routing.StateIncludeIdle{})

	bootstrap := new(NullSM[routing.BootstrapEvent, routing.BootstrapState])
	probe := new(NullSM[routing.ProbeEvent, routing.ProbeState])

	routingBehaviour := NewRoutingBehaviour(self, bootstrap, include, probe, slog.Default(), otel.Tracer("test"))

	failure := errors.New("failed")
	ev := &EventGetCloserNodesFailure{
		QueryID: query.QueryID("include"),
		To:      nodes[1].NodeInfo,
		Target:  kadt.PeerID(nodes[0].NodeInfo.ID).Key(),
		Err:     failure,
	}

	routingBehaviour.Notify(ctx, ev)

	// include should receive message response event
	require.IsType(t, &routing.EventIncludeMessageFailure[KadKey, ma.Multiaddr]{}, include.Received)

	rev := include.Received.(*routing.EventIncludeMessageFailure[KadKey, ma.Multiaddr])
	require.Equal(t, nodes[1].NodeInfo, NodeInfoToAddrInfo(rev.NodeInfo))
	require.Equal(t, failure, rev.Error)
}

func TestRoutingIncludedNodeAddToProbeList(t *testing.T) {
	ctx, cancel := kadtest.CtxShort(t)
	defer cancel()

	clk := clock.NewMock()
	_, nodes, err := nettest.LinearTopology(4, clk)
	require.NoError(t, err)

	self := nodes[0].NodeInfo.ID
	rt := nodes[0].RoutingTable

	includeCfg := routing.DefaultIncludeConfig()
	includeCfg.Clock = clk
	include, err := routing.NewInclude[KadKey, ma.Multiaddr](rt, includeCfg)
	require.NoError(t, err)

	probeCfg := routing.DefaultProbeConfig()
	probeCfg.Clock = clk
	probeCfg.CheckInterval = 5 * time.Minute
	probe, err := routing.NewProbe[KadKey, ma.Multiaddr](rt, probeCfg)
	require.NoError(t, err)

	// ensure bootstrap is always idle
	bootstrap := NewRecordingSM[routing.BootstrapEvent, routing.BootstrapState](&routing.StateBootstrapIdle{})

	routingBehaviour := NewRoutingBehaviour(self, bootstrap, include, probe, slog.Default(), otel.Tracer("test"))

	// a new node to be included
	candidate := nodes[len(nodes)-1].NodeInfo

	// the routing table should not contain the node yet
	_, intable := rt.GetNode(kadt.PeerID(candidate.ID).Key())
	require.False(t, intable)

	// notify that there is a new node to be included
	routingBehaviour.Notify(ctx, &EventAddAddrInfo{
		NodeInfo: candidate,
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
		CloserNodes: []peer.AddrInfo{nodes[1].NodeInfo}, // must include one for include check to pass
	})

	// the routing table should now contain the node
	_, intable = rt.GetNode(kadt.PeerID(candidate.ID).Key())
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
	require.Equal(t, query.QueryID("probe"), oev.QueryID)
	require.Equal(t, candidate.ID, oev.To.ID)
}
