package coord

import (
	"context"
	"log"
	"testing"

	"github.com/benbjohnson/clock"
	"github.com/stretchr/testify/require"

	"github.com/libp2p/go-libp2p-kad-dht/v2/internal/coord/internal/nettest"
	"github.com/libp2p/go-libp2p-kad-dht/v2/internal/kadt"
	"github.com/libp2p/go-libp2p-kad-dht/v2/internal/kadtest"
	"github.com/libp2p/go-libp2p-kad-dht/v2/pb"
)

func TestConfigValidate(t *testing.T) {
	t.Run("default is valid", func(t *testing.T) {
		cfg := DefaultCoordinatorConfig()

		require.NoError(t, cfg.Validate())
	})

	t.Run("clock is not nil", func(t *testing.T) {
		cfg := DefaultCoordinatorConfig()

		cfg.Clock = nil
		require.Error(t, cfg.Validate())
	})

	t.Run("query concurrency positive", func(t *testing.T) {
		cfg := DefaultCoordinatorConfig()

		cfg.QueryConcurrency = 0
		require.Error(t, cfg.Validate())
		cfg.QueryConcurrency = -1
		require.Error(t, cfg.Validate())
	})

	t.Run("query timeout positive", func(t *testing.T) {
		cfg := DefaultCoordinatorConfig()

		cfg.QueryTimeout = 0
		require.Error(t, cfg.Validate())
		cfg.QueryTimeout = -1
		require.Error(t, cfg.Validate())
	})

	t.Run("request concurrency positive", func(t *testing.T) {
		cfg := DefaultCoordinatorConfig()

		cfg.RequestConcurrency = 0
		require.Error(t, cfg.Validate())
		cfg.RequestConcurrency = -1
		require.Error(t, cfg.Validate())
	})

	t.Run("request timeout positive", func(t *testing.T) {
		cfg := DefaultCoordinatorConfig()

		cfg.RequestTimeout = 0
		require.Error(t, cfg.Validate())
		cfg.RequestTimeout = -1
		require.Error(t, cfg.Validate())
	})

	t.Run("logger not nil", func(t *testing.T) {
		cfg := DefaultCoordinatorConfig()

		cfg.Logger = nil
		require.Error(t, cfg.Validate())
	})

	t.Run("meter provider not nil", func(t *testing.T) {
		cfg := DefaultCoordinatorConfig()
		cfg.MeterProvider = nil
		require.Error(t, cfg.Validate())
	})

	t.Run("tracer provider not nil", func(t *testing.T) {
		cfg := DefaultCoordinatorConfig()
		cfg.TracerProvider = nil
		require.Error(t, cfg.Validate())
	})
}

func TestExhaustiveQuery(t *testing.T) {
	ctx := kadtest.CtxShort(t)

	clk := clock.NewMock()
	_, nodes, err := nettest.LinearTopology(4, clk)
	require.NoError(t, err)
	ccfg := DefaultCoordinatorConfig()

	ccfg.Clock = clk

	// A (ids[0]) is looking for D (ids[3])
	// A will first ask B, B will reply with C's address (and A's address)
	// A will then ask C, C will reply with D's address (and B's address)
	self := kadt.PeerID(nodes[0].NodeID)
	c, err := NewCoordinator(self, nodes[0].Router, nodes[0].RoutingTable, ccfg)
	require.NoError(t, err)

	target := kadt.PeerID(nodes[3].NodeID).Key()

	visited := make(map[string]int)

	// Record the nodes as they are visited
	qfn := func(ctx context.Context, id kadt.PeerID, msg *pb.Message, stats QueryStats) error {
		visited[id.String()]++
		return nil
	}

	// Run a query to find the value
	_, _, err = c.QueryClosest(ctx, target, qfn, 20)
	require.NoError(t, err)

	require.Equal(t, 3, len(visited))
	require.Contains(t, visited, nodes[1].NodeID.String())
	require.Contains(t, visited, nodes[2].NodeID.String())
	require.Contains(t, visited, nodes[3].NodeID.String())
}

func TestRoutingUpdatedEventEmittedForCloserNodes(t *testing.T) {
	ctx := kadtest.CtxShort(t)

	clk := clock.NewMock()
	_, nodes, err := nettest.LinearTopology(4, clk)
	require.NoError(t, err)

	ccfg := DefaultCoordinatorConfig()

	ccfg.Clock = clk

	// A (ids[0]) is looking for D (ids[3])
	// A will first ask B, B will reply with C's address (and A's address)
	// A will then ask C, C will reply with D's address (and B's address)
	self := kadt.PeerID(nodes[0].NodeID)
	c, err := NewCoordinator(self, nodes[0].Router, nodes[0].RoutingTable, ccfg)
	if err != nil {
		log.Fatalf("unexpected error creating coordinator: %v", err)
	}

	rn := NewBufferedRoutingNotifier()
	c.SetRoutingNotifier(rn)

	qfn := func(ctx context.Context, id kadt.PeerID, msg *pb.Message, stats QueryStats) error {
		return nil
	}

	// Run a query to find the value
	target := nodes[3].NodeID.Key()
	_, _, err = c.QueryClosest(ctx, target, qfn, 20)
	require.NoError(t, err)

	// the query run by the dht should have received a response from nodes[1] with closer nodes
	// nodes[0] and nodes[2] which should trigger a routing table update since nodes[2] was
	// not in the dht's routing table.
	// the query then continues and should have received a response from nodes[2] with closer nodes
	// nodes[1] and nodes[3] which should trigger a routing table update since nodes[3] was
	// not in the dht's routing table.

	// no EventRoutingUpdated is sent for the self node

	// However the order in which these events are emitted may vary depending on timing.

	ev1, err := rn.Expect(ctx, &EventRoutingUpdated{})
	require.NoError(t, err)
	tev1 := ev1.(*EventRoutingUpdated)

	ev2, err := rn.Expect(ctx, &EventRoutingUpdated{})
	require.NoError(t, err)
	tev2 := ev2.(*EventRoutingUpdated)

	if tev1.NodeID.Equal(nodes[2].NodeID) {
		require.Equal(t, nodes[3].NodeID, tev2.NodeID)
	} else if tev2.NodeID.Equal(nodes[2].NodeID) {
		require.Equal(t, nodes[3].NodeID, tev1.NodeID)
	} else {
		require.Failf(t, "did not see routing updated event for %s", nodes[2].NodeID.String())
	}
}

func TestBootstrap(t *testing.T) {
	ctx := kadtest.CtxShort(t)

	clk := clock.NewMock()
	_, nodes, err := nettest.LinearTopology(4, clk)
	require.NoError(t, err)

	ccfg := DefaultCoordinatorConfig()

	ccfg.Clock = clk

	self := kadt.PeerID(nodes[0].NodeID)
	d, err := NewCoordinator(self, nodes[0].Router, nodes[0].RoutingTable, ccfg)
	require.NoError(t, err)

	rn := NewBufferedRoutingNotifier()
	d.SetRoutingNotifier(rn)

	seeds := []kadt.PeerID{nodes[1].NodeID}
	err = d.Bootstrap(ctx, seeds)
	require.NoError(t, err)

	// the query run by the dht should have completed
	ev, err := rn.Expect(ctx, &EventBootstrapFinished{})
	require.NoError(t, err)

	require.IsType(t, &EventBootstrapFinished{}, ev)
	tevf := ev.(*EventBootstrapFinished)
	require.Equal(t, 3, tevf.Stats.Requests)
	require.Equal(t, 3, tevf.Stats.Success)
	require.Equal(t, 0, tevf.Stats.Failure)

	_, err = rn.Expect(ctx, &EventRoutingUpdated{})
	require.NoError(t, err)

	_, err = rn.Expect(ctx, &EventRoutingUpdated{})
	require.NoError(t, err)

	// coordinator will have node1 in its routing table
	_, err = d.GetNode(ctx, nodes[1].NodeID)
	require.NoError(t, err)

	// coordinator should now have node2 in its routing table
	_, err = d.GetNode(ctx, nodes[2].NodeID)
	require.NoError(t, err)

	// coordinator should now have node3 in its routing table
	_, err = d.GetNode(ctx, nodes[3].NodeID)
	require.NoError(t, err)
}

func TestIncludeNode(t *testing.T) {
	ctx := kadtest.CtxShort(t)

	clk := clock.NewMock()
	_, nodes, err := nettest.LinearTopology(4, clk)
	require.NoError(t, err)

	ccfg := DefaultCoordinatorConfig()

	ccfg.Clock = clk

	candidate := nodes[len(nodes)-1].NodeID // not in nodes[0] routing table

	self := nodes[0].NodeID
	d, err := NewCoordinator(self, nodes[0].Router, nodes[0].RoutingTable, ccfg)
	if err != nil {
		log.Fatalf("unexpected error creating dht: %v", err)
	}
	rn := NewBufferedRoutingNotifier()
	d.SetRoutingNotifier(rn)

	// the routing table should not contain the node yet
	_, err = d.GetNode(ctx, candidate)
	require.ErrorIs(t, err, ErrNodeNotFound)

	// inject a new node
	err = d.AddNodes(ctx, []kadt.PeerID{candidate})
	require.NoError(t, err)

	// the include state machine runs in the background and eventually should add the node to routing table
	ev, err := rn.Expect(ctx, &EventRoutingUpdated{})
	require.NoError(t, err)

	tev := ev.(*EventRoutingUpdated)
	require.Equal(t, candidate, tev.NodeID)

	// the routing table should now contain the node
	_, err = d.GetNode(ctx, candidate)
	require.NoError(t, err)
}
