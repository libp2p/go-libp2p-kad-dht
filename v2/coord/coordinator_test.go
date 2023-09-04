package coord

import (
	"context"
	"fmt"
	"log"
	"reflect"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"

	"github.com/libp2p/go-libp2p-kad-dht/v2/coord/internal/kadtest"
	"github.com/libp2p/go-libp2p-kad-dht/v2/coord/internal/nettest"
	"github.com/libp2p/go-libp2p-kad-dht/v2/kadt"
)

const peerstoreTTL = 10 * time.Minute

// expectEventType selects on the event channel until an event of the expected type is sent.
func expectEventType(t *testing.T, ctx context.Context, events <-chan RoutingNotification, expected RoutingNotification) (RoutingNotification, error) {
	t.Helper()
	for {
		select {
		case ev := <-events:
			t.Logf("saw event: %T\n", ev)
			if reflect.TypeOf(ev) == reflect.TypeOf(expected) {
				return ev, nil
			}
		case <-ctx.Done():
			return nil, fmt.Errorf("test deadline exceeded while waiting for event %T", expected)
		}
	}
}

func TestConfigValidate(t *testing.T) {
	t.Run("default is valid", func(t *testing.T) {
		cfg := DefaultConfig()
		require.NoError(t, cfg.Validate())
	})

	t.Run("clock is not nil", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.Clock = nil
		require.Error(t, cfg.Validate())
	})

	t.Run("query concurrency positive", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.QueryConcurrency = 0
		require.Error(t, cfg.Validate())
		cfg.QueryConcurrency = -1
		require.Error(t, cfg.Validate())
	})

	t.Run("query timeout positive", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.QueryTimeout = 0
		require.Error(t, cfg.Validate())
		cfg.QueryTimeout = -1
		require.Error(t, cfg.Validate())
	})

	t.Run("request concurrency positive", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.RequestConcurrency = 0
		require.Error(t, cfg.Validate())
		cfg.QueryConcurrency = -1
		require.Error(t, cfg.Validate())
	})

	t.Run("request timeout positive", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.RequestTimeout = 0
		require.Error(t, cfg.Validate())
		cfg.RequestTimeout = -1
		require.Error(t, cfg.Validate())
	})

	t.Run("logger not nil", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.Logger = nil
		require.Error(t, cfg.Validate())
	})
}

func TestExhaustiveQuery(t *testing.T) {
	ctx, cancel := kadtest.CtxShort(t)
	defer cancel()

	clk := clock.NewMock()
	_, nodes, err := nettest.LinearTopology(4, clk)
	require.NoError(t, err)
	ccfg := DefaultConfig()
	ccfg.Clock = clk
	ccfg.PeerstoreTTL = peerstoreTTL

	// A (ids[0]) is looking for D (ids[3])
	// A will first ask B, B will reply with C's address (and A's address)
	// A will then ask C, C will reply with D's address (and B's address)
	self := nodes[0].NodeInfo.ID
	c, err := NewCoordinator(self, nodes[0].Router, nodes[0].RoutingTable, ccfg)
	require.NoError(t, err)

	target := kadt.PeerID(nodes[3].NodeInfo.ID).Key()

	visited := make(map[string]int)

	// Record the nodes as they are visited
	qfn := func(ctx context.Context, node Node, stats QueryStats) error {
		visited[node.ID().String()]++
		return nil
	}

	// Run a query to find the value
	_, err = c.Query(ctx, target, qfn)
	require.NoError(t, err)

	require.Equal(t, 3, len(visited))
	require.Contains(t, visited, nodes[1].NodeInfo.ID.String())
	require.Contains(t, visited, nodes[2].NodeInfo.ID.String())
	require.Contains(t, visited, nodes[3].NodeInfo.ID.String())
}

func TestRoutingUpdatedEventEmittedForCloserNodes(t *testing.T) {
	ctx, cancel := kadtest.CtxShort(t)
	defer cancel()

	clk := clock.NewMock()
	_, nodes, err := nettest.LinearTopology(4, clk)
	require.NoError(t, err)

	ccfg := DefaultConfig()
	ccfg.Clock = clk
	ccfg.PeerstoreTTL = peerstoreTTL

	// A (ids[0]) is looking for D (ids[3])
	// A will first ask B, B will reply with C's address (and A's address)
	// A will then ask C, C will reply with D's address (and B's address)
	self := nodes[0].NodeInfo.ID
	c, err := NewCoordinator(self, nodes[0].Router, nodes[0].RoutingTable, ccfg)
	if err != nil {
		log.Fatalf("unexpected error creating dht: %v", err)
	}

	buffer := make(chan RoutingNotification, 5)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case ev := <-c.RoutingNotifications():
				buffer <- ev
			}
		}
	}()

	qfn := func(ctx context.Context, node Node, stats QueryStats) error {
		return nil
	}

	// Run a query to find the value
	target := kadt.PeerID(nodes[3].NodeInfo.ID).Key()
	_, err = c.Query(ctx, target, qfn)
	require.NoError(t, err)

	// the query run by the dht should have received a response from nodes[1] with closer nodes
	// nodes[0] and nodes[2] which should trigger a routing table update since nodes[2] was
	// not in the dht's routing table.
	ev, err := expectEventType(t, ctx, buffer, &EventRoutingUpdated{})
	require.NoError(t, err)

	tev := ev.(*EventRoutingUpdated)
	require.Equal(t, nodes[2].NodeInfo.ID, NodeIDToPeerID(tev.NodeInfo.ID()))

	// no EventRoutingUpdated is sent for the self node

	// the query continues and should have received a response from nodes[2] with closer nodes
	// nodes[1] and nodes[3] which should trigger a routing table update since nodes[3] was
	// not in the dht's routing table.
	ev, err = expectEventType(t, ctx, buffer, &EventRoutingUpdated{})
	require.NoError(t, err)

	tev = ev.(*EventRoutingUpdated)
	require.Equal(t, nodes[3].NodeInfo.ID, NodeIDToPeerID(tev.NodeInfo.ID()))
}

func TestBootstrap(t *testing.T) {
	ctx, cancel := kadtest.CtxShort(t)
	defer cancel()

	clk := clock.NewMock()
	_, nodes, err := nettest.LinearTopology(4, clk)
	require.NoError(t, err)

	ccfg := DefaultConfig()
	ccfg.Clock = clk
	ccfg.PeerstoreTTL = peerstoreTTL

	self := nodes[0].NodeInfo.ID
	d, err := NewCoordinator(self, nodes[0].Router, nodes[0].RoutingTable, ccfg)
	if err != nil {
		log.Fatalf("unexpected error creating dht: %v", err)
	}

	buffer := make(chan RoutingNotification, 5)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case ev := <-d.RoutingNotifications():
				buffer <- ev
			}
		}
	}()

	seeds := []peer.ID{
		nodes[1].NodeInfo.ID,
	}
	err = d.Bootstrap(ctx, seeds)
	require.NoError(t, err)

	// the query run by the dht should have completed
	ev, err := expectEventType(t, ctx, buffer, &EventBootstrapFinished{})
	require.NoError(t, err)

	require.IsType(t, &EventBootstrapFinished{}, ev)
	tevf := ev.(*EventBootstrapFinished)
	require.Equal(t, 3, tevf.Stats.Requests)
	require.Equal(t, 3, tevf.Stats.Success)
	require.Equal(t, 0, tevf.Stats.Failure)

	// DHT should now have node1 in its routing table
	_, err = d.GetNode(ctx, nodes[1].NodeInfo.ID)
	require.NoError(t, err)

	// DHT should now have node2 in its routing table
	_, err = d.GetNode(ctx, nodes[2].NodeInfo.ID)
	require.NoError(t, err)

	// DHT should now have node3 in its routing table
	_, err = d.GetNode(ctx, nodes[3].NodeInfo.ID)
	require.NoError(t, err)
}

func TestIncludeNode(t *testing.T) {
	ctx, cancel := kadtest.CtxShort(t)
	defer cancel()

	clk := clock.NewMock()
	_, nodes, err := nettest.LinearTopology(4, clk)
	require.NoError(t, err)

	ccfg := DefaultConfig()
	ccfg.Clock = clk
	ccfg.PeerstoreTTL = peerstoreTTL

	candidate := nodes[len(nodes)-1].NodeInfo // not in nodes[0] routing table

	self := nodes[0].NodeInfo.ID
	d, err := NewCoordinator(self, nodes[0].Router, nodes[0].RoutingTable, ccfg)
	if err != nil {
		log.Fatalf("unexpected error creating dht: %v", err)
	}

	// the routing table should not contain the node yet
	_, err = d.GetNode(ctx, candidate.ID)
	require.ErrorIs(t, err, ErrNodeNotFound)

	events := d.RoutingNotifications()

	// inject a new node into the dht's includeEvents queue
	err = d.AddNodes(ctx, []peer.AddrInfo{candidate})
	require.NoError(t, err)

	// the include state machine runs in the background and eventually should add the node to routing table
	ev, err := expectEventType(t, ctx, events, &EventRoutingUpdated{})
	require.NoError(t, err)

	tev := ev.(*EventRoutingUpdated)
	require.Equal(t, candidate.ID, NodeIDToPeerID(tev.NodeInfo.ID()))

	// the routing table should not contain the node yet
	_, err = d.GetNode(ctx, candidate.ID)
	require.NoError(t, err)
}
