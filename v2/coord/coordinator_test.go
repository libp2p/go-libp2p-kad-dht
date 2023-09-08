package coord

import (
	"context"
	"fmt"
	"log"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"

	"github.com/libp2p/go-libp2p-kad-dht/v2/coord/internal/nettest"
	"github.com/libp2p/go-libp2p-kad-dht/v2/internal/kadtest"
	"github.com/libp2p/go-libp2p-kad-dht/v2/kadt"
	"github.com/libp2p/go-libp2p-kad-dht/v2/tele"
)

const peerstoreTTL = 10 * time.Minute

type notificationWatcher struct {
	mu       sync.Mutex
	buffered []RoutingNotification
	signal   chan struct{}
}

func (w *notificationWatcher) Watch(t *testing.T, ctx context.Context, ch <-chan RoutingNotification) {
	t.Helper()
	w.signal = make(chan struct{}, 1)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case ev := <-ch:
				w.mu.Lock()
				t.Logf("buffered routing notification: %T\n", ev)
				w.buffered = append(w.buffered, ev)
				select {
				case w.signal <- struct{}{}:
				default:
				}
				w.mu.Unlock()

			}
		}
	}()
}

func (w *notificationWatcher) Expect(ctx context.Context, expected RoutingNotification) (RoutingNotification, error) {
	for {
		// look in buffered events
		w.mu.Lock()
		for i, ev := range w.buffered {
			if reflect.TypeOf(ev) == reflect.TypeOf(expected) {
				// remove first from buffer and return it
				w.buffered = w.buffered[:i+copy(w.buffered[i:], w.buffered[i+1:])]
				w.mu.Unlock()
				return ev, nil
			}
		}
		w.mu.Unlock()

		// wait to be signaled that there is a new event
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("test deadline exceeded while waiting for event %T", expected)
		case <-w.signal:
		}
	}
}

// TracingTelemetry may be used to create a Telemetry that traces a test
func TracingTelemetry(t *testing.T) *tele.Telemetry {
	telemetry, err := tele.New(otel.GetMeterProvider(), kadtest.JaegerTracerProvider(t))
	if err != nil {
		t.Fatalf("unexpected error creating telemetry: %v", err)
	}

	return telemetry
}

func TestConfigValidate(t *testing.T) {
	t.Run("default is valid", func(t *testing.T) {
		cfg, err := DefaultCoordinatorConfig()
		require.NoError(t, err)

		require.NoError(t, cfg.Validate())
	})

	t.Run("clock is not nil", func(t *testing.T) {
		cfg, err := DefaultCoordinatorConfig()
		require.NoError(t, err)

		cfg.Clock = nil
		require.Error(t, cfg.Validate())
	})

	t.Run("query concurrency positive", func(t *testing.T) {
		cfg, err := DefaultCoordinatorConfig()
		require.NoError(t, err)

		cfg.QueryConcurrency = 0
		require.Error(t, cfg.Validate())
		cfg.QueryConcurrency = -1
		require.Error(t, cfg.Validate())
	})

	t.Run("query timeout positive", func(t *testing.T) {
		cfg, err := DefaultCoordinatorConfig()
		require.NoError(t, err)

		cfg.QueryTimeout = 0
		require.Error(t, cfg.Validate())
		cfg.QueryTimeout = -1
		require.Error(t, cfg.Validate())
	})

	t.Run("request concurrency positive", func(t *testing.T) {
		cfg, err := DefaultCoordinatorConfig()
		require.NoError(t, err)

		cfg.RequestConcurrency = 0
		require.Error(t, cfg.Validate())
		cfg.QueryConcurrency = -1
		require.Error(t, cfg.Validate())
	})

	t.Run("request timeout positive", func(t *testing.T) {
		cfg, err := DefaultCoordinatorConfig()
		require.NoError(t, err)

		cfg.RequestTimeout = 0
		require.Error(t, cfg.Validate())
		cfg.RequestTimeout = -1
		require.Error(t, cfg.Validate())
	})

	t.Run("logger not nil", func(t *testing.T) {
		cfg, err := DefaultCoordinatorConfig()
		require.NoError(t, err)

		cfg.Logger = nil
		require.Error(t, cfg.Validate())
	})

	t.Run("telemetry not nil", func(t *testing.T) {
		cfg, err := DefaultCoordinatorConfig()
		require.NoError(t, err)

		cfg.Tele = nil
		require.Error(t, cfg.Validate())
	})
}

func TestExhaustiveQuery(t *testing.T) {
	ctx := kadtest.CtxShort(t)

	clk := clock.NewMock()
	_, nodes, err := nettest.LinearTopology(4, clk)
	require.NoError(t, err)
	ccfg, err := DefaultCoordinatorConfig()
	require.NoError(t, err)

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
	ctx := kadtest.CtxShort(t)

	clk := clock.NewMock()
	_, nodes, err := nettest.LinearTopology(4, clk)
	require.NoError(t, err)

	ccfg, err := DefaultCoordinatorConfig()
	require.NoError(t, err)

	ccfg.Clock = clk
	ccfg.PeerstoreTTL = peerstoreTTL

	// A (ids[0]) is looking for D (ids[3])
	// A will first ask B, B will reply with C's address (and A's address)
	// A will then ask C, C will reply with D's address (and B's address)
	self := nodes[0].NodeInfo.ID
	c, err := NewCoordinator(self, nodes[0].Router, nodes[0].RoutingTable, ccfg)
	if err != nil {
		log.Fatalf("unexpected error creating coordinator: %v", err)
	}

	w := new(notificationWatcher)
	w.Watch(t, ctx, c.RoutingNotifications())

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
	// the query then continues and should have received a response from nodes[2] with closer nodes
	// nodes[1] and nodes[3] which should trigger a routing table update since nodes[3] was
	// not in the dht's routing table.

	// no EventRoutingUpdated is sent for the self node

	// However the order in which these events are emitted may vary depending on timing.

	ev1, err := w.Expect(ctx, &EventRoutingUpdated{})
	require.NoError(t, err)
	tev1 := ev1.(*EventRoutingUpdated)

	ev2, err := w.Expect(ctx, &EventRoutingUpdated{})
	require.NoError(t, err)
	tev2 := ev2.(*EventRoutingUpdated)

	if tev1.NodeInfo.ID == nodes[2].NodeInfo.ID {
		require.Equal(t, nodes[3].NodeInfo.ID, tev2.NodeInfo.ID)
	} else if tev2.NodeInfo.ID == nodes[2].NodeInfo.ID {
		require.Equal(t, nodes[3].NodeInfo.ID, tev1.NodeInfo.ID)
	} else {
		require.Failf(t, "did not see routing updated event for %s", nodes[2].NodeInfo.ID.String())
	}
}

func TestBootstrap(t *testing.T) {
	ctx := kadtest.CtxShort(t)

	clk := clock.NewMock()
	_, nodes, err := nettest.LinearTopology(4, clk)
	require.NoError(t, err)

	ccfg, err := DefaultCoordinatorConfig()
	require.NoError(t, err)

	ccfg.Clock = clk
	ccfg.PeerstoreTTL = peerstoreTTL

	self := nodes[0].NodeInfo.ID
	d, err := NewCoordinator(self, nodes[0].Router, nodes[0].RoutingTable, ccfg)
	require.NoError(t, err)

	w := new(notificationWatcher)
	w.Watch(t, ctx, d.RoutingNotifications())

	seeds := []peer.ID{nodes[1].NodeInfo.ID}
	err = d.Bootstrap(ctx, seeds)
	require.NoError(t, err)

	// the query run by the dht should have completed
	ev, err := w.Expect(ctx, &EventBootstrapFinished{})
	require.NoError(t, err)

	require.IsType(t, &EventBootstrapFinished{}, ev)
	tevf := ev.(*EventBootstrapFinished)
	require.Equal(t, 3, tevf.Stats.Requests)
	require.Equal(t, 3, tevf.Stats.Success)
	require.Equal(t, 0, tevf.Stats.Failure)

	_, err = w.Expect(ctx, &EventRoutingUpdated{})
	require.NoError(t, err)

	_, err = w.Expect(ctx, &EventRoutingUpdated{})
	require.NoError(t, err)

	// coordinator will have node1 in its routing table
	_, err = d.GetNode(ctx, nodes[1].NodeInfo.ID)
	require.NoError(t, err)

	// coordinator should now have node2 in its routing table
	_, err = d.GetNode(ctx, nodes[2].NodeInfo.ID)
	require.NoError(t, err)

	// coordinator should now have node3 in its routing table
	_, err = d.GetNode(ctx, nodes[3].NodeInfo.ID)
	require.NoError(t, err)
}

func TestIncludeNode(t *testing.T) {
	ctx := kadtest.CtxShort(t)

	clk := clock.NewMock()
	_, nodes, err := nettest.LinearTopology(4, clk)
	require.NoError(t, err)

	ccfg, err := DefaultCoordinatorConfig()
	require.NoError(t, err)

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

	w := new(notificationWatcher)
	w.Watch(t, ctx, d.RoutingNotifications())

	// inject a new node into the dht's includeEvents queue
	err = d.AddNodes(ctx, []peer.AddrInfo{candidate}, time.Minute)
	require.NoError(t, err)

	// the include state machine runs in the background and eventually should add the node to routing table
	ev, err := w.Expect(ctx, &EventRoutingUpdated{})
	require.NoError(t, err)

	tev := ev.(*EventRoutingUpdated)
	require.Equal(t, candidate.ID, tev.NodeInfo.ID)

	// the routing table should now contain the node
	_, err = d.GetNode(ctx, candidate.ID)
	require.NoError(t, err)
}
