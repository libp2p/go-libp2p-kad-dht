package coord

import (
	"testing"

	"github.com/libp2p/go-libp2p-kad-dht/v2/kadt"

	"github.com/benbjohnson/clock"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/exp/slog"

	"github.com/libp2p/go-libp2p-kad-dht/v2/coord/internal/nettest"
	"github.com/libp2p/go-libp2p-kad-dht/v2/internal/kadtest"
)

// TODO: this is just a basic is-it-working test that needs to be improved
func TestGetClosestNodes(t *testing.T) {
	ctx, cancel := kadtest.CtxShort(t)
	defer cancel()

	clk := clock.NewMock()
	_, nodes, err := nettest.LinearTopology(4, clk)
	require.NoError(t, err)

	h := NewNodeHandler(kadt.PeerID(nodes[1].NodeInfo.ID), nodes[1].Router, slog.Default(), trace.NewNoopTracerProvider().Tracer(""))

	// node 1 has node 2 in its routing table so it will return it along with node 0
	found, err := h.GetClosestNodes(ctx, kadt.PeerID(nodes[2].NodeInfo.ID).Key(), 2)
	require.NoError(t, err)
	for _, f := range found {
		t.Logf("found node %v", f.ID())
	}
	require.Equal(t, 2, len(found))
}
