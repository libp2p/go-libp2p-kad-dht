package kademlia

import (
	"testing"

	"github.com/benbjohnson/clock"
	"github.com/plprobelab/go-kademlia/key"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slog"

	"github.com/libp2p/go-libp2p-kad-dht/v2/coord/internal/kadtest"
	"github.com/libp2p/go-libp2p-kad-dht/v2/coord/internal/nettest"
)

var _ Router[key.Key8, kadtest.StrAddr] = (*nettest.Router[key.Key8, kadtest.StrAddr])(nil)

// TODO: this is just a basic is-it-working test that needs to be improved
func TestGetClosestNodes(t *testing.T) {
	ctx, cancel := kadtest.CtxShort(t)
	defer cancel()

	clk := clock.NewMock()
	_, nodes := nettest.LinearTopology(4, clk)

	h := NewNodeHandler[key.Key8, kadtest.StrAddr](nodes[1].NodeInfo, nodes[1].Router, slog.Default())

	// node 1 has node 2 in its routing table so it will return it along with node 0
	found, err := h.GetClosestNodes(ctx, nodes[2].NodeInfo.ID().Key(), 2)
	require.NoError(t, err)
	for _, f := range found {
		t.Logf("found node %v", f.ID())
	}
	require.Equal(t, 2, len(found))
}
