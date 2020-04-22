package rtrefresh

import (
	"testing"

	"github.com/libp2p/go-libp2p-core/peer"

	"github.com/stretchr/testify/require"
)

func TestRefreshDiscoverNewPeers(t *testing.T) {
	// when cpl is full, we always get true
	p1 := []peer.ID{"a"}
	p2 := []peer.ID{"a"}

	require.True(t, refreshDiscoverNewPeers(true, p1, p2))
	require.False(t, refreshDiscoverNewPeers(false, p1, p1))
	require.False(t, refreshDiscoverNewPeers(false, nil, nil))
	require.True(t, refreshDiscoverNewPeers(false, p1, append(p1, peer.ID("x"))))
	require.True(t, refreshDiscoverNewPeers(false, []peer.ID{"x"}, []peer.ID{"y"}))
}
