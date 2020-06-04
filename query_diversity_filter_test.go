package dht

import (
	"context"
	"github.com/libp2p/go-libp2p-core/peer"
	ma "github.com/multiformats/go-multiaddr"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p-kbucket/peerdiversity"
	swarmt "github.com/libp2p/go-libp2p-swarm/testing"
	bhost "github.com/libp2p/go-libp2p/p2p/host/basic"

	"github.com/stretchr/testify/require"
)

func TestQueryDiversityFilter(t *testing.T) {
	ctx := context.Background()
	h := bhost.New(swarmt.GenSwarm(t, ctx, swarmt.OptDisableReuseport))
	r := NewQueryDiversityFilter(h, 2)

	// table should only have 2 for each prefix
	key := "key"
	g := peerdiversity.PeerGroupInfo{Cpl: 1, IPGroupKey: peerdiversity.PeerIPGroupKey(key)}
	require.True(t, r.Allow(g))
	r.Increment(g)
	g.Cpl = 2
	require.True(t, r.Allow(g))
	r.Increment(g)
	g.Cpl = 3
	require.False(t, r.Allow(g))

	// remove a group and it should work
	r.Decrement(g)
	require.True(t, r.Allow(g))
}

func TestNewQueryDiversityFilterAddresses(t *testing.T) {
	ctx := context.Background()
	h := bhost.New(swarmt.GenSwarm(t, ctx, swarmt.OptDisableReuseport))
	r := NewQueryDiversityFilter(h, 2)

	require.Empty(t, r.PeerAddresses(peer.ID("rand")))

	h.Peerstore().AddAddrs(peer.ID("rand"),
		[]ma.Multiaddr{ma.StringCast("/ip4/155.11.11.1/tcp/0"), ma.StringCast("/ip4/45.11.11.1/tcp/0"),
			ma.StringCast("/ip4/192.168.2.2/tcp/0")}, 1*time.Hour)

	addrs := r.PeerAddresses(peer.ID("rand"))
	require.Len(t, addrs, 2)
	require.Contains(t, addrs, ma.StringCast("/ip4/155.11.11.1/tcp/0"))
	require.Contains(t, addrs, ma.StringCast("/ip4/45.11.11.1/tcp/0"))
}
