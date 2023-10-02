package dht

import (
	"context"
	"testing"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-kad-dht/v2/kadt"
	"github.com/libp2p/go-libp2p-kbucket/peerdiversity"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/plprobelab/go-libdht/kad/triert"
	"github.com/stretchr/testify/require"
)

type peerInfo struct {
	ID    string
	Addrs []string
}

var inputPeers []peerInfo = []peerInfo{ // first bits of Kad Key, bucket index wrt. 1EoooPEER1
	{"1EoooPEER1", []string{"/ip4/0.0.0.0/tcp/4001"}},     // 0100 1101
	{"1EoooPEER2", []string{"/ip4/1.1.1.1/tcp/4001"}},     // 1110 -> b0
	{"1EoooPEER3", []string{"/ip4/1.1.64.64/tcp/4001"}},   // 0110 -> b2
	{"1EoooPEER4", []string{"/ip4/1.1.128.128/tcp/4001"}}, // 0110 -> b2
	{"1EoooPEER5", []string{"/ip4/1.1.192.192/tcp/4001"}}, // 0100 1110 -> b6
	{"1EoooPEER6", []string{"/ip4/1.1.2.2/tcp/4001"}},     // 1010 -> b0
	{"1EoooPEER7", []string{"/ip4/1.2.1.1/tcp/4001"}},     // 0000 -> b1
	{"1EoooPEER8", []string{"/ip4/1.1.255.255/tcp/4001"}}, // 0111 -> b2
	{"1EoooPEER9", []string{}},                            // 1111 -> b0

	{"1EooPEER11", []string{"/ip6/2000:1234::/tcp/4001", "/ip4/1.1.3.3/tcp/4001"}},  // 1100 -> b0
	{"1EooPEER12", []string{"/ip6/2000:1234::1/tcp/4001"}},                          // 0001 -> b1
	{"1EooPEER13", []string{"/ip6/2000:1234:5678::/tcp/4001"}},                      // 0110 -> b2
	{"1EooPEER14", []string{"/ip6/2000:1234::2/tcp/4001", "/ip4/3.3.3.3/tcp/4001"}}, // 0000 -> b1
	{"1EooPEER15", []string{"/ip6/2000:1234:5678::1/tcp/4001"}},                     // 0000 -> b1
}

var peers map[string]peer.AddrInfo = make(map[string]peer.AddrInfo)

func init() {
	for _, p := range inputPeers {
		pid, err := peer.Decode(p.ID)
		if err != nil {
			panic(err)
		}
		addrs := make([]ma.Multiaddr, 0, len(p.Addrs))
		for _, a := range p.Addrs {
			addr, err := ma.NewMultiaddr(a)
			if err != nil {
				panic(err)
			}
			addrs = append(addrs, addr)
		}
		peers[p.ID] = peer.AddrInfo{ID: pid, Addrs: addrs}
	}
}

func TestRtPeerIPGroupFilter(t *testing.T) {
	gf := newRtPeerIPGroupFilter(2, 3, func(p peer.ID) []ma.Multiaddr {
		for _, pi := range peers {
			if pi.ID == p {
				return pi.Addrs
			}
		}
		return nil
	})

	filter, err := peerdiversity.NewFilter(gf, "triert/diversity",
		func(p peer.ID) int {
			return kadt.PeerID(peers["1EoooPEER1"].ID).Key().CommonPrefixLength(
				kadt.PeerID(p).Key())
		})
	require.NoError(t, err)

	// generate routing table using 1EoooPEER1 as the local peer, and the
	// diversity filter
	rtcfg := &triert.Config[kadt.Key, kadt.PeerID]{
		NodeFilter: &TrieRTPeerDiversityFilter{Filter: filter},
	}
	rt, err := triert.New[kadt.Key, kadt.PeerID](
		kadt.PeerID(peers["1EoooPEER1"].ID), rtcfg)
	require.NoError(t, err)

	// add 3 peers with the same IP group (1.1.0.0/16)
	success := rt.AddNode(kadt.PeerID(peers["1EoooPEER2"].ID))
	require.True(t, success)
	success = rt.AddNode(kadt.PeerID(peers["1EoooPEER3"].ID))
	require.True(t, success)
	success = rt.AddNode(kadt.PeerID(peers["1EoooPEER4"].ID))
	require.True(t, success)

	// add another peer with the same IP group (1.1.0.0/16) will fail
	// (maxForTable = 3)
	success = rt.AddNode(kadt.PeerID(peers["1EoooPEER5"].ID))
	require.False(t, success)

	// remove 1EoooPEER2 from the routing table
	success = rt.RemoveKey(kadt.PeerID(peers["1EoooPEER2"].ID).Key())
	require.True(t, success)

	// adding 1EoooPEER8 will fail, because it falls in the same bucket (2) as
	// 1EoooPEER3 and 1EoooPEER4 and it has the same IP group (maxPerCpl = 2)
	success = rt.AddNode(kadt.PeerID(peers["1EoooPEER8"].ID))
	require.False(t, success)

	// adding 1EoooPEER6 will succeed, because it belongs to bucket 0, which has
	// no other peers using the same IP group
	success = rt.AddNode(kadt.PeerID(peers["1EoooPEER6"].ID))
	require.True(t, success)

	// adding 1EoooPEER7 will succeed, because it doesn't share the same IP
	// group with any other peer in the routing table
	success = rt.AddNode(kadt.PeerID(peers["1EoooPEER7"].ID))
	require.True(t, success)
	// removing the last peer from an IP group
	success = rt.RemoveKey(kadt.PeerID(peers["1EoooPEER7"].ID).Key())
	require.True(t, success)

	// adding 1EoooPEER9 will fail, because it doesn't have a valid multiaddr
	success = rt.AddNode(kadt.PeerID(peers["1EoooPEER9"].ID))
	require.False(t, success)

	// adding 1EooPEER11 will fail, because out of its 2 multiaddrs, one belongs
	// to an IP group that already has 3 peers in the routing table
	success = rt.AddNode(kadt.PeerID(peers["1EooPEER11"].ID))
	require.False(t, success)

	// adding 1EooPEER12 will succeed, because it is the first peer in its
	// ip6 group
	success = rt.AddNode(kadt.PeerID(peers["1EooPEER12"].ID))
	require.True(t, success)

	// adding 1EooPEER14 will succeed because both its multiaddrs belong to non
	// full ip groups
	success = rt.AddNode(kadt.PeerID(peers["1EooPEER14"].ID))
	require.True(t, success)

	// adding 1EooPEER15 will fail because its ip6 group is full for cpl = 1
	success = rt.AddNode(kadt.PeerID(peers["1EooPEER15"].ID))
	require.False(t, success)

	// adding 1EooPEER13 will succeed, because even tough it shares the same
	// ip6 group with 1EooPEER15, it has a different cpl
	success = rt.AddNode(kadt.PeerID(peers["1EooPEER13"].ID))
	require.True(t, success)
}

func TestRTPeerDiversityFilter(t *testing.T) {
	ctx := context.Background()
	h, err := libp2p.New()
	require.NoError(t, err)

	// create 2 remote peers
	h1, err := libp2p.New()
	require.NoError(t, err)
	h2, err := libp2p.New()
	require.NoError(t, err)

	// connect h to h1 and h2
	err = h.Connect(ctx, peer.AddrInfo{ID: h1.ID(), Addrs: h1.Addrs()})
	require.NoError(t, err)
	err = h.Connect(ctx, peer.AddrInfo{ID: h2.ID(), Addrs: h2.Addrs()})
	require.NoError(t, err)

	// create peer filter and routing table
	peerFilter, err := NewRTPeerDiversityFilter(h, 1, 1)
	require.NoError(t, err)
	rtcfg := &triert.Config[kadt.Key, kadt.PeerID]{
		NodeFilter: peerFilter,
	}
	rt, err := triert.New[kadt.Key, kadt.PeerID](kadt.PeerID(h.ID()), rtcfg)
	require.NoError(t, err)

	// try to add h1 to the routing table. succeeds because it is the first peer
	success := rt.AddNode(kadt.PeerID(h1.ID()))
	require.True(t, success)

	// try to add h2 to the routing table. fails because it is the second peer
	success = rt.AddNode(kadt.PeerID(h2.ID()))
	require.False(t, success)
}
