package dht

import (
	"context"
	"testing"
	"time"

	"github.com/plprobelab/go-kademlia/kad"
	"github.com/plprobelab/go-kademlia/key"

	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/require"
)

func TestRouter(t *testing.T) {
	d := newTestDHT(t)
	ctx := context.Background()

	// friend is the first peer we know in the IPFS DHT network (bootstrap node)
	friendID, err := peer.Decode("12D3KooWGjgvfDkpuVAoNhd7PRRvMTEG4ZgzHBFURqDe1mqEzAMS")
	require.NoError(t, err)

	// multiaddress of friend
	friendAddr, err := multiaddr.NewMultiaddr("/ip4/45.32.75.236/tcp/4001")
	require.NoError(t, err)

	t.Log("connecting...")
	friendInfo := peer.AddrInfo{ID: friendID, Addrs: []multiaddr.Multiaddr{friendAddr}}
	err = d.host.Connect(ctx, friendInfo)
	require.NoError(t, err)
	t.Log("connected")

	// target is the peer we want to find
	target, err := peer.Decode("12D3KooWGWcyxn3JfihYiu2HspbE5XHzfgZiLwihVCeyXQQU8yC1")
	require.NoError(t, err)

	// Error -> delay between AddNodes and added to routing table
	err = d.kad.AddNodes(ctx, []kad.NodeInfo[key.Key256, multiaddr.Multiaddr]{
		nodeInfo{info: friendInfo},
	})
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)

	d.rt.AddNode(nodeID(friendInfo.ID))

	targetInfo, err := d.FindPeer(ctx, target)
	require.NoError(t, err)
	t.Log(targetInfo.ID)
	t.Log(targetInfo.Addrs)
}
