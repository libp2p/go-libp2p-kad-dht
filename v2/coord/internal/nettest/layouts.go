package nettest

import (
	"context"
	"fmt"

	"github.com/libp2p/go-libp2p-kad-dht/v2/coord"

	"github.com/benbjohnson/clock"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/plprobelab/go-kademlia/kad"
	"github.com/plprobelab/go-kademlia/routing/simplert"
)

// LinearTopology creates a network topology consisting of n nodes peered in a linear chain.
// The nodes are configured with routing tables that contain immediate neighbours.
// It returns the topology and the nodes ordered such that nodes[x] has nodes[x-1] and nodes[x+1] in its routing table
// The topology is not a ring: nodes[0] only has nodes[1] in its table and nodes[n-1] only has nodes[n-2] in its table.
// nodes[1] has nodes[0] and nodes[2] in its routing table.
// If n > 2 then the first and last nodes will not have one another in their routing tables.
func LinearTopology(n int, clk clock.Clock) (*Topology, []*Node, error) {
	nodes := make([]*Node, n)

	top := NewTopology(clk)
	for i := range nodes {

		a, err := ma.NewMultiaddr(fmt.Sprintf("/ip4/127.0.0.1/tcp/%d", 2000+i))
		if err != nil {
			return nil, nil, err
		}

		ai, err := NewAddrInfo([]ma.Multiaddr{a})
		if err != nil {
			return nil, nil, err
		}

		nodes[i] = &Node{
			NodeInfo:     ai,
			Router:       NewRouter(ai.ID, top),
			RoutingTable: simplert.New[coord.Key, kad.NodeID[coord.Key]](coord.PeerID(ai.ID), 20),
		}
	}

	// Define the network topology, with default network links between every node
	for i := 0; i < len(nodes); i++ {
		for j := i + 1; j < len(nodes); j++ {
			top.ConnectNodes(nodes[i], nodes[j])
		}
	}

	// Connect nodes in a chain
	for i := 0; i < len(nodes); i++ {
		if i > 0 {
			nodes[i].Router.AddNodeInfo(context.Background(), nodes[i-1].NodeInfo, 0)
			nodes[i].RoutingTable.AddNode(coord.PeerID(nodes[i-1].NodeInfo.ID))
		}
		if i < len(nodes)-1 {
			nodes[i].Router.AddNodeInfo(context.Background(), nodes[i+1].NodeInfo, 0)
			nodes[i].RoutingTable.AddNode(coord.PeerID(nodes[i+1].NodeInfo.ID))
		}
	}

	return top, nodes, nil
}
