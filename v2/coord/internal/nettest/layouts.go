package nettest

import (
	"context"

	"github.com/benbjohnson/clock"

	"github.com/plprobelab/go-kademlia/kad"
	"github.com/plprobelab/go-kademlia/key"
	"github.com/plprobelab/go-kademlia/routing/simplert"

	"github.com/libp2p/go-libp2p-kad-dht/v2/coord/internal/kadtest"
)

// LinearTopology creates a network topology consisting of n nodes peered in a linear chain.
// The nodes are configured with routing tables that contain immediate neighbours.
// It returns the topology and the nodes ordered such that nodes[x] has nodes[x-1] and nodes[x+1] in its routing table
// The topology is not a ring: nodes[0] only has nodes[1] in its table and nodes[n-1] only has nodes[n-2] in its table.
// nodes[1] has nodes[0] and nodes[2] in its routing table.
// If n > 2 then the first and last nodes will not have one another in their routing tables.
func LinearTopology(n int, clk *clock.Mock) (*Topology[key.Key8, kadtest.StrAddr], []*Node[key.Key8, kadtest.StrAddr]) {
	nodes := make([]*Node[key.Key8, kadtest.StrAddr], n)

	top := NewTopology[key.Key8, kadtest.StrAddr](clk)

	for i := range nodes {
		id := kadtest.NewID(key.Key8(byte(i)))
		nodes[i] = &Node[key.Key8, kadtest.StrAddr]{
			NodeInfo:     kadtest.NewInfo(id, []kadtest.StrAddr{}),
			Router:       NewRouter[key.Key8](id, top),
			RoutingTable: simplert.New[key.Key8, kad.NodeID[key.Key8]](id, 2),
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
			nodes[i].RoutingTable.AddNode(nodes[i-1].NodeInfo.ID())
		}
		if i < len(nodes)-1 {
			nodes[i].Router.AddNodeInfo(context.Background(), nodes[i+1].NodeInfo, 0)
			nodes[i].RoutingTable.AddNode(nodes[i+1].NodeInfo.ID())
		}
	}

	return top, nodes
}
