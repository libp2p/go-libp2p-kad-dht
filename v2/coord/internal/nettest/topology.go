package nettest

import (
	"context"
	"fmt"

	"github.com/benbjohnson/clock"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/plprobelab/go-kademlia/key"
	"github.com/plprobelab/go-kademlia/network/address"

	"github.com/libp2p/go-libp2p-kad-dht/v2/coord/routing"
	"github.com/libp2p/go-libp2p-kad-dht/v2/kadt"
	"github.com/libp2p/go-libp2p-kad-dht/v2/pb"
)

type Node struct {
	NodeInfo     peer.AddrInfo
	Router       *Router
	RoutingTable routing.RoutingTableCpl[key.Key256, kadt.PeerID]
}

type Topology struct {
	clk       clock.Clock
	links     map[string]Link
	nodes     []*Node
	nodeIndex map[peer.ID]*Node
	routers   map[peer.ID]*Router
}

func NewTopology(clk clock.Clock) *Topology {
	return &Topology{
		clk:       clk,
		links:     make(map[string]Link),
		nodeIndex: make(map[peer.ID]*Node),
		routers:   make(map[peer.ID]*Router),
	}
}

func (t *Topology) Nodes() []*Node {
	return t.nodes
}

func (t *Topology) ConnectNodes(a *Node, b *Node) {
	t.ConnectNodesWithRoute(a, b, &DefaultLink{})
}

func (t *Topology) ConnectNodesWithRoute(a *Node, b *Node, l Link) {
	akey := a.NodeInfo.ID
	if _, exists := t.nodeIndex[akey]; !exists {
		t.nodeIndex[akey] = a
		t.nodes = append(t.nodes, a)
		t.routers[akey] = a.Router
	}

	bkey := b.NodeInfo.ID
	if _, exists := t.nodeIndex[bkey]; !exists {
		t.nodeIndex[bkey] = b
		t.nodes = append(t.nodes, b)
		t.routers[bkey] = b.Router
	}

	atob := fmt.Sprintf("%s->%s", akey, bkey)
	t.links[atob] = l

	// symmetrical routing assumed
	btoa := fmt.Sprintf("%s->%s", bkey, akey)
	t.links[btoa] = l
}

func (t *Topology) findRoute(ctx context.Context, from peer.ID, to peer.ID) (Link, error) {
	key := fmt.Sprintf("%s->%s", from, to)

	route, ok := t.links[key]
	if !ok {
		return nil, fmt.Errorf("no route to node")
	}

	return route, nil
}

func (t *Topology) Dial(ctx context.Context, from peer.ID, to peer.ID) (peer.AddrInfo, error) {
	if from == to {
		node, ok := t.nodeIndex[to]
		if !ok {
			return peer.AddrInfo{}, fmt.Errorf("unknown node")
		}

		return node.NodeInfo, nil
	}

	route, err := t.findRoute(ctx, from, to)
	if err != nil {
		return peer.AddrInfo{}, fmt.Errorf("find route: %w", err)
	}

	latency := route.DialLatency()
	if latency > 0 {
		t.clk.Sleep(latency)
	}

	if err := route.DialErr(); err != nil {
		return peer.AddrInfo{}, err
	}

	node, ok := t.nodeIndex[to]
	if !ok {
		return peer.AddrInfo{}, fmt.Errorf("unknown node")
	}

	return node.NodeInfo, nil
}

func (t *Topology) RouteMessage(ctx context.Context, from peer.ID, to peer.ID, protoID address.ProtocolID, req *pb.Message) (*pb.Message, error) {
	if from == to {
		node, ok := t.nodeIndex[to]
		if !ok {
			return nil, fmt.Errorf("unknown node")
		}

		return node.Router.HandleMessage(ctx, from, protoID, req)
	}

	route, err := t.findRoute(ctx, from, to)
	if err != nil {
		return nil, fmt.Errorf("find route: %w", err)
	}

	latency := route.ConnLatency()
	if latency > 0 {
		t.clk.Sleep(latency)
	}

	node, ok := t.nodeIndex[to]
	if !ok {
		return nil, fmt.Errorf("no route to node")
	}

	return node.Router.HandleMessage(ctx, from, protoID, req)
}
