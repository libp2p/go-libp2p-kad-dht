package nettest

import (
	"context"
	"fmt"

	"github.com/benbjohnson/clock"

	"github.com/plprobelab/go-kademlia/kad"
	"github.com/plprobelab/go-kademlia/key"
	"github.com/plprobelab/go-kademlia/network/address"
	"github.com/plprobelab/go-kademlia/routing"
)

type Node[K kad.Key[K], A kad.Address[A]] struct {
	NodeInfo     kad.NodeInfo[K, A]
	Router       *Router[K, A]
	RoutingTable routing.RoutingTableCpl[K, kad.NodeID[K]]
}

type Topology[K kad.Key[K], A kad.Address[A]] struct {
	clk       *clock.Mock
	links     map[string]Link
	nodes     []*Node[K, A]
	nodeIndex map[string]*Node[K, A]
	routers   map[string]*Router[K, A]
}

func NewTopology[K kad.Key[K], A kad.Address[A]](clk *clock.Mock) *Topology[K, A] {
	return &Topology[K, A]{
		clk:       clk,
		links:     make(map[string]Link),
		nodeIndex: make(map[string]*Node[K, A]),
		routers:   make(map[string]*Router[K, A]),
	}
}

func (t *Topology[K, A]) Nodes() []*Node[K, A] {
	return t.nodes
}

func (t *Topology[K, A]) ConnectNodes(a *Node[K, A], b *Node[K, A]) {
	t.ConnectNodesWithRoute(a, b, &DefaultLink{})
}

func (t *Topology[K, A]) ConnectNodesWithRoute(a *Node[K, A], b *Node[K, A], l Link) {
	akey := key.HexString(a.NodeInfo.ID().Key())
	if _, exists := t.nodeIndex[akey]; !exists {
		t.nodeIndex[akey] = a
		t.nodes = append(t.nodes, a)
		t.routers[akey] = a.Router
	}

	bkey := key.HexString(b.NodeInfo.ID().Key())
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

func (t *Topology[K, A]) findRoute(ctx context.Context, from kad.NodeID[K], to kad.NodeID[K]) (Link, error) {
	fkey := key.HexString(from.Key())
	tkey := key.HexString(to.Key())

	key := fmt.Sprintf("%s->%s", fkey, tkey)

	route, ok := t.links[key]
	if !ok {
		return nil, fmt.Errorf("no route to node")
	}

	return route, nil
}

func (t *Topology[K, A]) Dial(ctx context.Context, from kad.NodeID[K], to kad.NodeID[K]) (kad.NodeInfo[K, A], error) {
	if key.Equal(from.Key(), to.Key()) {
		tkey := key.HexString(to.Key())
		node, ok := t.nodeIndex[tkey]
		if !ok {
			return nil, fmt.Errorf("unknown node")
		}

		return node.NodeInfo, nil
	}

	route, err := t.findRoute(ctx, from, to)
	if err != nil {
		return nil, fmt.Errorf("find route: %w", err)
	}

	latency := route.DialLatency()
	if latency > 0 {
		t.clk.Sleep(latency)
	}

	if err := route.DialErr(); err != nil {
		return nil, err
	}

	tkey := key.HexString(to.Key())
	node, ok := t.nodeIndex[tkey]
	if !ok {
		return nil, fmt.Errorf("unknown node")
	}

	return node.NodeInfo, nil
}

func (t *Topology[K, A]) RouteMessage(ctx context.Context, from kad.NodeID[K], to kad.NodeID[K], protoID address.ProtocolID, req kad.Request[K, A]) (kad.Response[K, A], error) {
	if key.Equal(from.Key(), to.Key()) {
		tkey := key.HexString(to.Key())
		node, ok := t.nodeIndex[tkey]
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

	tkey := key.HexString(to.Key())
	node, ok := t.nodeIndex[tkey]
	if !ok {
		return nil, fmt.Errorf("no route to node")
	}

	return node.Router.HandleMessage(ctx, from, protoID, req)
}
