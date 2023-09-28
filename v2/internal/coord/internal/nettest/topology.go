package nettest

import (
	"context"
	"fmt"

	"github.com/benbjohnson/clock"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/libp2p/go-libp2p-kad-dht/v2/internal/coord/routing"
	"github.com/libp2p/go-libp2p-kad-dht/v2/kadt"
	"github.com/libp2p/go-libp2p-kad-dht/v2/pb"
)

type Peer struct {
	NodeID       kadt.PeerID
	Router       *Router
	RoutingTable routing.RoutingTableCpl[kadt.Key, kadt.PeerID]
}

type Topology struct {
	clk       clock.Clock
	links     map[string]Link
	nodes     []*Peer
	nodeIndex map[string]*Peer
	routers   map[string]*Router
}

func NewTopology(clk clock.Clock) *Topology {
	return &Topology{
		clk:       clk,
		links:     make(map[string]Link),
		nodeIndex: make(map[string]*Peer),
		routers:   make(map[string]*Router),
	}
}

func (t *Topology) Peers() []*Peer {
	return t.nodes
}

func (t *Topology) ConnectPeers(a *Peer, b *Peer) {
	t.ConnectPeersWithRoute(a, b, &DefaultLink{})
}

func (t *Topology) ConnectPeersWithRoute(a *Peer, b *Peer, l Link) {
	akey := a.NodeID.String()
	if _, exists := t.nodeIndex[akey]; !exists {
		t.nodeIndex[akey] = a
		t.nodes = append(t.nodes, a)
		t.routers[akey] = a.Router
	}

	bkey := b.NodeID.String()
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

func (t *Topology) findRoute(ctx context.Context, from kadt.PeerID, to kadt.PeerID) (Link, error) {
	key := fmt.Sprintf("%s->%s", peer.ID(from), peer.ID(to))

	route, ok := t.links[key]
	if !ok {
		return nil, fmt.Errorf("no route to node")
	}

	return route, nil
}

func (t *Topology) Dial(ctx context.Context, from kadt.PeerID, to kadt.PeerID) error {
	if from == to {
		_, ok := t.nodeIndex[to.String()]
		if !ok {
			return fmt.Errorf("unknown node")
		}

		return nil
	}

	route, err := t.findRoute(ctx, from, to)
	if err != nil {
		return fmt.Errorf("find route: %w", err)
	}

	latency := route.DialLatency()
	if latency > 0 {
		t.clk.Sleep(latency)
	}

	if err := route.DialErr(); err != nil {
		return err
	}

	_, ok := t.nodeIndex[to.String()]
	if !ok {
		return fmt.Errorf("unknown node")
	}

	return nil
}

func (t *Topology) RouteMessage(ctx context.Context, from kadt.PeerID, to kadt.PeerID, req *pb.Message) (*pb.Message, error) {
	if from == to {
		node, ok := t.nodeIndex[to.String()]
		if !ok {
			return nil, fmt.Errorf("unknown node")
		}

		return node.Router.handleMessage(ctx, from, req)
	}

	route, err := t.findRoute(ctx, from, to)
	if err != nil {
		return nil, fmt.Errorf("find route: %w", err)
	}

	latency := route.ConnLatency()
	if latency > 0 {
		t.clk.Sleep(latency)
	}

	node, ok := t.nodeIndex[to.String()]
	if !ok {
		return nil, fmt.Errorf("no route to node")
	}

	return node.Router.handleMessage(ctx, from, req)
}
