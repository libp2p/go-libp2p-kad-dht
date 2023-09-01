package nettest

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/plprobelab/go-kademlia/kad"
	"github.com/plprobelab/go-kademlia/key"
	"github.com/plprobelab/go-kademlia/network/address"
	"github.com/plprobelab/go-kademlia/network/endpoint"
	"github.com/plprobelab/go-kademlia/sim"
)

// Link represents the route between two nodes. It allows latency and transport failures to be simulated.
type Link interface {
	ConnLatency() time.Duration // the simulated time taken to return an error or successful outcome
	DialLatency() time.Duration // the simulated time taken to connect to a node
	DialErr() error             // an error that should be returned on dial, nil if the dial is successful
}

// DefaultLink is the default link used if none is specified.
// It has zero latency and always succeeds.
type DefaultLink struct{}

func (l *DefaultLink) DialErr() error             { return nil }
func (l *DefaultLink) ConnLatency() time.Duration { return 0 }
func (l *DefaultLink) DialLatency() time.Duration { return 0 }

type Router[K kad.Key[K], A kad.Address[A]] struct {
	self  kad.NodeID[K]
	top   *Topology[K, A]
	mu    sync.Mutex // guards nodes
	nodes map[string]*nodeStatus[K, A]
}

type nodeStatus[K kad.Key[K], A kad.Address[A]] struct {
	NodeInfo      kad.NodeInfo[K, A]
	Connectedness endpoint.Connectedness
}

func NewRouter[K kad.Key[K], A kad.Address[A]](self kad.NodeID[K], top *Topology[K, A]) *Router[K, A] {
	return &Router[K, A]{
		self:  self,
		top:   top,
		nodes: make(map[string]*nodeStatus[K, A]),
	}
}

func (r *Router[K, A]) NodeID() kad.NodeID[K] {
	return r.self
}

func (r *Router[K, A]) SendMessage(ctx context.Context, to kad.NodeInfo[K, A], protoID address.ProtocolID, req kad.Request[K, A]) (kad.Response[K, A], error) {
	if err := r.AddNodeInfo(ctx, to, 0); err != nil {
		return nil, fmt.Errorf("add node info: %w", err)
	}

	if err := r.Dial(ctx, to); err != nil {
		return nil, fmt.Errorf("dial: %w", err)
	}

	return r.top.RouteMessage(ctx, r.self, to.ID(), protoID, req)
}

func (r *Router[K, A]) HandleMessage(ctx context.Context, n kad.NodeID[K], protoID address.ProtocolID, req kad.Request[K, A]) (kad.Response[K, A], error) {
	closer := make([]kad.NodeInfo[K, A], 0)

	r.mu.Lock()
	for _, n := range r.nodes {
		// only include self if it was the target of the request
		if key.Equal(n.NodeInfo.ID().Key(), r.self.Key()) && !key.Equal(n.NodeInfo.ID().Key(), req.Target()) {
			continue
		}
		closer = append(closer, n.NodeInfo)
	}
	r.mu.Unlock()

	resp := sim.NewResponse(closer)
	return resp, nil
}

func (r *Router[K, A]) Dial(ctx context.Context, to kad.NodeInfo[K, A]) error {
	tkey := key.HexString(to.ID().Key())

	r.mu.Lock()
	status, ok := r.nodes[tkey]
	r.mu.Unlock()

	if ok {
		switch status.Connectedness {
		case endpoint.Connected:
			return nil
		case endpoint.CanConnect:
			if _, err := r.top.Dial(ctx, r.self, to.ID()); err != nil {
				return err
			}

			status.Connectedness = endpoint.Connected
			r.mu.Lock()
			r.nodes[tkey] = status
			r.mu.Unlock()
			return nil
		}
	}
	return endpoint.ErrUnknownPeer
}

func (r *Router[K, A]) AddNodeInfo(ctx context.Context, info kad.NodeInfo[K, A], ttl time.Duration) error {
	key := key.HexString(info.ID().Key())
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.nodes[key]; !ok {
		r.nodes[key] = &nodeStatus[K, A]{
			NodeInfo:      info,
			Connectedness: endpoint.CanConnect,
		}
	}
	return nil
}

func (r *Router[K, A]) GetNodeInfo(ctx context.Context, id kad.NodeID[K]) (kad.NodeInfo[K, A], error) {
	key := key.HexString(id.Key())
	r.mu.Lock()
	defer r.mu.Unlock()

	status, ok := r.nodes[key]
	if !ok {
		return nil, fmt.Errorf("unknown node")
	}
	return status.NodeInfo, nil
}

func (r *Router[K, A]) GetClosestNodes(ctx context.Context, to kad.NodeInfo[K, A], target K) ([]kad.NodeInfo[K, A], error) {
	protoID := address.ProtocolID("/test/1.0.0")

	resp, err := r.SendMessage(ctx, to, protoID, sim.NewRequest[K, A](target))
	if err != nil {
		return nil, err
	}
	return resp.CloserNodes(), nil
}
