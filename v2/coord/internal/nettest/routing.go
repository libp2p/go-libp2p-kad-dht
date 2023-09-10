package nettest

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p-kad-dht/v2/kadt"

	"github.com/libp2p/go-libp2p-kad-dht/v2/coord"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/plprobelab/go-kademlia/key"
	"github.com/plprobelab/go-kademlia/network/address"
	"github.com/plprobelab/go-kademlia/network/endpoint"

	"github.com/libp2p/go-libp2p-kad-dht/v2/pb"
)

var rng = rand.New(rand.NewSource(6283185))

func NewAddrInfo(addrs []ma.Multiaddr) (peer.AddrInfo, error) {
	_, pub, err := crypto.GenerateEd25519Key(rng)
	if err != nil {
		return peer.AddrInfo{}, err
	}
	pid, err := peer.IDFromPublicKey(pub)
	if err != nil {
		return peer.AddrInfo{}, err
	}

	return peer.AddrInfo{
		ID:    pid,
		Addrs: addrs,
	}, nil
}

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

type Router struct {
	self  peer.ID
	top   *Topology
	mu    sync.Mutex // guards nodes
	nodes map[peer.ID]*nodeStatus
}

var _ coord.Router = (*Router)(nil)

type nodeStatus struct {
	NodeInfo      peer.AddrInfo
	Connectedness endpoint.Connectedness
}

func NewRouter(self peer.ID, top *Topology) *Router {
	return &Router{
		self:  self,
		top:   top,
		nodes: make(map[peer.ID]*nodeStatus),
	}
}

func (r *Router) NodeID() kadt.PeerID {
	return kadt.PeerID(r.self)
}

func (r *Router) SendMessage(ctx context.Context, to peer.ID, protoID address.ProtocolID, req *pb.Message) (*pb.Message, error) {
	if err := r.Dial(ctx, to); err != nil {
		return nil, fmt.Errorf("dial: %w", err)
	}

	return r.top.RouteMessage(ctx, r.self, to, protoID, req)
}

func (r *Router) HandleMessage(ctx context.Context, n peer.ID, protoID address.ProtocolID, req *pb.Message) (*pb.Message, error) {
	closer := make([]*pb.Message_Peer, 0)

	r.mu.Lock()
	for _, n := range r.nodes {
		// only include self if it was the target of the request
		if n.NodeInfo.ID == r.self && !key.Equal(kadt.PeerID(n.NodeInfo.ID).Key(), req.Target()) {
			continue
		}
		closer = append(closer, pb.FromAddrInfo(n.NodeInfo))
	}
	r.mu.Unlock()

	// initialize the response message
	resp := &pb.Message{
		Type: req.GetType(),
		Key:  req.GetKey(),
	}
	resp.CloserPeers = closer
	return resp, nil
}

func (r *Router) Dial(ctx context.Context, to peer.ID) error {
	r.mu.Lock()
	status, ok := r.nodes[to]
	r.mu.Unlock()

	if ok {
		switch status.Connectedness {
		case endpoint.Connected:
			return nil
		case endpoint.CanConnect:
			if _, err := r.top.Dial(ctx, r.self, to); err != nil {
				return err
			}

			status.Connectedness = endpoint.Connected
			r.mu.Lock()
			r.nodes[to] = status
			r.mu.Unlock()
			return nil
		}
	}
	return endpoint.ErrUnknownPeer
}

func (r *Router) GetClosestNodes(ctx context.Context, to peer.ID, target kadt.Key) ([]peer.AddrInfo, error) {
	protoID := address.ProtocolID("/test/1.0.0")

	req := &pb.Message{
		Type: pb.Message_FIND_NODE,
		Key:  []byte("random-key"),
	}

	resp, err := r.SendMessage(ctx, to, protoID, req)
	if err != nil {
		return nil, err
	}
	return resp.CloserPeersAddrInfos(), nil
}
