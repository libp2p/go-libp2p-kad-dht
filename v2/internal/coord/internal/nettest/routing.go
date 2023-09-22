package nettest

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/plprobelab/go-kademlia/kad"
	"github.com/plprobelab/go-kademlia/key"
	"github.com/plprobelab/go-kademlia/network/address"
	"github.com/plprobelab/go-kademlia/network/endpoint"

	"github.com/libp2p/go-libp2p-kad-dht/v2/kadt"
	"github.com/libp2p/go-libp2p-kad-dht/v2/pb"
)

var rng = rand.New(rand.NewSource(6283185))

func NewPeerID() (kadt.PeerID, error) {
	_, pub, err := crypto.GenerateEd25519Key(rng)
	if err != nil {
		return kadt.PeerID(""), err
	}
	pid, err := peer.IDFromPublicKey(pub)
	if err != nil {
		return kadt.PeerID(""), err
	}

	return kadt.PeerID(pid), nil
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
	self  kadt.PeerID
	top   *Topology
	mu    sync.Mutex // guards nodes
	nodes map[string]*nodeStatus
}

type nodeStatus struct {
	NodeID        kadt.PeerID
	Connectedness endpoint.Connectedness
}

func NewRouter(self kadt.PeerID, top *Topology) *Router {
	return &Router{
		self:  self,
		top:   top,
		nodes: make(map[string]*nodeStatus),
	}
}

func (r *Router) NodeID() kad.NodeID[kadt.Key] {
	return r.self
}

func (r *Router) handleMessage(ctx context.Context, n kadt.PeerID, protoID address.ProtocolID, req *pb.Message) (*pb.Message, error) {
	closer := make([]*pb.Message_Peer, 0)

	r.mu.Lock()
	for _, n := range r.nodes {
		// only include self if it was the target of the request
		if n.NodeID.Equal(r.self) && !key.Equal(n.NodeID.Key(), req.Target()) {
			continue
		}
		closer = append(closer, pb.FromAddrInfo(peer.AddrInfo{ID: peer.ID(n.NodeID)}))
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

func (r *Router) dial(ctx context.Context, to kadt.PeerID) error {
	r.mu.Lock()
	status, ok := r.nodes[to.String()]
	r.mu.Unlock()

	if !ok {
		status = &nodeStatus{
			NodeID:        to,
			Connectedness: endpoint.CanConnect,
		}
	}

	if status.Connectedness == endpoint.Connected {
		return nil
	}
	if err := r.top.Dial(ctx, r.self, to); err != nil {
		return err
	}

	status.Connectedness = endpoint.Connected
	r.mu.Lock()
	r.nodes[to.String()] = status
	r.mu.Unlock()
	return nil
}

func (r *Router) AddToPeerStore(ctx context.Context, id kadt.PeerID) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.nodes[id.String()]; !ok {
		r.nodes[id.String()] = &nodeStatus{
			NodeID:        id,
			Connectedness: endpoint.CanConnect,
		}
	}
	return nil
}

func (r *Router) SendMessage(ctx context.Context, to kadt.PeerID, req *pb.Message) (*pb.Message, error) {
	if err := r.dial(ctx, to); err != nil {
		return nil, fmt.Errorf("dial: %w", err)
	}

	return r.top.RouteMessage(ctx, r.self, to, "", req)
}

func (r *Router) GetClosestNodes(ctx context.Context, to kadt.PeerID, target kadt.Key) ([]kadt.PeerID, error) {
	req := &pb.Message{
		Type: pb.Message_FIND_NODE,
		Key:  []byte("random-key"),
	}

	resp, err := r.SendMessage(ctx, to, req)
	if err != nil {
		return nil, err
	}

	// possibly learned about some new nodes
	for _, id := range resp.CloserNodes() {
		r.AddToPeerStore(ctx, id)
	}

	return resp.CloserNodes(), nil
}
