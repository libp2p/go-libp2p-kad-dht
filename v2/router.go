package dht

import (
	"context"
	"fmt"
	"time"

	"github.com/libp2p/go-msgio"

	pb "github.com/libp2p/go-libp2p-kad-dht/v2/pb"

	"github.com/libp2p/go-libp2p/core/peer"
	"google.golang.org/protobuf/proto"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-msgio/pbio"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/iand/zikade/kademlia"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/protocol"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/plprobelab/go-kademlia/kad"
	"github.com/plprobelab/go-kademlia/key"
	"github.com/plprobelab/go-kademlia/network/address"
)

type Router struct {
	host host.Host
}

var _ kademlia.Router[key.Key256, ma.Multiaddr] = (*Router)(nil)

func WriteMsg(s network.Stream, msg protoreflect.ProtoMessage) error {
	w := pbio.NewDelimitedWriter(s)
	return w.WriteMsg(msg)
}

func ReadMsg(s network.Stream, msg proto.Message) error {
	r := pbio.NewDelimitedReader(s, network.MessageSizeMax)
	return r.ReadMsg(msg)
}

type ProtoKadMessage interface {
	proto.Message
}

type ProtoKadRequestMessage[K kad.Key[K], A kad.Address[A]] interface {
	ProtoKadMessage
	kad.Request[K, A]
}

type ProtoKadResponseMessage[K kad.Key[K], A kad.Address[A]] interface {
	ProtoKadMessage
	kad.Response[K, A]
}

func (r *Router) SendMessage(ctx context.Context, to kad.NodeInfo[key.Key256, ma.Multiaddr], protoID address.ProtocolID, req kad.Request[key.Key256, ma.Multiaddr]) (kad.Response[key.Key256, ma.Multiaddr], error) {
	if err := r.AddNodeInfo(ctx, to, time.Hour); err != nil {
		return nil, fmt.Errorf("add node info: %w", err)
	}

	protoReq, ok := req.(ProtoKadMessage)
	if !ok {
		return nil, fmt.Errorf("aaah ProtoKadMessage")
	}

	p := peer.ID(to.ID().(nodeID))

	if len(r.host.Peerstore().Addrs(p)) == 0 {
		return nil, fmt.Errorf("aaah ProtoKadMessage")
	}

	var cancel context.CancelFunc
	ctx, cancel = context.WithCancel(ctx)
	defer cancel()

	var err error

	var s network.Stream
	s, err = r.host.NewStream(ctx, p, protocol.ID(protoID))
	if err != nil {
		return nil, fmt.Errorf("stream creation: %w", err)
	}
	defer s.Close()

	w := pbio.NewDelimitedWriter(s)
	reader := msgio.NewVarintReaderSize(s, network.MessageSizeMax)

	err = w.WriteMsg(protoReq)
	if err != nil {
		return nil, fmt.Errorf("write message: %w", err)
	}

	data, err := reader.ReadMsg()
	if err != nil {
		return nil, fmt.Errorf("read message: %w", err)
	}
	protoResp := pb.Message{}
	if err = protoResp.Unmarshal(data); err != nil {
		return nil, err
	}

	for _, info := range protoResp.CloserPeersAddrInfos() {
		_ = r.AddNodeInfo(ctx, nodeInfo{
			info: info,
		}, time.Hour)
	}

	return &protoResp, err
}

func (r *Router) AddNodeInfo(ctx context.Context, info kad.NodeInfo[key.Key256, ma.Multiaddr], ttl time.Duration) error {
	p := peer.ID(info.ID().(nodeID))

	ai := peer.AddrInfo{
		ID:    p,
		Addrs: info.Addresses(),
	}

	// Don't add addresses for self or our connected peers. We have better ones.
	if ai.ID == r.host.ID() ||
		r.host.Network().Connectedness(ai.ID) == network.Connected {
		return nil
	}
	r.host.Peerstore().AddAddrs(ai.ID, ai.Addrs, ttl)
	return nil
}

func (r *Router) GetNodeInfo(ctx context.Context, id kad.NodeID[key.Key256]) (kad.NodeInfo[key.Key256, ma.Multiaddr], error) {
	// TODO implement me
	panic("implement me")
}

func (r *Router) GetClosestNodes(ctx context.Context, to kad.NodeInfo[key.Key256, ma.Multiaddr], target key.Key256) ([]kad.NodeInfo[key.Key256, ma.Multiaddr], error) {
	resp, err := r.SendMessage(ctx, to, address.ProtocolID(ProtocolIPFS), FindKeyRequest(target))
	if err != nil {
		return nil, err
	}
	return resp.CloserNodes(), nil
}

func FindKeyRequest(k key.Key256) *pb.Message {
	marshalledKey, _ := k.MarshalBinary()
	return &pb.Message{
		Type: pb.Message_FIND_NODE,
		Key:  marshalledKey,
	}
}
