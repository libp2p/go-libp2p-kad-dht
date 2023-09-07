package dht

import (
	"context"
	"fmt"
	"time"

	"github.com/libp2p/go-libp2p-kad-dht/v2/coord"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-msgio"
	"github.com/libp2p/go-msgio/pbio"
	"github.com/plprobelab/go-kademlia/kad"
	"github.com/plprobelab/go-kademlia/key"
	"github.com/plprobelab/go-kademlia/network/address"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/libp2p/go-libp2p-kad-dht/v2/pb"
)

type Router struct {
	host host.Host
}

var _ coord.Router = (*Router)(nil)

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

func (r *Router) SendMessage(ctx context.Context, to peer.AddrInfo, protoID address.ProtocolID, req *pb.Message) (*pb.Message, error) {
	if err := r.AddNodeInfo(ctx, to, time.Hour); err != nil {
		return nil, fmt.Errorf("add node info: %w", err)
	}

	// TODO: what to do with addresses in peer.AddrInfo?
	if len(r.host.Peerstore().Addrs(to.ID)) == 0 {
		return nil, fmt.Errorf("aaah ProtoKadMessage")
	}

	var cancel context.CancelFunc
	ctx, cancel = context.WithCancel(ctx)
	defer cancel()

	var err error

	var s network.Stream
	s, err = r.host.NewStream(ctx, to.ID, protocol.ID(protoID))
	if err != nil {
		return nil, fmt.Errorf("stream creation: %w", err)
	}
	defer s.Close()

	w := pbio.NewDelimitedWriter(s)
	reader := msgio.NewVarintReaderSize(s, network.MessageSizeMax)

	err = w.WriteMsg(req)
	if err != nil {
		return nil, fmt.Errorf("write message: %w", err)
	}

	data, err := reader.ReadMsg()
	if err != nil {
		return nil, fmt.Errorf("read message: %w", err)
	}
	protoResp := pb.Message{}
	if err = proto.Unmarshal(data, &protoResp); err != nil {
		return nil, err
	}

	for _, info := range protoResp.CloserPeersAddrInfos() {
		_ = r.AddNodeInfo(ctx, info, time.Hour)
	}

	return &protoResp, err
}

func (r *Router) AddNodeInfo(ctx context.Context, ai peer.AddrInfo, ttl time.Duration) error {
	// Don't add addresses for self or our connected peers. We have better ones.
	if ai.ID == r.host.ID() || r.host.Network().Connectedness(ai.ID) == network.Connected {
		return nil
	}

	r.host.Peerstore().AddAddrs(ai.ID, ai.Addrs, ttl)
	return nil
}

func (r *Router) GetNodeInfo(ctx context.Context, id peer.ID) (peer.AddrInfo, error) {
	return r.host.Peerstore().PeerInfo(id), nil
}

func (r *Router) GetClosestNodes(ctx context.Context, to peer.AddrInfo, target key.Key256) ([]peer.AddrInfo, error) {
	resp, err := r.SendMessage(ctx, to, address.ProtocolID(ProtocolIPFS), FindKeyRequest(target))
	if err != nil {
		return nil, err
	}

	return resp.CloserPeersAddrInfos(), nil
}

func FindKeyRequest(k key.Key256) *pb.Message {
	marshalledKey, _ := k.MarshalBinary()
	return &pb.Message{
		Type: pb.Message_FIND_NODE,
		Key:  marshalledKey,
	}
}
