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

type ProtoKadRequestMessage[K kad.Key[K], N kad.NodeID[K]] interface {
	ProtoKadMessage
	kad.Request[K, N]
}

type ProtoKadResponseMessage[K kad.Key[K], N kad.NodeID[K]] interface {
	ProtoKadMessage
	kad.Response[K, N]
}

func (r *Router) SendMessage(ctx context.Context, to peer.ID, protoID address.ProtocolID, req *pb.Message) (*pb.Message, error) {
	var cancel context.CancelFunc
	ctx, cancel = context.WithCancel(ctx)
	defer cancel()

	ai := r.host.Peerstore().PeerInfo(to)

	var err error
	if err = r.host.Connect(ctx, ai); err != nil {
		return nil, fmt.Errorf("connect to %s: %w", to, err)
	}

	var s network.Stream
	s, err = r.host.NewStream(ctx, to, protocol.ID(protoID))
	if err != nil {
		return nil, fmt.Errorf("stream creation: %w", err)
	}
	defer s.Close()

	w := pbio.NewDelimitedWriter(s)
	reader := msgio.NewVarintReaderSize(s, network.MessageSizeMax)

	fmt.Println("SENDING")
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

	// Don't add addresses for self or our connected peers. We have better ones.
	if to == r.host.ID() || r.host.Network().Connectedness(ai.ID) == network.Connected {
		return &protoResp, nil
	}

	r.host.Peerstore().AddAddrs(ai.ID, ai.Addrs, time.Hour)

	return &protoResp, err
}

func (r *Router) AddNodeInfo(ctx context.Context, ai peer.AddrInfo, ttl time.Duration) error {
	return nil
}

func (r *Router) GetNodeInfo(ctx context.Context, id peer.ID) (peer.AddrInfo, error) {
	return r.host.Peerstore().PeerInfo(id), nil
}

func (r *Router) GetClosestNodes(ctx context.Context, to peer.ID, target key.Key256) ([]peer.AddrInfo, error) {
	resp, err := r.SendMessage(ctx, to, address.ProtocolID(ProtocolAmino), FindKeyRequest(target))
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
