package dht

import (
	"context"
	"fmt"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-msgio"
	"github.com/libp2p/go-msgio/pbio"
	"google.golang.org/protobuf/proto"

	"github.com/libp2p/go-libp2p-kad-dht/v2/coord"
	"github.com/libp2p/go-libp2p-kad-dht/v2/kadt"
	"github.com/libp2p/go-libp2p-kad-dht/v2/pb"
)

type Router struct {
	host host.Host
	// ProtocolID represents the DHT [protocol] we can query with and respond to.
	//
	// [protocol]: https://docs.libp2p.io/concepts/fundamentals/protocols/
	ProtocolID protocol.ID
}

var _ coord.Router[kadt.Key, kadt.PeerID, *pb.Message] = (*Router)(nil)

func FindKeyRequest(k kadt.Key) *pb.Message {
	marshalledKey, _ := k.MarshalBinary()
	return &pb.Message{
		Type: pb.Message_FIND_NODE,
		Key:  marshalledKey,
	}
}

func (r *Router) SendMessage(ctx context.Context, to kadt.PeerID, req *pb.Message) (*pb.Message, error) {
	// TODO: what to do with addresses in peer.AddrInfo?
	if len(r.host.Peerstore().Addrs(peer.ID(to))) == 0 {
		return nil, fmt.Errorf("no address for peer %s", to)
	}

	var cancel context.CancelFunc
	ctx, cancel = context.WithCancel(ctx)
	defer cancel()

	var err error

	var s network.Stream
	s, err = r.host.NewStream(ctx, peer.ID(to), r.ProtocolID)
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
		_ = r.addToPeerStore(ctx, info, time.Hour) // TODO: replace hard coded time.Hour with config
	}

	return &protoResp, err
}

func (r *Router) GetClosestNodes(ctx context.Context, to kadt.PeerID, target kadt.Key) ([]kadt.PeerID, error) {
	resp, err := r.SendMessage(ctx, to, FindKeyRequest(target))
	if err != nil {
		return nil, err
	}

	return resp.CloserNodes(), nil
}

func (r *Router) addToPeerStore(ctx context.Context, ai peer.AddrInfo, ttl time.Duration) error {
	// Don't add addresses for self or our connected peers. We have better ones.
	if ai.ID == r.host.ID() || r.host.Network().Connectedness(ai.ID) == network.Connected {
		return nil
	}

	r.host.Peerstore().AddAddrs(ai.ID, ai.Addrs, ttl)
	return nil
}
