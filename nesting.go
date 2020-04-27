package dht

import (
	"context"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	pstore "github.com/libp2p/go-libp2p-core/peerstore"
	"github.com/libp2p/go-libp2p-core/protocol"
	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
	"github.com/libp2p/go-msgio"
	"io"
	"time"

	"github.com/libp2p/go-libp2p-core/host"
)

type NestedDHT struct {
	Inner, Outer *IpfsDHT
}

const transferProto protocol.ID = "/adin/ipfs/kad/1.0.0"

func NewNested(ctx context.Context, h host.Host, innerOptions []Option, outerOptions []Option) (*NestedDHT, error) {
	inner, err := New(ctx, h, innerOptions...)
	if err != nil {
		return nil, err
	}

	outer, err := New(ctx, h, outerOptions...)
	if err != nil {
		return nil, err
	}

	d := &NestedDHT{
		Inner: inner,
		Outer: outer,
	}

	h.SetStreamHandler(transferProto, func(s network.Stream) {
		defer s.Reset() //nolint
		if d.handleNewMessage(s) {
			// Gracefully close the stream for writes.
			s.Close()
		}
	})

	return d, nil
}

// Returns true on orderly completion of writes (so we can Close the stream).
func (dht *NestedDHT) handleNewMessage(s network.Stream) bool {
	ctx := dht.Inner.ctx
	r := msgio.NewVarintReaderSize(s, network.MessageSizeMax)

	mPeer := s.Conn().RemotePeer()

	timer := time.AfterFunc(dhtStreamIdleTimeout, func() { _ = s.Reset() })
	defer timer.Stop()

	for {
		if dht.Inner.getMode() != modeServer {
			logger.Errorf("ignoring incoming dht message while not in server mode")
			return false
		}

		var req pb.Message
		msgbytes, err := r.ReadMsg()
		msgLen := len(msgbytes)
		if err != nil {
			r.ReleaseMsg(msgbytes)
			if err == io.EOF {
				return true
			}
			// This string test is necessary because there isn't a single stream reset error
			// instance	in use.
			if err.Error() != "stream reset" {
				logger.Debugf("error reading message: %#v", err)
			}
			if msgLen > 0 {
			}
			return false
		}
		err = req.Unmarshal(msgbytes)
		r.ReleaseMsg(msgbytes)
		if err != nil {
			return false
		}

		timer.Reset(dhtStreamIdleTimeout)

		var handler dhtHandler
		if req.GetType() == pb.Message_FIND_NODE {
			handler = dht.Outer.handlerForMsgType(req.GetType())
		}

		if handler == nil {
			return false
		}

		resp, err := handler(ctx, mPeer, &req)
		if err != nil {
			return false
		}

		if resp == nil {
			continue
		}

		// send out response msg
		err = writeMsg(s, resp)
		if err != nil {
			return false
		}
	}
}

type nestedProtocolSender struct {
	host host.Host
	proto protocol.ID
}

func (ps *nestedProtocolSender) getHost() host.Host {
	return ps.host
}

func (ps *nestedProtocolSender) getProtocols() []protocol.ID {
	return []protocol.ID{ps.proto}
}

func (dht *NestedDHT) transferGCP(ctx context.Context, p peer.ID, key string) ([]*peer.AddrInfo, error){
	pmes := pb.NewMessage(pb.Message_FIND_NODE, []byte(key), 0)
	nps := &nestedProtocolSender{host: dht.Inner.host, proto: transferProto}
	ms := &messageSender{p: p, dht: nps, lk: newCtxMutex()}
	resp, err := ms.SendRequest(ctx,pmes)
	if err != nil {
		return nil, err
	}
	peers := pb.PBPeersToPeerInfos(resp.GetCloserPeers())
	return peers, nil
}

func (dht *NestedDHT) GetClosestPeers(ctx context.Context, key string) ([]peer.ID, error) {
	var innerResult []peer.ID
	peerCh, err := dht.Inner.GetClosestPeersSeeded(ctx, key, nil)
	if err == nil {
		innerResult = getPeersFromCh(peerCh)
	}

	seedPeerSet := peer.NewSet()
	for _, p := range innerResult {
		innerTransferPeers, err := dht.transferGCP(ctx, p, key)
		if err == nil {
			for _, outerPeer := range innerTransferPeers {
				if seedPeerSet.TryAdd(outerPeer.ID) {
					dht.Inner.host.Peerstore().AddAddrs(outerPeer.ID, outerPeer.Addrs, pstore.TempAddrTTL)
				}
			}
		}
	}

	outerResultCh, err := dht.Outer.GetClosestPeersSeeded(ctx, key, seedPeerSet.Peers())
	if err != nil {
		return nil, err
	}

	return getPeersFromCh(outerResultCh), nil
}

func getPeersFromCh(peerCh <-chan peer.ID) []peer.ID {
	var peers []peer.ID
	for p := range peerCh {
		peers = append(peers, p)
	}
	return peers
}
