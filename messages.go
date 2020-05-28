package dht

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"
	"github.com/libp2p/go-libp2p-core/routing"

	record "github.com/libp2p/go-libp2p-record"
	recpb "github.com/libp2p/go-libp2p-record/pb"
	"github.com/multiformats/go-multihash"

	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
)

type ProtocolMessenger struct {
	m         *messageManager
	validator record.Validator
}

func NewProtocolMessenger(host host.Host, protocols []protocol.ID, validator record.Validator) *ProtocolMessenger {
	return &ProtocolMessenger{
		m: &messageManager{
			host:      host,
			strmap:    make(map[peer.ID]*messageSender),
			protocols: protocols,
		},
		validator: validator,
	}
}

// putValueToPeer stores the given key/value pair at the peer 'p'
func (pm *ProtocolMessenger) PutValue(ctx context.Context, p peer.ID, rec *recpb.Record) error {
	pmes := pb.NewMessage(pb.Message_PUT_VALUE, rec.Key, 0)
	pmes.Record = rec
	rpmes, err := pm.m.sendRequest(ctx, p, pmes)
	if err != nil {
		logger.Debugw("failed to put value to peer", "to", p, "key", loggableRecordKeyBytes(rec.Key), "error", err)
		return err
	}

	if !bytes.Equal(rpmes.GetRecord().Value, pmes.GetRecord().Value) {
		logger.Infow("value not put correctly", "put-message", pmes, "get-message", rpmes)
		return errors.New("value not put correctly")
	}

	return nil
}

// GetValue queries a particular peer p for the value for
// key. It returns the value and a list of closer peers.
func (pm *ProtocolMessenger) GetValue(ctx context.Context, p peer.ID, key string) (*recpb.Record, []*peer.AddrInfo, error) {
	pmes := pb.NewMessage(pb.Message_GET_VALUE, []byte(key), 0)
	respMsg, err := pm.m.sendRequest(ctx, p, pmes)
	if err != nil {
		return nil, nil, err
	}

	// Perhaps we were given closer peers
	peers := pb.PBPeersToPeerInfos(respMsg.GetCloserPeers())

	if rec := respMsg.GetRecord(); rec != nil {
		// Success! We were given the value
		logger.Debug("got value")

		// make sure record is valid.
		err = pm.validator.Validate(string(rec.GetKey()), rec.GetValue())
		if err != nil {
			logger.Debug("received invalid record (discarded)")
			// return a sentinal to signify an invalid record was received
			err = errInvalidRecord
			rec = new(recpb.Record)
		}
		return rec, peers, err
	}

	if len(peers) > 0 {
		return nil, peers, nil
	}

	return nil, nil, routing.ErrNotFound
}

// findPeerSingle asks peer 'p' if they know where the peer with id 'id' is
func (pm *ProtocolMessenger) GetClosestPeers(ctx context.Context, p peer.ID, id peer.ID) ([]*peer.AddrInfo, error) {
	pmes := pb.NewMessage(pb.Message_FIND_NODE, []byte(id), 0)
	respMsg, err := pm.m.sendRequest(ctx, p, pmes)
	if err != nil {
		return nil, err
	}
	peers := pb.PBPeersToPeerInfos(respMsg.GetCloserPeers())
	return peers, nil
}

func (pm *ProtocolMessenger) PutProvider(ctx context.Context, p peer.ID, key multihash.Multihash, host host.Host) error {
	pi := peer.AddrInfo{
		ID:    host.ID(),
		Addrs: host.Addrs(),
	}

	// // only share WAN-friendly addresses ??
	// pi.Addrs = addrutil.WANShareableAddrs(pi.Addrs)
	if len(pi.Addrs) < 1 {
		return fmt.Errorf("no known addresses for self, cannot put provider")
	}

	pmes := pb.NewMessage(pb.Message_ADD_PROVIDER, key, 0)
	pmes.ProviderPeers = pb.RawPeerInfosToPBPeers([]peer.AddrInfo{pi})

	return pm.m.sendMessage(ctx, p, pmes)
}

func (pm *ProtocolMessenger) GetProviders(ctx context.Context, p peer.ID, key multihash.Multihash) ([]*peer.AddrInfo, []*peer.AddrInfo, error) {
	pmes := pb.NewMessage(pb.Message_GET_PROVIDERS, key, 0)
	respMsg, err := pm.m.sendRequest(ctx, p, pmes)
	if err != nil {
		return nil, nil, err
	}
	provs := pb.PBPeersToPeerInfos(respMsg.GetProviderPeers())
	closerPeers := pb.PBPeersToPeerInfos(respMsg.GetCloserPeers())
	return provs, closerPeers, nil
}

// Ping sends a ping message to the passed peer and waits for a response.
func (pm *ProtocolMessenger) Ping(ctx context.Context, p peer.ID) error {
	req := pb.NewMessage(pb.Message_PING, nil, 0)
	resp, err := pm.m.sendRequest(ctx, p, req)
	if err != nil {
		return fmt.Errorf("sending request: %w", err)
	}
	if resp.Type != pb.Message_PING {
		return fmt.Errorf("got unexpected response type: %v", resp.Type)
	}
	return nil
}
