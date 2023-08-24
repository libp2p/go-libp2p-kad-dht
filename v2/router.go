package dht

import (
	"context"
	"fmt"
	"time"

	"github.com/iand/zikade/kademlia"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/plprobelab/go-kademlia/kad"
	"github.com/plprobelab/go-kademlia/key"
	"github.com/plprobelab/go-kademlia/network/address"
)

var _ kademlia.Router[key.Key256, ma.Multiaddr] = (*DHT)(nil)

func (d *DHT) SendMessage(ctx context.Context, to kad.NodeInfo[key.Key256, ma.Multiaddr], protoID address.ProtocolID, req kad.Request[key.Key256, ma.Multiaddr]) (kad.Response[key.Key256, ma.Multiaddr], error) {
	s, err := d.host.NewStream(ctx, peer.ID(to.ID().(nodeID)), d.cfg.ProtocolID)
	if err != nil {
		return nil, fmt.Errorf("new stream: %w", err)
	}
	defer d.logErr(s.Close(), "failed to close stream")

	return nil, nil
}

func (d *DHT) AddNodeInfo(ctx context.Context, info kad.NodeInfo[key.Key256, ma.Multiaddr], ttl time.Duration) error {
	// TODO implement me
	panic("implement me")
}

func (d *DHT) GetNodeInfo(ctx context.Context, id kad.NodeID[key.Key256]) (kad.NodeInfo[key.Key256, ma.Multiaddr], error) {
	// TODO implement me
	panic("implement me")
}

func (d *DHT) GetClosestNodes(ctx context.Context, to kad.NodeInfo[key.Key256, ma.Multiaddr], target key.Key256) ([]kad.NodeInfo[key.Key256, ma.Multiaddr], error) {
	// TODO implement me
	panic("implement me")
}
