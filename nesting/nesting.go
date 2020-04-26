package nesting

import (
	"context"
	"github.com/libp2p/go-libp2p-core/peer"

	"github.com/libp2p/go-libp2p-core/host"

	dht "github.com/libp2p/go-libp2p-kad-dht"
)

type DHT struct {
	Inner, Outer *dht.IpfsDHT
}

func New(ctx context.Context, h host.Host, innerOptions []dht.Option, outerOptions []dht.Option) (*DHT, error) {
	inner, err := dht.New(ctx, h, innerOptions...)
	if err != nil {
		return nil, err
	}

	outer, err := dht.New(ctx, h, outerOptions...)
	if err != nil {
		return nil, err
	}

	d := &DHT{
		Inner: inner,
		Outer: outer,
	}

	return d, nil
}

func (dht *DHT) GetClosestPeers(ctx context.Context, key string) ([]peer.ID, error) {
	var innerResult []peer.ID
	peerCh, err := dht.Inner.GetClosestPeersSeeded(ctx, key, nil)
	if err == nil {
		innerResult = getPeersFromCh(peerCh)
	}

	outerResultCh, err := dht.Outer.GetClosestPeersSeeded(ctx, key, innerResult)
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