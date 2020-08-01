package nesting

import (
	"context"
	"fmt"
	"github.com/hashicorp/go-multierror"
	"github.com/ipfs/go-cid"
	ci "github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/routing"
	"github.com/pkg/errors"

	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"

	"github.com/libp2p/go-libp2p-kad-dht"
)

// DHT implements the routing interface to
type DHT struct {
	Inner, Outer *dht.IpfsDHT
}

// Assert that IPFS assumptions about interfaces aren't broken. These aren't a
// guarantee, but we can use them to aid refactoring.
var (
	_ routing.ContentRouting = (*DHT)(nil)
	_ routing.Routing        = (*DHT)(nil)
	_ routing.PeerRouting    = (*DHT)(nil)
	_ routing.PubKeyFetcher  = (*DHT)(nil)
	_ routing.ValueStore     = (*DHT)(nil)
)

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

func (dht *DHT) GetPublicKey(ctx context.Context, id peer.ID) (ci.PubKey, error) {
	panic("implement me")
}

func (dht *DHT) Provide(ctx context.Context, cid cid.Cid, b bool) error {
	panic("implement me")
}

func (dht *DHT) FindProvidersAsync(ctx context.Context, cid cid.Cid, i int) <-chan peer.AddrInfo {
	panic("implement me")
}

func (dht *DHT) FindPeer(ctx context.Context, id peer.ID) (peer.AddrInfo, error) {
	panic("implement me")
}

func (dht *DHT) PutValue(ctx context.Context, s string, bytes []byte, option ...routing.Option) error {
	panic("implement me")
}

func (dht *DHT) GetValue(ctx context.Context, s string, option ...routing.Option) ([]byte, error) {
	panic("implement me")
}

func (dht *DHT) SearchValue(ctx context.Context, s string, option ...routing.Option) (<-chan []byte, error) {
	panic("implement me")
}

func (dht *DHT) Bootstrap(ctx context.Context) error {
	errI := dht.Inner.Bootstrap(ctx)
	errO := dht.Outer.Bootstrap(ctx)

	errs := make([]error, 0, 2)
	if errI != nil {
		errs = append(errs, errors.Wrap(errI, fmt.Sprintf("failed to bootstrap inner dht")))
	}
	if errO != nil {
		errs = append(errs, errors.Wrap(errI, fmt.Sprintf("failed to bootstrap outer dht")))
	}

	switch len(errs) {
	case 0:
		return nil
	case 1:
		return errs[0]
	default:
		return multierror.Append(errs[0], errs[1:]...)
	}
}