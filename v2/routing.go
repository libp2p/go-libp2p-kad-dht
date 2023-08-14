package dht

import (
	"context"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"
)

// Assert that IPFS assumptions about interfaces aren't broken. These aren't a
// guarantee, but we can use them to aid refactoring.
var (
	_ routing.Routing = (*DHT)(nil)
)

func (d DHT) Provide(ctx context.Context, cid cid.Cid, b bool) error {
	panic("implement me")
}

func (d DHT) FindProvidersAsync(ctx context.Context, cid cid.Cid, i int) <-chan peer.AddrInfo {
	panic("implement me")
}

func (d DHT) FindPeer(ctx context.Context, id peer.ID) (peer.AddrInfo, error) {
	panic("implement me")
}

func (d DHT) PutValue(ctx context.Context, s string, bytes []byte, option ...routing.Option) error {
	panic("implement me")
}

func (d DHT) GetValue(ctx context.Context, s string, option ...routing.Option) ([]byte, error) {
	panic("implement me")
}

func (d DHT) SearchValue(ctx context.Context, s string, option ...routing.Option) (<-chan []byte, error) {
	panic("implement me")
}

func (d DHT) Bootstrap(ctx context.Context) error {
	panic("implement me")
}
