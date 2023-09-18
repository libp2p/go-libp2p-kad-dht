package dht

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	"github.com/libp2p/go-libp2p-kad-dht/v2/coord"
	"github.com/libp2p/go-libp2p-kad-dht/v2/kadt"
	record "github.com/libp2p/go-libp2p-record"
	recpb "github.com/libp2p/go-libp2p-record/pb"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"
	"go.opentelemetry.io/otel/attribute"
	otel "go.opentelemetry.io/otel/trace"
)

var _ routing.Routing = (*DHT)(nil)

func (d *DHT) FindPeer(ctx context.Context, id peer.ID) (peer.AddrInfo, error) {
	ctx, span := d.tele.Tracer.Start(ctx, "DHT.FindPeer")
	defer span.End()

	// First check locally. If we are or were recently connected to the peer,
	// return the addresses from our peerstore unless the information doesn't
	// contain any.
	switch d.host.Network().Connectedness(id) {
	case network.Connected, network.CanConnect:
		addrInfo := d.host.Peerstore().PeerInfo(id)
		if addrInfo.ID != "" && len(addrInfo.Addrs) > 0 {
			return addrInfo, nil
		}
	default:
		// we're
	}

	target := kadt.PeerID(id)

	var foundNode coord.Node
	fn := func(ctx context.Context, node coord.Node, stats coord.QueryStats) error {
		if node.ID() == id {
			foundNode = node
			return coord.ErrSkipRemaining
		}
		return nil
	}

	_, err := d.kad.Query(ctx, target.Key(), fn)
	if err != nil {
		return peer.AddrInfo{}, fmt.Errorf("failed to run query: %w", err)
	}

	if foundNode == nil {
		return peer.AddrInfo{}, fmt.Errorf("peer record not found")
	}

	return peer.AddrInfo{
		ID:    foundNode.ID(),
		Addrs: foundNode.Addresses(),
	}, nil
}

func (d *DHT) Provide(ctx context.Context, c cid.Cid, brdcst bool) error {
	ctx, span := d.tele.Tracer.Start(ctx, "DHT.Provide", otel.WithAttributes(attribute.String("cid", c.String())))
	defer span.End()

	// verify if this DHT supports provider records by checking if a "providers"
	// backend is registered.
	b, found := d.backends[namespaceProviders]
	if !found {
		return routing.ErrNotSupported
	}

	// verify that it's "defined" CID (not empty)
	if !c.Defined() {
		return fmt.Errorf("invalid cid: undefined")
	}

	// store ourselves as one provider for that CID
	_, err := b.Store(ctx, string(c.Hash()), peer.AddrInfo{ID: d.host.ID()})
	if err != nil {
		return fmt.Errorf("storing own provider record: %w", err)
	}

	// if broadcast is "false" we won't query the DHT
	if !brdcst {
		return nil
	}

	// TODO reach out to Zikade
	panic("implement me")
}

func (d *DHT) FindProvidersAsync(ctx context.Context, c cid.Cid, count int) <-chan peer.AddrInfo {
	_, span := d.tele.Tracer.Start(ctx, "DHT.FindProvidersAsync", otel.WithAttributes(attribute.String("cid", c.String()), attribute.Int("count", count)))
	defer span.End()

	// verify if this DHT supports provider records by checking if a "providers"
	// backend is registered.
	_, found := d.backends[namespaceProviders]
	if !found || !c.Defined() {
		peerOut := make(chan peer.AddrInfo)
		close(peerOut)
		return peerOut
	}

	// TODO reach out to Zikade
	panic("implement me")
}

func (d *DHT) PutValue(ctx context.Context, key string, value []byte, option ...routing.Option) error {
	ctx, span := d.tele.Tracer.Start(ctx, "DHT.PutValue")
	defer span.End()

	ns, path, err := record.SplitKey(key)
	if err != nil {
		return fmt.Errorf("splitting key: %w", err)
	}

	b, found := d.backends[ns]
	if !found {
		return routing.ErrNotSupported
	}

	rec := record.MakePutRecord(key, value)
	rec.TimeReceived = time.Now().UTC().Format(time.RFC3339Nano)

	_, err = b.Store(ctx, path, rec)
	if err != nil {
		return fmt.Errorf("store record locally: %w", err)
	}

	// TODO reach out to Zikade
	panic("implement me")
}

func (d *DHT) GetValue(ctx context.Context, key string, option ...routing.Option) ([]byte, error) {
	ctx, span := d.tele.Tracer.Start(ctx, "DHT.GetValue")
	defer span.End()

	ns, path, err := record.SplitKey(key)
	if err != nil {
		return nil, fmt.Errorf("splitting key: %w", err)
	}

	b, found := d.backends[ns]
	if !found {
		return nil, routing.ErrNotSupported
	}

	val, err := b.Fetch(ctx, path)
	if err != nil {
		if !errors.Is(err, ds.ErrNotFound) {
			return nil, fmt.Errorf("fetch value locally: %w", err)
		}
	} else {
		rec, ok := val.(*recpb.Record)
		if !ok {
			return nil, fmt.Errorf("expected *recpb.Record from backend, got: %T", val)
		}
		return rec.GetValue(), nil
	}

	// TODO reach out to Zikade
	panic("implement me")
}

func (d *DHT) SearchValue(ctx context.Context, s string, option ...routing.Option) (<-chan []byte, error) {
	_, span := d.tele.Tracer.Start(ctx, "DHT.SearchValue")
	defer span.End()

	panic("implement me")
}

func (d *DHT) Bootstrap(ctx context.Context) error {
	ctx, span := d.tele.Tracer.Start(ctx, "DHT.Bootstrap")
	defer span.End()

	seed := make([]peer.ID, len(d.cfg.BootstrapPeers))
	seedStr := make([]string, len(d.cfg.BootstrapPeers))
	for i, addrInfo := range d.cfg.BootstrapPeers {
		seed[i] = addrInfo.ID
		seedStr[i] = addrInfo.ID.String()
	}

	span.SetAttributes(attribute.StringSlice("seed", seedStr))

	return d.kad.Bootstrap(ctx, seed)
}
