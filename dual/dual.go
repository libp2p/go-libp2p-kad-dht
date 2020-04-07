// Package dual provides an implementaiton of a split or "dual" dht, where two parallel instances
// are maintained for the global internet and the local LAN respectively.
package dual

import (
	"context"
	"fmt"
	"sync"

	"github.com/ipfs/go-cid"
	ci "github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/routing"
	dht "github.com/libp2p/go-libp2p-kad-dht"
)

// DHT implements the routing interface to provide two concrete DHT implementationts for use
// in IPFS that are used to support both global network users and disjoint LAN usecases.
type DHT struct {
	WAN *dht.IpfsDHT
	LAN *dht.IpfsDHT
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

// NewDHT creates a new DualDHT instance. Options provided are forwarded on to the two concrete
// IpfsDHT internal constructions, modulo additional options used by the Dual DHT to enforce
// the LAN-vs-WAN distinction.
func NewDHT(ctx context.Context, h host.Host, options ...dht.Option) (*DHT, error) {
	wanOpts := append(options,
		dht.ProtocolPrefix(dht.DefaultPrefix),
		dht.QueryFilter(dht.PublicQueryFilter),
		dht.RoutingTableFilter(dht.PublicRoutingTableFilter),
	)
	wan, err := dht.New(ctx, h, wanOpts...)
	if err != nil {
		return nil, err
	}

	lanOpts := append(options,
		dht.ProtocolPrefix(dht.DefaultPrefix+"/lan"),
		dht.QueryFilter(dht.PrivateQueryFilter),
		dht.RoutingTableFilter(dht.PrivateRoutingTableFilter),
	)
	lan, err := dht.New(ctx, h, lanOpts...)
	if err != nil {
		return nil, err
	}

	impl := DHT{wan, lan}
	return &impl, nil
}

func (dht *DHT) activeWAN() bool {
	wanPeers := dht.WAN.RoutingTable().ListPeers()
	return len(wanPeers) > 0
}

// Provide adds the given cid to the content routing system.
func (dht *DHT) Provide(ctx context.Context, key cid.Cid, announce bool) error {
	if dht.activeWAN() {
		return dht.WAN.Provide(ctx, key, announce)
	}
	return dht.LAN.Provide(ctx, key, announce)
}

// FindProvidersAsync searches for peers who are able to provide a given key
func (dht *DHT) FindProvidersAsync(ctx context.Context, key cid.Cid, count int) <-chan peer.AddrInfo {
	if dht.activeWAN() {
		return dht.WAN.FindProvidersAsync(ctx, key, count)
	}
	return dht.LAN.FindProvidersAsync(ctx, key, count)
}

// FindPeer searches for a peer with given ID
func (dht *DHT) FindPeer(ctx context.Context, pid peer.ID) (peer.AddrInfo, error) {
	// TODO: should these be run in parallel?
	infoa, erra := dht.WAN.FindPeer(ctx, pid)
	infob, errb := dht.LAN.FindPeer(ctx, pid)
	return peer.AddrInfo{
		ID:    pid,
		Addrs: append(infoa.Addrs, infob.Addrs...),
	}, mergeErrors(erra, errb)
}

func mergeErrors(a, b error) error {
	if a == nil && b == nil {
		return nil
	} else if a != nil && b != nil {
		return fmt.Errorf("%v, %v", a, b)
	} else if a != nil {
		return a
	}
	return b
}

// Bootstrap allows callers to hint to the routing system to get into a
// Boostrapped state and remain there.
func (dht *DHT) Bootstrap(ctx context.Context) error {
	erra := dht.WAN.Bootstrap(ctx)
	errb := dht.LAN.Bootstrap(ctx)
	return mergeErrors(erra, errb)
}

// PutValue adds value corresponding to given Key.
func (dht *DHT) PutValue(ctx context.Context, key string, val []byte, opts ...routing.Option) error {
	if dht.activeWAN() {
		return dht.WAN.PutValue(ctx, key, val, opts...)
	}
	return dht.LAN.PutValue(ctx, key, val, opts...)
}

// GetValue searches for the value corresponding to given Key.
func (dht *DHT) GetValue(ctx context.Context, key string, opts ...routing.Option) ([]byte, error) {
	vala, erra := dht.WAN.GetValue(ctx, key, opts...)
	if vala != nil {
		return vala, erra
	}
	valb, errb := dht.LAN.GetValue(ctx, key, opts...)
	return valb, mergeErrors(erra, errb)
}

// SearchValue searches for better values from this value
func (dht *DHT) SearchValue(ctx context.Context, key string, opts ...routing.Option) (<-chan []byte, error) {
	streama, erra := dht.WAN.SearchValue(ctx, key, opts...)
	streamb, errb := dht.WAN.SearchValue(ctx, key, opts...)
	if erra == nil && errb == nil {
		combinedStream := make(chan []byte)
		var combinedWg sync.WaitGroup
		combinedWg.Add(2)
		go func(out chan []byte) {
			for itm := range streama {
				out <- itm
			}
			combinedWg.Done()
		}(combinedStream)
		go func(out chan []byte) {
			for itm := range streamb {
				out <- itm
			}
			combinedWg.Done()
		}(combinedStream)
		go func() {
			combinedWg.Wait()
			close(combinedStream)
		}()
		return combinedStream, nil
	} else if erra == nil {
		return streama, nil
	} else if errb == nil {
		return streamb, nil
	}
	return nil, mergeErrors(erra, errb)
}

// GetPublicKey returns the public key for the given peer.
func (dht *DHT) GetPublicKey(ctx context.Context, pid peer.ID) (ci.PubKey, error) {
	pka, erra := dht.WAN.GetPublicKey(ctx, pid)
	if erra == nil {
		return pka, nil
	}
	pkb, errb := dht.LAN.GetPublicKey(ctx, pid)
	if errb == nil {
		return pkb, nil
	}
	return nil, mergeErrors(erra, errb)
}
