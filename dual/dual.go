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
	"github.com/libp2p/go-libp2p-core/protocol"
	"github.com/libp2p/go-libp2p-core/routing"
	dht "github.com/libp2p/go-libp2p-kad-dht"
)

// DHT implements the routing interface to provide two concrete DHT implementationts for use
// in IPFS that are used to support both global network users and disjoint LAN usecases.
type DHT struct {
	WAN *dht.IpfsDHT
	LAN *dht.IpfsDHT
}

// DefaultLanExtension is used to differentiate local protocol requests from those on the WAN DHT.
const DefaultLanExtension protocol.ID = "/lan"

// Assert that IPFS assumptions about interfaces aren't broken. These aren't a
// guarantee, but we can use them to aid refactoring.
var (
	_ routing.ContentRouting = (*DHT)(nil)
	_ routing.Routing        = (*DHT)(nil)
	_ routing.PeerRouting    = (*DHT)(nil)
	_ routing.PubKeyFetcher  = (*DHT)(nil)
	_ routing.ValueStore     = (*DHT)(nil)
)

// New creates a new DualDHT instance. Options provided are forwarded on to the two concrete
// IpfsDHT internal constructions, modulo additional options used by the Dual DHT to enforce
// the LAN-vs-WAN distinction.
// Note: query or routing table functional options provided as arguments to this function
// will be overriden by this constructor.
func New(ctx context.Context, h host.Host, options ...dht.Option) (*DHT, error) {
	wanOpts := append(options,
		dht.QueryFilter(dht.PublicQueryFilter),
		dht.RoutingTableFilter(dht.PublicRoutingTableFilter),
	)
	wan, err := dht.New(ctx, h, wanOpts...)
	if err != nil {
		return nil, err
	}

	// Unless overridden by user supplied options, the LAN DHT should default
	// to 'AutoServer' mode.
	lanOpts := append(options,
		dht.ProtocolExtension(DefaultLanExtension),
		dht.QueryFilter(dht.PrivateQueryFilter),
		dht.RoutingTableFilter(dht.PrivateRoutingTableFilter),
	)
	if wan.Mode() != dht.ModeClient {
		lanOpts = append(lanOpts, dht.Mode(dht.ModeServer))
	}
	lan, err := dht.New(ctx, h, lanOpts...)
	if err != nil {
		return nil, err
	}

	impl := DHT{wan, lan}
	return &impl, nil
}

// Close closes the DHT context.
func (dht *DHT) Close() error {
	return mergeErrors(dht.WAN.Close(), dht.LAN.Close())
}

func (dht *DHT) activeWAN() bool {
	return dht.WAN.RoutingTable().Size() > 0
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
	reqCtx, cancel := context.WithCancel(ctx)
	outCh := make(chan peer.AddrInfo)
	wanCh := dht.WAN.FindProvidersAsync(reqCtx, key, count)
	lanCh := dht.LAN.FindProvidersAsync(reqCtx, key, count)
	go func() {
		defer cancel()
		defer close(outCh)

		found := make(map[peer.ID]struct{}, count)
		nch := 2
		var pi peer.AddrInfo
		for nch > 0 && count > 0 {
			var ok bool
			select {
			case pi, ok = <-wanCh:
				if !ok {
					wanCh = nil
					nch--
					continue
				}
			case pi, ok = <-lanCh:
				if !ok {
					lanCh = nil
					nch--
					continue
				}
			}
			// already found
			if _, ok = found[pi.ID]; ok {
				continue
			}

			select {
			case outCh <- pi:
				found[pi.ID] = struct{}{}
				count--
			case <-ctx.Done():
				return
			}
		}
	}()
	return outCh
}

// FindPeer searches for a peer with given ID
// Note: with signed peer records, we can change this to short circuit once either DHT returns.
func (dht *DHT) FindPeer(ctx context.Context, pid peer.ID) (peer.AddrInfo, error) {
	var wg sync.WaitGroup
	wg.Add(2)
	var wanInfo, lanInfo peer.AddrInfo
	var wanErr, lanErr error
	go func() {
		defer wg.Done()
		wanInfo, wanErr = dht.WAN.FindPeer(ctx, pid)
	}()
	go func() {
		defer wg.Done()
		lanInfo, lanErr = dht.LAN.FindPeer(ctx, pid)
	}()

	wg.Wait()

	return peer.AddrInfo{
		ID:    pid,
		Addrs: append(wanInfo.Addrs, lanInfo.Addrs...),
	}, mergeErrors(wanErr, lanErr)
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
func (d *DHT) GetValue(ctx context.Context, key string, opts ...routing.Option) ([]byte, error) {
	reqCtx, cncl := context.WithCancel(ctx)
	defer cncl()

	resChan := make(chan []byte)
	defer close(resChan)
	errChan := make(chan error)
	defer close(errChan)
	runner := func(impl *dht.IpfsDHT, valCh chan []byte, errCh chan error) {
		val, err := impl.GetValue(reqCtx, key, opts...)
		if err != nil {
			errCh <- err
			return
		}
		valCh <- val
	}
	go runner(d.WAN, resChan, errChan)
	go runner(d.LAN, resChan, errChan)

	var err error
	var val []byte
	select {
	case val = <-resChan:
		cncl()
	case err = <-errChan:
	}

	// Drain or wait for the slower runner
	select {
	case secondVal := <-resChan:
		if val == nil {
			val = secondVal
		}
	case secondErr := <-errChan:
		if err != nil {
			err = mergeErrors(err, secondErr)
		} else if val == nil {
			err = secondErr
		}
	}
	return val, err
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
func (d *DHT) GetPublicKey(ctx context.Context, pid peer.ID) (ci.PubKey, error) {
	reqCtx, cncl := context.WithCancel(ctx)
	defer cncl()

	resChan := make(chan ci.PubKey)
	defer close(resChan)
	errChan := make(chan error)
	defer close(errChan)
	runner := func(impl *dht.IpfsDHT, valCh chan ci.PubKey, errCh chan error) {
		val, err := impl.GetPublicKey(reqCtx, pid)
		if err != nil {
			errCh <- err
			return
		}
		valCh <- val
	}
	go runner(d.WAN, resChan, errChan)
	go runner(d.LAN, resChan, errChan)

	var err error
	var val ci.PubKey
	select {
	case val = <-resChan:
		cncl()
	case err = <-errChan:
	}

	// Drain or wait for the slower runner
	select {
	case secondVal := <-resChan:
		if val == nil {
			val = secondVal
		}
	case secondErr := <-errChan:
		if err != nil {
			err = mergeErrors(err, secondErr)
		} else if val == nil {
			err = secondErr
		}
	}
	return val, err
}
