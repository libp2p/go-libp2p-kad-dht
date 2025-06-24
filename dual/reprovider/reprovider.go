package reprovider

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p-kad-dht/dual"
	"github.com/libp2p/go-libp2p-kad-dht/reprovider"
	"github.com/libp2p/go-libp2p-kad-dht/reprovider/datastore"
	kb "github.com/libp2p/go-libp2p-kbucket"
	mh "github.com/multiformats/go-multihash"
)

var (
	_ reprovider.Provider    = &SweepingReprovider{}
	_ reprovider.ProvideMany = &SweepingReprovider{}
	_ reprovider.Reprovider  = &SweepingReprovider{}
)

var rLogger = logging.Logger("dht/dual/reprovider")

type SweepingReprovider struct {
	dht     *dual.DHT
	LAN     *reprovider.SweepingReprovider
	WAN     *reprovider.SweepingReprovider
	mhStore *datastore.MHStore
}

func NewSweepingReprovider(d *dual.DHT, opts ...ReproviderOption) (*SweepingReprovider, error) {
	if d == nil || (d.LAN == nil && d.WAN == nil) {
		return nil, errors.New("cannot create sweeping reprovider for nil dual DHT")
	}

	var cfg reproviderConfig
	err := cfg.apply(append([]ReproviderOption{DefaultReproviderConfig}, opts...)...)
	if err != nil {
		return nil, err
	}
	err = cfg.validate()
	if err != nil {
		return nil, err
	}

	sweepingReproviders := make([]*reprovider.SweepingReprovider, 2)
	for i, dht := range []*dht.IpfsDHT{d.LAN, d.WAN} {
		if dht == nil {
			continue
		}
		dhtOpts := []reprovider.Option{
			reprovider.WithPeerID(dht.PeerID()),
			reprovider.WithReplicationFactor(dht.BucketSize()),
			reprovider.WithSelfAddrs(dht.FilteredAddrs),
			reprovider.WithRouter(dht),
			reprovider.WithMessageSender(dht.MessageSender()),
			reprovider.WithAddLocalRecord(func(h mh.Multihash) error {
				return dht.Provide(dht.Context(), cid.NewCidV1(cid.Raw, h), false)
			}),
			reprovider.WithMHStore(cfg.mhStore),
			reprovider.WithReprovideInterval(cfg.reprovideInterval[i]),
			reprovider.WithMaxReprovideDelay(cfg.maxReprovideDelay[i]),
			reprovider.WithConnectivityCheckOnlineInterval(cfg.connectivityCheckOnlineInterval[i]),
			reprovider.WithConnectivityCheckOfflineInterval(cfg.connectivityCheckOfflineInterval[i]),
			reprovider.WithMaxWorkers(cfg.maxWorkers[i]),
			reprovider.WithDedicatedPeriodicWorkers(cfg.dedicatedPeriodicWorkers[i]),
			reprovider.WithDedicatedBurstWorkers(cfg.dedicatedBurstWorkers[i]),
			reprovider.WithMaxProvideConnsPerWorker(cfg.maxProvideConnsPerWorker[i]),
		}
		sweepingReproviders[i], err = reprovider.NewReprovider(dht.Context(), dhtOpts...)
		if err != nil {
			return nil, err
		}
	}

	return &SweepingReprovider{
		dht:     d,
		LAN:     sweepingReproviders[0],
		WAN:     sweepingReproviders[1],
		mhStore: cfg.mhStore,
	}, nil
}

func (s *SweepingReprovider) runOnBoth(fn func(r *reprovider.SweepingReprovider) error) error {
	var errLan, errWan error
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		errLan = fn(s.LAN)
	}()
	go func() {
		defer wg.Done()
		errWan = fn(s.WAN)
	}()

	wg.Wait()

	// ignore empty LAN-table errors
	if errLan == kb.ErrLookupFailure {
		errLan = nil
	}

	return dual.CombineErrors(errLan, errWan)
}

// Provide returns an error if the cid failed to be provided to either network.
// However, it will keep reproviding the cid to both networks regardless of
// whether the first provide succeeded.
func (s *SweepingReprovider) Provide(ctx context.Context, c cid.Cid, announce bool) error {
	return s.runOnBoth(func(r *reprovider.SweepingReprovider) error {
		return r.Provide(ctx, c, announce)
	})
}

// ProvideMany provides multiple cids to the network. It will return an error
// if none of the cids could be provided in either DHT, however it will keep
// reproviding these cids to both networks regardless of the initial provide
// success.
func (s *SweepingReprovider) ProvideMany(ctx context.Context, keys []mh.Multihash) error {
	return s.runOnBoth(func(r *reprovider.SweepingReprovider) error {
		return r.ProvideMany(ctx, keys)
	})
}

// StartProviding provides the given keys to both DHT swarms unless they are
// currently being reprovided. The keys will be periodically reprovided until
// StopProviding is called for the same keys or user defined garbage collection
// deletes the keys.
func (s *SweepingReprovider) StartProviding(keys ...mh.Multihash) {
	ctx := context.Background()
	cids, err := s.mhStore.Put(ctx, keys...)
	if err != nil {
		rLogger.Errorf("failed to store multihashes: %v", err)
		return
	}
	go s.InstantProvide(ctx, cids...)
}

// StopProviding stops reproviding the given keys to both DHT swarms. The node
// stops being referred as a provider when the provider records in both DHT
// swarms expire.
func (s *SweepingReprovider) StopProviding(keys ...mh.Multihash) {
	err := s.mhStore.Delete(context.Background(), keys...)
	if err != nil {
		rLogger.Errorf("failed to stop providing keys: %s", err)
	}
}

// InstantProvide only sends provider records for the given keys out to both
// DHT swarms. It does NOT take the responsibility to reprovide these keys.
func (s *SweepingReprovider) InstantProvide(ctx context.Context, keys ...mh.Multihash) error {
	return s.runOnBoth(func(r *reprovider.SweepingReprovider) error {
		return r.InstantProvide(ctx, keys...)
	})
}

// ForceProvide is similar to StartProviding, but it sends provider records out
// to the DHTs even if the keys were already provided in the past. Blocks until
// provide is complete or an error occurs.
func (s *SweepingReprovider) ForceProvide(ctx context.Context, keys ...mh.Multihash) error {
	_, err := s.mhStore.Put(ctx, keys...)
	if err != nil {
		return fmt.Errorf("failed to store multihashes: %w", err)
	}
	return s.InstantProvide(ctx, keys...)
}
