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
	"github.com/libp2p/go-libp2p-kad-dht/provider"
	"github.com/libp2p/go-libp2p-kad-dht/provider/datastore"
	kb "github.com/libp2p/go-libp2p-kbucket"
	mh "github.com/multiformats/go-multihash"
)

var (
	_ provider.BoxoProvider = &SweepingProvider{}
	_ provider.ProvideMany  = &SweepingProvider{}
	_ provider.Provider     = &SweepingProvider{}
)

var rLogger = logging.Logger("dht/dual/provider")

type SweepingProvider struct {
	dht      *dual.DHT
	LAN      *provider.SweepingProvider
	WAN      *provider.SweepingProvider
	keyStore *datastore.KeyStore
}

func NewSweepingProvider(d *dual.DHT, opts ...Option) (*SweepingProvider, error) {
	if d == nil || (d.LAN == nil && d.WAN == nil) {
		return nil, errors.New("cannot create sweeping provider for nil dual DHT")
	}

	var cfg config
	err := cfg.apply(append([]Option{DefaultConfig}, opts...)...)
	if err != nil {
		return nil, err
	}
	err = cfg.validate()
	if err != nil {
		return nil, err
	}

	sweepingProviders := make([]*provider.SweepingProvider, 2)
	for i, dht := range []*dht.IpfsDHT{d.LAN, d.WAN} {
		if dht == nil {
			continue
		}
		dhtOpts := []provider.Option{
			provider.WithPeerID(dht.PeerID()),
			provider.WithReplicationFactor(dht.BucketSize()),
			provider.WithSelfAddrs(dht.FilteredAddrs),
			provider.WithRouter(dht),
			provider.WithMessageSender(dht.MessageSender()),
			provider.WithAddLocalRecord(func(h mh.Multihash) error {
				return dht.Provide(dht.Context(), cid.NewCidV1(cid.Raw, h), false)
			}),
			provider.WithKeyStore(cfg.keyStore),
			provider.WithReprovideInterval(cfg.reprovideInterval[i]),
			provider.WithMaxReprovideDelay(cfg.maxReprovideDelay[i]),
			provider.WithConnectivityCheckOnlineInterval(cfg.connectivityCheckOnlineInterval[i]),
			provider.WithConnectivityCheckOfflineInterval(cfg.connectivityCheckOfflineInterval[i]),
			provider.WithMaxWorkers(cfg.maxWorkers[i]),
			provider.WithDedicatedPeriodicWorkers(cfg.dedicatedPeriodicWorkers[i]),
			provider.WithDedicatedBurstWorkers(cfg.dedicatedBurstWorkers[i]),
			provider.WithMaxProvideConnsPerWorker(cfg.maxProvideConnsPerWorker[i]),
		}
		sweepingProviders[i], err = provider.NewProvider(dht.Context(), dhtOpts...)
		if err != nil {
			return nil, err
		}
	}

	return &SweepingProvider{
		dht:      d,
		LAN:      sweepingProviders[0],
		WAN:      sweepingProviders[1],
		keyStore: cfg.keyStore,
	}, nil
}

func (s *SweepingProvider) runOnBoth(fn func(r *provider.SweepingProvider) error) error {
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
func (s *SweepingProvider) Provide(ctx context.Context, c cid.Cid, announce bool) error {
	return s.runOnBoth(func(r *provider.SweepingProvider) error {
		return r.Provide(ctx, c, announce)
	})
}

// ProvideMany provides multiple cids to the network. It will return an error
// if none of the cids could be provided in either DHT, however it will keep
// reproviding these cids to both networks regardless of the initial provide
// success.
func (s *SweepingProvider) ProvideMany(ctx context.Context, keys []mh.Multihash) error {
	return s.runOnBoth(func(r *provider.SweepingProvider) error {
		return r.ProvideMany(ctx, keys)
	})
}

// StartProviding provides the given keys to both DHT swarms unless they are
// currently being reprovided. The keys will be periodically reprovided until
// StopProviding is called for the same keys or user defined garbage collection
// deletes the keys.
func (s *SweepingProvider) StartProviding(keys ...mh.Multihash) {
	ctx := context.Background()
	cids, err := s.keyStore.Put(ctx, keys...)
	if err != nil {
		rLogger.Errorf("failed to store multihashes: %v", err)
		return
	}
	go s.InstantProvide(ctx, cids...)
}

// StopProviding stops reproviding the given keys to both DHT swarms. The node
// stops being referred as a provider when the provider records in both DHT
// swarms expire.
func (s *SweepingProvider) StopProviding(keys ...mh.Multihash) {
	err := s.keyStore.Delete(context.Background(), keys...)
	if err != nil {
		rLogger.Errorf("failed to stop providing keys: %s", err)
	}
}

// InstantProvide only sends provider records for the given keys out to both
// DHT swarms. It does NOT take the responsibility to reprovide these keys.
func (s *SweepingProvider) InstantProvide(ctx context.Context, keys ...mh.Multihash) error {
	return s.runOnBoth(func(r *provider.SweepingProvider) error {
		return r.InstantProvide(ctx, keys...)
	})
}

// ForceProvide is similar to StartProviding, but it sends provider records out
// to the DHTs even if the keys were already provided in the past. Blocks until
// provide is complete or an error occurs.
func (s *SweepingProvider) ForceProvide(ctx context.Context, keys ...mh.Multihash) error {
	_, err := s.keyStore.Put(ctx, keys...)
	if err != nil {
		return fmt.Errorf("failed to store multihashes: %w", err)
	}
	return s.InstantProvide(ctx, keys...)
}
