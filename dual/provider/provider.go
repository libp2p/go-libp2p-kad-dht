package provider

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p-kad-dht/dual"
	"github.com/libp2p/go-libp2p-kad-dht/provider"
	"github.com/libp2p/go-libp2p-kad-dht/provider/datastore"
	mh "github.com/multiformats/go-multihash"
)

var logger = logging.Logger(provider.LoggerName)

// SweepingProvider manages provides and reprovides for both DHT swarms (LAN
// and WAN) in the dual DHT setup.
type SweepingProvider struct {
	dht      *dual.DHT
	LAN      *provider.SweepingProvider
	WAN      *provider.SweepingProvider
	keyStore *datastore.KeyStore
}

// New creates a new SweepingProvider that manages provides and reprovides for
// both DHT swarms (LAN and WAN) in a dual DHT setup.
func New(d *dual.DHT, opts ...Option) (*SweepingProvider, error) {
	if d == nil || (d.LAN == nil || d.WAN == nil) {
		return nil, errors.New("cannot create sweeping provider for nil dual DHT")
	}

	var cfg config
	err := cfg.apply(append([]Option{DefaultConfig}, opts...)...)
	if err != nil {
		return nil, err
	}
	cfg.resolveDefaults(d)
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
			provider.WithAddLocalRecord(func(h mh.Multihash) error {
				return dht.Provide(dht.Context(), cid.NewCidV1(cid.Raw, h), false)
			}),
			provider.WithKeyStore(cfg.keyStore),
			provider.WithMessageSender(cfg.msgSenders[i]),
			provider.WithReprovideInterval(cfg.reprovideInterval[i]),
			provider.WithMaxReprovideDelay(cfg.maxReprovideDelay[i]),
			provider.WithConnectivityCheckOnlineInterval(cfg.connectivityCheckOnlineInterval[i]),
			provider.WithConnectivityCheckOfflineInterval(cfg.connectivityCheckOfflineInterval[i]),
			provider.WithMaxWorkers(cfg.maxWorkers[i]),
			provider.WithDedicatedPeriodicWorkers(cfg.dedicatedPeriodicWorkers[i]),
			provider.WithDedicatedBurstWorkers(cfg.dedicatedBurstWorkers[i]),
			provider.WithMaxProvideConnsPerWorker(cfg.maxProvideConnsPerWorker[i]),
		}
		sweepingProviders[i], err = provider.New(dhtOpts...)
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

// runOnBoth runs the provided function on both the LAN and WAN providers in
// parallel and waits for both to complete.
func (s *SweepingProvider) runOnBoth(f func(*provider.SweepingProvider)) {
	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		f(s.LAN)
	}()
	go func() {
		defer wg.Done()
		f(s.WAN)
	}()
	wg.Wait()
}

// ProvideOnce sends provider records for the specified keys to both DHT swarms
// only once. It does not automatically reprovide those keys afterward.
//
// Add the supplied multihashes to the provide queue, and return immediately.
// The provide operation happens asynchronously.
func (s *SweepingProvider) ProvideOnce(keys ...mh.Multihash) {
	s.runOnBoth(func(p *provider.SweepingProvider) {
		p.ProvideOnce(keys...)
	})
}

// StartProviding ensures keys are periodically advertised to both DHT swarms.
//
// If the `keys` aren't currently being reprovided, they are added to the
// queue to be provided to the DHT swarm as soon as possible, and scheduled
// to be reprovided periodically. If `force` is set to true, all keys are
// provided to the DHT swarm, regardless of whether they were already being
// reprovided in the past. `keys` keep being reprovided until `StopProviding`
// is called.
//
// This operation is asynchronous, it returns as soon as the `keys` are added
// to the provide queue, and provides happens asynchronously.
func (s *SweepingProvider) StartProviding(force bool, keys ...mh.Multihash) {
	ctx := context.Background()
	newKeys, err := s.keyStore.Put(ctx, keys...)
	if err != nil {
		logger.Warnf("failed to store multihashes: %v", err)
		return
	}

	s.runOnBoth(func(p *provider.SweepingProvider) {
		p.AddToSchedule(newKeys...)
	})

	if !force {
		keys = newKeys
	}

	s.ProvideOnce(keys...)
}

// StopProviding stops reproviding the given keys to both DHT swarms. The node
// stops being referred as a provider when the provider records in the DHT
// swarms expire.
//
// Remove the `keys` from the schedule and return immediately. Valid records
// can remain in the DHT swarms up to the provider record TTL after calling
// `StopProviding`.
func (s *SweepingProvider) StopProviding(keys ...mh.Multihash) {
	err := s.keyStore.Delete(context.Background(), keys...)
	if err != nil {
		logger.Warnf("failed to stop providing keys: %s", err)
	}
}

// Clear clears the all the keys from the provide queues of both DHTs and
// returns the number of keys that were cleared (sum of both queues).
//
// The keys are not deleted from the keystore, so they will continue to be
// reprovided as scheduled.
func (s *SweepingProvider) Clear() int {
	var total atomic.Int32
	s.runOnBoth(func(p *provider.SweepingProvider) {
		total.Add(int32(p.Clear()))
	})
	return int(total.Load())
}

// RefreshSchedule scans the KeyStore for any keys that are not currently
// scheduled for reproviding. If such keys are found, it schedules their
// associated keyspace region to be reprovided for both DHT providers.
//
// This function doesn't remove prefixes that have no keys from the schedule.
// This is done automatically during the reprovide operation if a region has no
// keys.
func (s *SweepingProvider) RefreshSchedule() {
	s.runOnBoth(func(p *provider.SweepingProvider) {
		p.RefreshSchedule()
	})
}

var (
	_ dhtProvider = (*SweepingProvider)(nil)
	_ dhtProvider = (*provider.SweepingProvider)(nil)
)

// dhtProvider is the interface to ensure that SweepingProvider and
// provider.SweepingProvider share the same interface.
type dhtProvider interface {
	StartProviding(force bool, keys ...mh.Multihash)
	StopProviding(keys ...mh.Multihash)
	ProvideOnce(keys ...mh.Multihash)
	Clear() int
	RefreshSchedule()
}
