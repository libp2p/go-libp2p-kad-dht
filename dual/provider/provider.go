package provider

import (
	"context"
	"errors"

	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p-kad-dht/dual"
	"github.com/libp2p/go-libp2p-kad-dht/provider"
	"github.com/libp2p/go-libp2p-kad-dht/provider/datastore"
	mh "github.com/multiformats/go-multihash"
)

var _ provider.DHTProvider = &SweepingProvider{}

var rLogger = logging.Logger("dht/dual/provider")

type SweepingProvider struct {
	dht      *dual.DHT
	LAN      *provider.SweepingProvider
	WAN      *provider.SweepingProvider
	keyStore *datastore.KeyStore
}

func New(d *dual.DHT, opts ...Option) (*SweepingProvider, error) {
	if d == nil || (d.LAN == nil && d.WAN == nil) {
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

// ProvideOnce only sends provider records for the given keys out to both
// DHT swarms. It does NOT take the responsibility to reprovide these keys.
func (s *SweepingProvider) ProvideOnce(keys ...mh.Multihash) {
	go s.LAN.ProvideOnce(keys...)
	go s.WAN.ProvideOnce(keys...)
}

// StartProviding provides the given keys to both DHT swarms unless they are
// currently being reprovided. The keys will be periodically reprovided until
// StopProviding is called for the same keys or user defined garbage collection
// deletes the keys.
func (s *SweepingProvider) StartProviding(force bool, keys ...mh.Multihash) {
	ctx := context.Background()
	newKeys, err := s.keyStore.Put(ctx, keys...)
	if err != nil {
		rLogger.Errorf("failed to store multihashes: %v", err)
		return
	}

	s.LAN.AddToSchedule(newKeys...)
	s.WAN.AddToSchedule(newKeys...)

	if !force {
		keys = newKeys
	}

	go s.ProvideOnce(keys...)
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

// Clear clears the all the keys from the provide queues of both DHTs and returns the number
// of keys that were cleared (sum of both queues).
//
// The keys are not deleted from the keystore, so they will continue to be
// reprovided as scheduled.
func (s *SweepingProvider) Clear() int {
	return s.LAN.Clear() + s.WAN.Clear()
}

// RefreshSchedule scans the KeyStore for any keys that are not currently
// scheduled for reproviding. If such keys are found, it schedules their
// associated keyspace region to be reprovided for both DHT providers.
//
// This function doesn't remove prefixes that have no keys from the schedule.
// This is done automatically during the reprovide operation if a region has no
// keys.
func (s *SweepingProvider) RefreshSchedule() {
	s.LAN.RefreshSchedule()
	s.WAN.RefreshSchedule()
}
