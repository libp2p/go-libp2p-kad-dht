package dual

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p-kad-dht/reprovider"
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
	dht     *DHT
	LAN     *reprovider.SweepingReprovider
	WAN     *reprovider.SweepingReprovider
	mhStore *reprovider.MHStore
}

func (d *DHT) NewSweepingReprovider(opts ...reprovider.Option) (*SweepingReprovider, error) {
	if d == nil || (d.LAN == nil && d.WAN == nil) {
		return nil, errors.New("cannot create sweeping reprovider for nil dual DHT")
	}
	sweepingReproviders := make([]*reprovider.SweepingReprovider, 2)
	var err error
	for i, dht := range []*dht.IpfsDHT{d.LAN, d.WAN} {
		if dht == nil {
			continue
		}
		currentOpts := append([]reprovider.Option{
			reprovider.WithPeerID(dht.PeerID()),
			reprovider.WithReplicationFactor(dht.BucketSize()),
			reprovider.WithSelfAddrs(dht.FilteredAddrs),
			reprovider.WithRouter(dht),
			reprovider.WithMessageSender(dht.MessageSender()),
			reprovider.WithAddLocalRecord(func(h mh.Multihash) error {
				return dht.Provide(dht.Context(), cid.NewCidV1(cid.Raw, h), false)
			}),
		},
			opts...)
		sweepingReproviders[i], err = reprovider.NewReprovider(dht.Context(), currentOpts...)
		if err != nil {
			return nil, err
		}
	}

	return &SweepingReprovider{
		dht: d,
		LAN: sweepingReproviders[0],
		WAN: sweepingReproviders[1],
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

	return combineErrors(errLan, errWan)
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
// to the DHTs even if the keys were already provided in the past.
func (s *SweepingReprovider) ForceProvide(ctx context.Context, keys ...mh.Multihash) error {
	_, err := s.mhStore.Put(ctx, keys...)
	if err != nil {
		return fmt.Errorf("failed to store multihashes: %w", err)
	}
	return s.InstantProvide(ctx, keys...)
}

func (s *SweepingReprovider) ResetReprovideSet(ctx context.Context, keyChan <-chan mh.Multihash) error {
	var errLan, errWan error
	keyChanLan := make(chan mh.Multihash, 1)
	keyChanWan := make(chan mh.Multihash, 1)
	wg := sync.WaitGroup{}
	wg.Add(3)
	go func() {
		defer wg.Done()
		defer close(keyChanLan)
		defer close(keyChanWan)
		for key := range keyChan {
			keyChanLan <- key
			keyChanWan <- key
		}
	}()
	go func() {
		defer wg.Done()
		errLan = s.LAN.ResetReprovideSet(ctx, keyChanLan)
	}()
	go func() {
		defer wg.Done()
		errWan = s.WAN.ResetReprovideSet(ctx, keyChanWan)
	}()
	wg.Wait()
	err := combineErrors(errLan, errWan)
	return err
}
