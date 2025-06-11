package dual

import (
	"context"
	"sync"

	"github.com/ipfs/go-cid"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p-kad-dht/reprovider"
	mh "github.com/multiformats/go-multihash"
)

type SweepingReprovider struct {
	dht *DHT
	LAN *reprovider.SweepingReprovider
	WAN *reprovider.SweepingReprovider
}

func (d *DHT) NewSweepingReprovider(opts ...reprovider.Option) (*SweepingReprovider, error) {
	sweepingReproviders := make([]*reprovider.SweepingReprovider, 2)
	var err error
	for i, dht := range []*dht.IpfsDHT{d.LAN, d.WAN} {
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

func (s *SweepingReprovider) Provide(ctx context.Context, c cid.Cid, announce bool) error {
	var errLan, errWan error
	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		errLan = s.LAN.Provide(ctx, c, announce)
	}()
	go func() {
		defer wg.Done()
		errWan = s.WAN.Provide(ctx, c, announce)
	}()
	wg.Wait()
	err := combineErrors(errLan, errWan)
	return err
}

func (s *SweepingReprovider) ResetReprovideSet(ctx context.Context, keyChan <-chan mh.Multihash) error {
	var errLan, errWan error
	var keyChanLan, keyChanWan chan mh.Multihash
	wg := sync.WaitGroup{}
	wg.Add(3)
	go func() {
		defer wg.Done()
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
