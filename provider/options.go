package provider

import (
	"errors"
	"fmt"
	"time"

	"github.com/filecoin-project/go-clock"
	"github.com/libp2p/go-libp2p-kad-dht/amino"
	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
	"github.com/libp2p/go-libp2p-kad-dht/provider/datastore"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	mh "github.com/multiformats/go-multihash"
)

const (
	DefaultMaxReprovideDelay                = 1 * time.Hour
	DefaultConnectivityCheckOnlineInterval  = 1 * time.Minute
	DefaultConnectivityCheckOfflineInterval = 5 * time.Minute
)

type config struct {
	replicationFactor                int
	reprovideInterval                time.Duration
	maxReprovideDelay                time.Duration
	connectivityCheckOnlineInterval  time.Duration
	connectivityCheckOfflineInterval time.Duration

	peerid peer.ID
	router KadClosestPeersRouter

	mhStore *datastore.MHStore

	msgSender      pb.MessageSender
	selfAddrs      func() []ma.Multiaddr
	addLocalRecord func(mh.Multihash) error

	clock clock.Clock

	maxWorkers               int
	dedicatedPeriodicWorkers int
	dedicatedBurstWorkers    int
	maxProvideConnsPerWorker int
}

func (cfg *config) apply(opts ...Option) error {
	for i, o := range opts {
		if err := o(cfg); err != nil {
			return fmt.Errorf("reprovider dht option %d failed: %w", i, err)
		}
	}
	return nil
}

func (c *config) validate() error {
	if len(c.peerid) == 0 {
		return errors.New("reprovider config: peer id is required")
	}
	if c.router == nil {
		return errors.New("reprovider config: router is required")
	}
	if c.msgSender == nil {
		return errors.New("reprovider config: message sender is required")
	}
	if c.selfAddrs == nil {
		return errors.New("reprovider config: self addrs func is required")
	}
	if c.dedicatedPeriodicWorkers+c.dedicatedBurstWorkers > c.maxWorkers {
		return errors.New("reprovider config: total dedicated workers exceed max workers")
	}
	return nil
}

type Option func(opt *config) error

var DefaultConfig = func(cfg *config) error {
	cfg.replicationFactor = amino.DefaultBucketSize
	cfg.reprovideInterval = amino.DefaultReprovideInterval
	cfg.maxReprovideDelay = DefaultMaxReprovideDelay
	cfg.connectivityCheckOnlineInterval = DefaultConnectivityCheckOnlineInterval
	cfg.connectivityCheckOfflineInterval = DefaultConnectivityCheckOfflineInterval

	cfg.clock = clock.New()

	cfg.maxWorkers = 4
	cfg.dedicatedPeriodicWorkers = 2
	cfg.dedicatedBurstWorkers = 1
	cfg.maxProvideConnsPerWorker = 20

	cfg.addLocalRecord = func(mh mh.Multihash) error { return nil }

	return nil
}

func WithReplicationFactor(n int) Option {
	return func(cfg *config) error {
		cfg.replicationFactor = n
		return nil
	}
}

func WithReprovideInterval(d time.Duration) Option {
	return func(cfg *config) error {
		cfg.reprovideInterval = d
		return nil
	}
}

func WithMaxReprovideDelay(d time.Duration) Option {
	return func(cfg *config) error {
		cfg.maxReprovideDelay = d
		return nil
	}
}

func WithConnectivityCheckOnlineInterval(d time.Duration) Option {
	return func(cfg *config) error {
		cfg.connectivityCheckOnlineInterval = d
		return nil
	}
}

func WithConnectivityCheckOfflineInterval(d time.Duration) Option {
	return func(cfg *config) error {
		cfg.connectivityCheckOfflineInterval = d
		return nil
	}
}

func WithPeerID(p peer.ID) Option {
	return func(cfg *config) error {
		cfg.peerid = p
		return nil
	}
}

func WithRouter(r KadClosestPeersRouter) Option {
	return func(cfg *config) error {
		cfg.router = r
		return nil
	}
}

func WithMessageSender(m pb.MessageSender) Option {
	return func(cfg *config) error {
		cfg.msgSender = m
		return nil
	}
}

func WithSelfAddrs(f func() []ma.Multiaddr) Option {
	return func(cfg *config) error {
		cfg.selfAddrs = f
		return nil
	}
}

func WithAddLocalRecord(f func(mh.Multihash) error) Option {
	return func(cfg *config) error {
		if f == nil {
			return errors.New("reprovider config: add local record function cannot be nil")
		}
		cfg.addLocalRecord = f
		return nil
	}
}

func WithClock(c clock.Clock) Option {
	return func(cfg *config) error {
		cfg.clock = c
		return nil
	}
}

func WithMaxWorkers(n int) Option {
	return func(cfg *config) error {
		if n < 0 {
			return errors.New("reprovider config: max workers must be non-negative")
		}
		cfg.maxWorkers = n
		return nil
	}
}

func WithDedicatedPeriodicWorkers(n int) Option {
	return func(cfg *config) error {
		if n < 0 {
			return errors.New("reprovider config: dedicated periodic workers must be non-negative")
		}
		cfg.dedicatedPeriodicWorkers = n
		return nil
	}
}

func WithDedicatedBurstWorkers(n int) Option {
	return func(cfg *config) error {
		if n < 0 {
			return errors.New("reprovider config: dedicated burst workers must be non-negative")
		}
		cfg.dedicatedBurstWorkers = n
		return nil
	}
}

func WithMaxProvideConnsPerWorker(n int) Option {
	return func(cfg *config) error {
		if n <= 0 {
			return errors.New("reprovider config: max provide conns per worker must be greater than 0")
		}
		cfg.maxProvideConnsPerWorker = n
		return nil
	}
}

func WithMHStore(mhStore *datastore.MHStore) Option {
	return func(cfg *config) error {
		if mhStore == nil {
			return errors.New("reprovider config: multihash store cannot be nil")
		}
		cfg.mhStore = mhStore
		return nil
	}
}
