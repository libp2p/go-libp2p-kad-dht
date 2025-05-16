package reprovider

import (
	"errors"
	"fmt"
	"time"

	"github.com/filecoin-project/go-clock"
	"github.com/libp2p/go-libp2p-kad-dht/amino"
	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

type config struct {
	replicationFactor int
	reprovideInterval time.Duration
	maxReprovideDelay time.Duration

	peerid peer.ID
	router KadRouter

	msgSender               pb.MessageSender
	selfAddrs               func() []ma.Multiaddr
	localNearestPeersToSelf func(int) []peer.ID

	provideWorkers int
	clock          clock.Clock
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
	if c.localNearestPeersToSelf == nil {
		return errors.New("reprovider config: local nearest peers to self func is required")
	}
	return nil
}

type Option func(opt *config) error

var DefaultConfig = config{
	replicationFactor: amino.DefaultBucketSize,
	reprovideInterval: 22 * time.Hour,
	maxReprovideDelay: 1 * time.Hour,

	provideWorkers: 4,
	clock:          clock.New(),
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

func WithPeerID(p peer.ID) Option {
	return func(cfg *config) error {
		cfg.peerid = p
		return nil
	}
}

func WithRouter(r KadRouter) Option {
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

func WithLocalNearestPeersToSelf(f func(int) []peer.ID) Option {
	return func(cfg *config) error {
		cfg.localNearestPeersToSelf = f
		return nil
	}
}

func WithProvideWorkers(n int) Option {
	return func(cfg *config) error {
		cfg.provideWorkers = n
		return nil
	}
}

func WithClock(c clock.Clock) Option {
	return func(cfg *config) error {
		cfg.clock = c
		return nil
	}
}
