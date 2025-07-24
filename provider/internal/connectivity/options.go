package connectivity

import (
	"fmt"
	"time"

	"github.com/filecoin-project/go-clock"
)

type config struct {
	clock                clock.Clock
	onlineCheckInterval  time.Duration // minimum check interval when online
	offlineCheckInterval time.Duration // periodic check frequency when offline
}

func (cfg *config) apply(opts ...Option) error {
	for i, o := range opts {
		if err := o(cfg); err != nil {
			return fmt.Errorf("reprovider dht option %d failed: %w", i, err)
		}
	}
	return nil
}

type Option func(opt *config) error

var DefaultConfig = func(cfg *config) error {
	cfg.clock = clock.New()
	cfg.onlineCheckInterval = 1 * time.Minute
	cfg.offlineCheckInterval = 5 * time.Minute
	return nil
}

// WithClock sets the clock used by the connectivity checker.
func WithClock(c clock.Clock) Option {
	return func(cfg *config) error {
		cfg.clock = c
		return nil
	}
}

// WithOnlineCheckInterval sets the minimum interval between online checks.
// This is used to throttle the number of connectivity checks when the node is
// online.
func WithOnlineCheckInterval(d time.Duration) Option {
	return func(cfg *config) error {
		if d <= 0 {
			return fmt.Errorf("online check interval must be positive, got %s", d)
		}
		cfg.onlineCheckInterval = d
		return nil
	}
}

// WithOfflineCheckInterval sets the interval between connectivity checks when
// the node is offline.
func WithOfflineCheckInterval(d time.Duration) Option {
	return func(cfg *config) error {
		if d <= 0 {
			return fmt.Errorf("offline check interval must be positive, got %s", d)
		}
		cfg.offlineCheckInterval = d
		return nil
	}
}
