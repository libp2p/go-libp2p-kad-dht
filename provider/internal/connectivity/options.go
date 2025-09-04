package connectivity

import (
	"fmt"
	"time"
)

type config struct {
	onlineCheckInterval time.Duration // minimum check interval when online

	offlineDelay time.Duration

	onOffline func()
	onOnline  func()
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
	cfg.onlineCheckInterval = 1 * time.Minute
	cfg.offlineDelay = 2 * time.Hour
	return nil
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

func WithOfflineDelay(d time.Duration) Option {
	return func(cfg *config) error {
		if d < 0 {
			return fmt.Errorf("offline delay must be non-negative, got %s", d)
		}
		cfg.offlineDelay = d
		return nil
	}
}

func WithOnOffline(f func()) Option {
	return func(cfg *config) error {
		cfg.onOffline = f
		return nil
	}
}

func WithOnOnline(f func()) Option {
	return func(cfg *config) error {
		cfg.onOnline = f
		return nil
	}
}
