package brdcst

import (
	"fmt"

	"github.com/libp2p/go-libp2p-kad-dht/v2/internal/coord/query"
)

// ConfigPool specifies the configuration for a broadcast [Pool].
type ConfigPool struct {
	pCfg *query.PoolConfig
}

// Validate checks the configuration options and returns an error if any have
// invalid values.
func (cfg *ConfigPool) Validate() error {
	if cfg.pCfg == nil {
		return fmt.Errorf("query pool config must not be nil")
	}

	return nil
}

// DefaultConfigPool returns the default configuration options for a Pool.
// Options may be overridden before passing to NewPool
func DefaultConfigPool() *ConfigPool {
	return &ConfigPool{
		pCfg: query.DefaultPoolConfig(),
	}
}

// Config is an interface that all broadcast configurations must implement.
// Because we have multiple ways of broadcasting records to the network, like
// [FollowUp] or [Static], the [EventPoolStartBroadcast] has a configuration
// field that depending on the concrete type of [Config] initializes the
// respective state machine. Then the broadcast operation will be performed
// based on the encoded rules in that state machine.
type Config interface {
	broadcastConfig()
}

func (c *ConfigFollowUp) broadcastConfig()   {}
func (c *ConfigOptimistic) broadcastConfig() {}
func (c *ConfigStatic) broadcastConfig()     {}

// ConfigFollowUp specifies the configuration for the [FollowUp] state machine.
type ConfigFollowUp struct{}

// Validate checks the configuration options and returns an error if any have
// invalid values.
func (c *ConfigFollowUp) Validate() error {
	return nil
}

// DefaultConfigFollowUp returns the default configuration options for the
// [FollowUp] state machine.
func DefaultConfigFollowUp() *ConfigFollowUp {
	return &ConfigFollowUp{}
}

// ConfigOptimistic specifies the configuration for the [Optimistic] state
// machine.
type ConfigOptimistic struct{}

// Validate checks the configuration options and returns an error if any have
// invalid values.
func (c *ConfigOptimistic) Validate() error {
	return nil
}

// DefaultConfigOptimistic returns the default configuration options for the
// [Optimistic] state machine.
func DefaultConfigOptimistic() *ConfigOptimistic {
	return &ConfigOptimistic{}
}

// ConfigStatic specifies the configuration for the [Static] state
// machine.
type ConfigStatic struct{}

// Validate checks the configuration options and returns an error if any have
// invalid values.
func (c *ConfigStatic) Validate() error {
	return nil
}

// DefaultConfigStatic returns the default configuration options for the
// [Static] state machine.
func DefaultConfigStatic() *ConfigStatic {
	return &ConfigStatic{}
}
