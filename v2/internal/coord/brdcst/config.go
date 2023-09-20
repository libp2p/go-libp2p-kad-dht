package brdcst

import "github.com/plprobelab/go-kademlia/kad"

// BroadcastConfig specifies optional configuration for a Pool
type BroadcastConfig[K kad.Key[K]] struct{}

// Validate checks the configuration options and returns an error if any have invalid values.
func (cfg *BroadcastConfig[K]) Validate() error {
	return nil
}

// DefaultPoolConfig returns the default configuration options for a Pool.
// Options may be overridden before passing to NewPool
func DefaultPoolConfig[K kad.Key[K]]() *BroadcastConfig[K] {
	return &BroadcastConfig[K]{}
}
