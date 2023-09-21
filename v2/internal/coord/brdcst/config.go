package brdcst

// BroadcastConfig specifies optional configuration for a Pool
type BroadcastConfig struct{}

// Validate checks the configuration options and returns an error if any have invalid values.
func (cfg *BroadcastConfig) Validate() error {
	return nil
}

// DefaultPoolConfig returns the default configuration options for a Pool.
// Options may be overridden before passing to NewPool
func DefaultPoolConfig() *BroadcastConfig {
	return &BroadcastConfig{}
}
