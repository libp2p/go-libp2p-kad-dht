package triert

import (
	"github.com/probe-lab/go-libdht/kad"
)

// Config holds configuration options for a TrieRT.
type Config[K kad.Key[K], N kad.NodeID[K]] struct {
	// KeyFilter defines the filter that is applied before a key is added to the
	// table. KeyFilter is applied before NodeFilter.
	// If nil, no filter is applied.
	KeyFilter KeyFilterFunc[K, N]
	// NodeFilter defines the filter that is applied before a node is added to
	// the table. NodeFilter is applied after KeyFilter.
	// If nil, no filter is applied.
	NodeFilter NodeFilter[K, N]
}

// DefaultConfig returns a default configuration for a TrieRT.
func DefaultConfig[K kad.Key[K], N kad.NodeID[K]]() *Config[K, N] {
	return &Config[K, N]{
		KeyFilter:  nil,
		NodeFilter: nil,
	}
}
