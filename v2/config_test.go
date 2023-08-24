package dht

import (
	"testing"
	"time"
)

func TestConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*Config) *Config
		wantErr bool
	}{
		{
			name:    "happy path",
			wantErr: false,
			mutate:  func(c *Config) *Config { return c },
		},
		{
			name:    "invalid mode",
			wantErr: true,
			mutate: func(c *Config) *Config {
				c.Mode = "invalid"
				return c
			},
		},
		{
			name:    "nil Kademlia configuration",
			wantErr: true,
			mutate: func(c *Config) *Config {
				c.Kademlia = nil
				return c
			},
		},
		{
			name:    "invalid Kademlia configuration",
			wantErr: true,
			mutate: func(c *Config) *Config {
				c.Kademlia.Clock = nil
				return c
			},
		},
		{
			name:    "empty protocol",
			wantErr: true,
			mutate: func(c *Config) *Config {
				c.ProtocolID = ""
				return c
			},
		},
		{
			name:    "nil logger",
			wantErr: true,
			mutate: func(c *Config) *Config {
				c.Logger = nil
				return c
			},
		},
		{
			name:    "0 stream idle timeout",
			wantErr: true,
			mutate: func(c *Config) *Config {
				c.TimeoutStreamIdle = time.Duration(0)
				return c
			},
		},
		{
			name:    "negative stream idle timeout",
			wantErr: true,
			mutate: func(c *Config) *Config {
				c.TimeoutStreamIdle = time.Duration(-1)
				return c
			},
		},
		{
			// When we're using the IPFS protocol, we always require support
			// for ipns, pk, and provider records.
			// If the Backends map is empty and the IPFS protocol is configured,
			// we automatically populate the DHT backends for these record
			// types.
			name:    "incompatible backends with ipfs protocol",
			wantErr: true,
			mutate: func(c *Config) *Config {
				c.ProtocolID = ProtocolIPFS
				c.Backends["another"] = &RecordBackend{}
				return c
			},
		},
		{
			name:    "additional backends for ipfs protocol",
			wantErr: true,
			mutate: func(c *Config) *Config {
				c.ProtocolID = ProtocolIPFS
				c.Backends[namespaceProviders] = &RecordBackend{}
				c.Backends[namespaceIPNS] = &RecordBackend{}
				c.Backends[namespacePublicKey] = &RecordBackend{}
				c.Backends["another"] = &RecordBackend{}
				return c
			},
		},
		{
			name:    "nil address filter",
			wantErr: true,
			mutate: func(c *Config) *Config {
				c.AddressFilter = nil
				return c
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := DefaultConfig()
			c = tt.mutate(c)
			if err := c.Validate(); (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
