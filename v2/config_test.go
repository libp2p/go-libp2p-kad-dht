package dht

import (
	"testing"
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
