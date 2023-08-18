package dht

import (
	"testing"

	"github.com/libp2p/go-libp2p"
	"github.com/stretchr/testify/assert"
)

func TestNew(t *testing.T) {
	h, err := libp2p.New(libp2p.NoListenAddrs)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name        string
		cfgBuilder  func(*Config) *Config
		wantBuilder func(*DHT) *DHT
		wantErr     bool
	}{
		{
			name: "mode set to server",
			cfgBuilder: func(c *Config) *Config {
				c.Mode = ModeOptServer
				return c
			},
			wantBuilder: func(dht *DHT) *DHT {
				dht.mode = modeServer
				return dht
			},
			wantErr: false,
		},
		{
			name: "mode set to auto client",
			cfgBuilder: func(c *Config) *Config {
				c.Mode = ModeOptAutoClient
				return c
			},
			wantBuilder: func(dht *DHT) *DHT {
				dht.mode = modeClient
				return dht
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := DefaultConfig()
			d, err := New(h, c)
			if err != nil {
				t.Fatal(err)
			}

			got, err := New(h, tt.cfgBuilder(c))
			if (err != nil) != tt.wantErr {
				t.Errorf("New() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			want := tt.wantBuilder(d)

			assert.Equal(t, want.mode, got.mode)
		})
	}
}
