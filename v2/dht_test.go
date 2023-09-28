package dht

import (
	"sync"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/libp2p/go-libp2p-kad-dht/v2/internal/coord/coordt"
	"github.com/libp2p/go-libp2p-kad-dht/v2/internal/kadtest"
	"github.com/libp2p/go-libp2p-kad-dht/v2/kadt"
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

func TestAddAddresses(t *testing.T) {
	ctx := kadtest.CtxShort(t)

	top := NewTopology(t)
	local := top.AddClient(nil)
	remote := top.AddServer(nil)

	// local routing table should not contain the node
	_, err := local.kad.GetNode(ctx, kadt.PeerID(remote.host.ID()))
	require.ErrorIs(t, err, coordt.ErrNodeNotFound)

	remoteAddrInfo := peer.AddrInfo{
		ID:    remote.host.ID(),
		Addrs: remote.host.Addrs(),
	}
	require.NotEmpty(t, remoteAddrInfo.ID)
	require.NotEmpty(t, remoteAddrInfo.Addrs)

	// Add remote's addresss to the local dht
	err = local.AddAddresses(ctx, []peer.AddrInfo{remoteAddrInfo}, time.Minute)
	require.NoError(t, err)

	// the include state machine runs in the background and eventually should add the node to routing table
	_, err = top.ExpectRoutingUpdated(ctx, local, remote.host.ID())
	require.NoError(t, err)

	// the routing table should now contain the node
	_, err = local.kad.GetNode(ctx, kadt.PeerID(remote.host.ID()))
	require.NoError(t, err)
}

func TestDHT_Close_idempotent(t *testing.T) {
	d := newTestDHT(t)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			assert.NoError(t, d.Close())
			wg.Done()
		}()
	}
	wg.Wait()
}
