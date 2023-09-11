package dht

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/libp2p/go-libp2p-kad-dht/v2/coord"
	"github.com/libp2p/go-libp2p-kad-dht/v2/internal/kadtest"
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

// expectEventType selects on the event channel until an event of the expected type is sent.
func expectEventType(t *testing.T, ctx context.Context, events <-chan coord.RoutingNotification, expected coord.RoutingNotification) (coord.RoutingNotification, error) {
	t.Helper()
	for {
		select {
		case ev := <-events:
			t.Logf("saw event: %T\n", ev)
			if reflect.TypeOf(ev) == reflect.TypeOf(expected) {
				return ev, nil
			}
		case <-ctx.Done():
			return nil, fmt.Errorf("test deadline exceeded while waiting for event %T", expected)
		}
	}
}

func TestAddAddresses(t *testing.T) {
	ctx, cancel := kadtest.CtxShort(t)
	defer cancel()

	local := newClientDht(t, nil)

	remote := newServerDht(t, nil)

	// Populate entries in remote's routing table, so it passes a connectivity check
	fillRoutingTable(t, remote, 1)

	// local routing table should not contain the node
	_, err := local.kad.GetNode(ctx, remote.host.ID())
	require.ErrorIs(t, err, coord.ErrNodeNotFound)

	remoteAddrInfo := peer.AddrInfo{
		ID:    remote.host.ID(),
		Addrs: remote.host.Addrs(),
	}
	require.NotEmpty(t, remoteAddrInfo.ID)
	require.NotEmpty(t, remoteAddrInfo.Addrs)

	// Add remote's addresses to the local dht
	err = local.AddAddresses(ctx, []peer.AddrInfo{remoteAddrInfo}, time.Minute)
	require.NoError(t, err)

	// the include state machine runs in the background and eventually should add the node to the routing table
	_, err = expectEventType(t, ctx, local.kad.RoutingNotifications(), &coord.EventRoutingUpdated{})
	require.NoError(t, err)

	// the routing table should now contain the node
	_, err = local.kad.GetNode(ctx, remote.host.ID())
	require.NoError(t, err)
}
