package dht

import (
	"testing"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/stretchr/testify/require"
)

func newServerHost(t testing.TB) host.Host {
	listenAddr := libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0")

	h, err := libp2p.New(listenAddr)
	require.NoError(t, err)

	t.Cleanup(func() {
		if err = h.Close(); err != nil {
			t.Logf("unexpected error when closing host: %s", err)
		}
	})

	return h
}

func newClientHost(t testing.TB) host.Host {
	h, err := libp2p.New(libp2p.NoListenAddrs)
	require.NoError(t, err)

	t.Cleanup(func() {
		if err = h.Close(); err != nil {
			t.Logf("unexpected error when closing host: %s", err)
		}
	})

	return h
}

func newServerDht(t testing.TB, cfg *Config) *DHT {
	h := newServerHost(t)

	var err error
	if cfg == nil {
		cfg, err = DefaultConfig()
		require.NoError(t, err)
	}
	cfg.Mode = ModeOptServer

	d, err := New(h, cfg)
	require.NoError(t, err)

	t.Cleanup(func() {
		if err = d.Close(); err != nil {
			t.Logf("unexpected error when closing dht: %s", err)
		}
	})
	return d
}

func newClientDht(t testing.TB, cfg *Config) *DHT {
	h := newClientHost(t)

	var err error
	if cfg == nil {
		cfg, err = DefaultConfig()
		require.NoError(t, err)
	}
	cfg.Mode = ModeOptClient
	d, err := New(h, cfg)
	require.NoError(t, err)

	t.Cleanup(func() {
		if err = d.Close(); err != nil {
			t.Logf("unexpected error when closing dht: %s", err)
		}
	})
	return d
}
