package dht

import (
	"context"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/stretchr/testify/require"

	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
)

func TestInvalidRemotePeers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mn, err := mocknet.FullMeshLinked(5)
	if err != nil {
		t.Fatal(err)
	}
	defer mn.Close()
	hosts := mn.Hosts()

	os := []Option{testPrefix, DisableAutoRefresh(), Mode(ModeServer)}
	d, err := New(ctx, hosts[0], os...)
	if err != nil {
		t.Fatal(err)
	}
	for _, proto := range d.serverProtocols {
		// Hang on every request.
		hosts[1].SetStreamHandler(proto, func(s network.Stream) {
			defer s.Reset() // nolint
			<-ctx.Done()
		})
	}

	err = mn.ConnectAllButSelf()
	if err != nil {
		t.Fatal("failed to connect peers", err)
	}

	time.Sleep(100 * time.Millisecond)

	// hosts[1] isn't added to the routing table because it isn't responding to
	// the DHT request
	require.Equal(t, 0, d.routingTable.Size())
}
