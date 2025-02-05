package net

import (
	"context"
	"testing"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"

	bhost "github.com/libp2p/go-libp2p/p2p/host/basic"
	swarmt "github.com/libp2p/go-libp2p/p2p/net/swarm/testing"

	"github.com/stretchr/testify/require"
)

func TestInvalidMessageSenderTracking(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	foo := peer.ID("asdasd")

	h, err := bhost.NewHost(swarmt.GenSwarm(t, swarmt.OptDisableReuseport), new(bhost.HostOpts))
	require.NoError(t, err)
	h.Start()
	defer h.Close()

	msgSender := NewMessageSenderImpl(h, []protocol.ID{"/test/kad/1.0.0"}).(*messageSenderImpl)

	_, err = msgSender.messageSenderForPeer(ctx, foo)
	require.Error(t, err, "should have failed to find message sender")

	msgSender.smlk.Lock()
	mscnt := len(msgSender.strmap)
	msgSender.smlk.Unlock()

	if mscnt > 0 {
		t.Fatal("should have no message senders in map")
	}
}
