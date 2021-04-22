package net

import (
	"context"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"
	swarmt "github.com/libp2p/go-libp2p-swarm/testing"
	bhost "github.com/libp2p/go-libp2p/p2p/host/basic"
	"testing"
)

func TestInvalidMessageSenderTracking(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	foo := peer.ID("asdasd")

	h := bhost.New(swarmt.GenSwarm(t, ctx, swarmt.OptDisableReuseport))

	msgSender := NewMessageSenderImpl(h, []protocol.ID{"/test/kad/1.0.0"}).(*messageSenderImpl)

	_, err := msgSender.messageSenderForPeer(ctx, foo)
	if err == nil {
		t.Fatal("that shouldnt have succeeded")
	}

	msgSender.smlk.Lock()
	mscnt := len(msgSender.strmap)
	msgSender.smlk.Unlock()

	if mscnt > 0 {
		t.Fatal("should have no message senders in map")
	}
}
