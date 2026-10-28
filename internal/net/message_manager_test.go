package net

import (
	"bytes"
	"testing"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-msgio"

	bhost "github.com/libp2p/go-libp2p/p2p/host/basic"
	swarmt "github.com/libp2p/go-libp2p/p2p/net/swarm/testing"

	"github.com/stretchr/testify/require"

	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
)

func TestInvalidMessageSenderTracking(t *testing.T) {
	ctx := t.Context()

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

// TestSendRequestRejectsTooManyPeerEntries checks that a response carrying more
// peer entries than its frame can hold is rejected rather than decoded.
func TestSendRequestRejectsTooManyPeerEntries(t *testing.T) {
	const protoID = protocol.ID("/test/kad/1.0.0")
	newHost := func() *bhost.BasicHost {
		h, err := bhost.NewHost(swarmt.GenSwarm(t, swarmt.OptDisableReuseport), new(bhost.HostOpts))
		require.NoError(t, err)
		h.Start()
		t.Cleanup(func() { _ = h.Close() })
		return h
	}
	server, client := newHost(), newHost()

	emptyPeers := append([]byte{0x08, 0x04}, bytes.Repeat([]byte{0x42, 0x00}, 50_000)...)
	server.SetStreamHandler(protoID, func(s network.Stream) {
		defer func() { _ = s.Close() }()
		r := msgio.NewVarintReaderSize(s, network.MessageSizeMax)
		w := msgio.NewVarintWriter(s)
		for {
			msg, err := r.ReadMsg()
			if err != nil {
				return
			}
			r.ReleaseMsg(msg)
			if w.WriteMsg(emptyPeers) != nil {
				return
			}
		}
	})

	ctx := t.Context()
	require.NoError(t, client.Connect(ctx, peer.AddrInfo{ID: server.ID(), Addrs: server.Addrs()}))

	sender := NewMessageSenderImpl(client, []protocol.ID{protoID})
	_, err := sender.SendRequest(ctx, server.ID(), pb.NewMessage(pb.Message_FIND_NODE, []byte("key"), 0))
	require.ErrorIs(t, err, pb.ErrTooManyPeerEntries)
}
