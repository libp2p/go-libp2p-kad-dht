package dht

import (
	"context"
	"fmt"
	"runtime"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-msgio"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/libp2p/go-libp2p-kad-dht/v2/pb"
)

type testReadWriter struct {
	w msgio.WriteCloser
	r msgio.ReadCloser
}

func newTestReadWriter(s network.Stream) *testReadWriter {
	return &testReadWriter{
		w: msgio.NewVarintWriter(s),
		r: msgio.NewVarintReader(s),
	}
}

func (trw testReadWriter) ReadMsg() (*pb.Message, error) {
	msg, err := trw.r.ReadMsg()
	if err != nil {
		return nil, err
	}

	resp := &pb.Message{}
	return resp, proto.Unmarshal(msg, resp)
}

func (trw testReadWriter) WriteMsg(msg *pb.Message) error {
	data, err := proto.Marshal(msg)
	if err != nil {
		return err
	}

	return trw.w.WriteMsg(data)
}

func newPeerPair(t testing.TB) (host.Host, *DHT) {
	listenAddr := libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0")

	client, err := libp2p.New(listenAddr)
	require.NoError(t, err)

	server, err := libp2p.New(listenAddr)
	require.NoError(t, err)

	cfg, err := DefaultConfig()
	require.NoError(t, err)
	cfg.Mode = ModeOptServer
	serverDHT, err := New(server, cfg)

	fillRoutingTable(t, serverDHT, 250)

	t.Cleanup(func() {
		if err = serverDHT.Close(); err != nil {
			t.Logf("failed closing DHT: %s", err)
		}

		if err = client.Close(); err != nil {
			t.Logf("failed closing client host: %s", err)
		}

		if err = server.Close(); err != nil {
			t.Logf("failed closing client host: %s", err)
		}
	})

	ctx := context.Background()
	err = client.Connect(ctx, peer.AddrInfo{
		ID:    server.ID(),
		Addrs: server.Addrs(),
	})
	require.NoError(t, err)

	return client, serverDHT
}

func TestDHT_handleStream_find_node(t *testing.T) {
	ctx := context.Background()
	client, serverDHT := newPeerPair(t)

	s, err := client.NewStream(ctx, serverDHT.host.ID(), serverDHT.cfg.ProtocolID)
	require.NoError(t, err)

	trw := newTestReadWriter(s)

	req := &pb.Message{
		Type: pb.Message_FIND_NODE,
		Key:  []byte("random-key"),
	}

	err = trw.WriteMsg(req)
	require.NoError(t, err)

	resp, err := trw.ReadMsg()
	require.NoError(t, err)

	assert.Equal(t, pb.Message_FIND_NODE, resp.Type)
	assert.Equal(t, req.Key, resp.Key)
	assert.Len(t, resp.CloserPeers, 20)
	assert.Len(t, resp.ProviderPeers, 0)

	assert.NoError(t, s.Close())
}

func TestDHT_handleStream_unknown_message_type(t *testing.T) {
	ctx := context.Background()
	client, serverDHT := newPeerPair(t)

	s, err := client.NewStream(ctx, serverDHT.host.ID(), serverDHT.cfg.ProtocolID)
	require.NoError(t, err)

	trw := newTestReadWriter(s)

	req := &pb.Message{
		Type: pb.Message_MessageType(99),
		Key:  []byte("random-key"),
	}

	err = trw.WriteMsg(req)
	require.NoError(t, err)

	_, err = trw.ReadMsg()
	assert.ErrorIs(t, err, network.ErrReset)
}

func TestDHT_handleStream_supported_but_unregistered_message_type(t *testing.T) {
	ctx := context.Background()
	client, serverDHT := newPeerPair(t)

	// unregister providers
	delete(serverDHT.backends, namespaceProviders)

	s, err := client.NewStream(ctx, serverDHT.host.ID(), serverDHT.cfg.ProtocolID)
	require.NoError(t, err)

	trw := newTestReadWriter(s)

	req := &pb.Message{
		Type: pb.Message_GET_PROVIDERS,
		Key:  []byte(fmt.Sprintf("/%s/random-key", namespaceProviders)),
	}

	err = trw.WriteMsg(req)
	require.NoError(t, err)

	_, err = trw.ReadMsg()
	assert.ErrorIs(t, err, network.ErrReset)
}

func TestDHT_handleStream_reset_stream_without_message(t *testing.T) {
	ctx := context.Background()
	client, serverDHT := newPeerPair(t)

	s, err := client.NewStream(ctx, serverDHT.host.ID(), serverDHT.cfg.ProtocolID)
	require.NoError(t, err)

	err = s.Reset()
	require.NoError(t, err)
}

func TestDHT_handleStream_garbage_data(t *testing.T) {
	ctx := context.Background()
	client, serverDHT := newPeerPair(t)

	s, err := client.NewStream(ctx, serverDHT.host.ID(), serverDHT.cfg.ProtocolID)
	require.NoError(t, err)

	trw := newTestReadWriter(s)

	_, err = trw.w.Write([]byte("garbage-data"))
	require.NoError(t, err)

	_, err = trw.ReadMsg()
	assert.ErrorIs(t, err, network.ErrReset)
}

func TestDHT_handleStream_write_nil_data(t *testing.T) {
	ctx := context.Background()
	client, serverDHT := newPeerPair(t)

	s, err := client.NewStream(ctx, serverDHT.host.ID(), serverDHT.cfg.ProtocolID)
	require.NoError(t, err)

	trw := newTestReadWriter(s)

	_, err = trw.w.Write(nil)
	require.NoError(t, err)

	_, err = trw.ReadMsg()
	assert.ErrorIs(t, err, network.ErrReset)
}

func TestDHT_handleStream_graceful_close(t *testing.T) {
	ctx := context.Background()
	client, serverDHT := newPeerPair(t)

	s, err := client.NewStream(ctx, serverDHT.host.ID(), serverDHT.cfg.ProtocolID)
	require.NoError(t, err)

	err = s.Close()
	require.NoError(t, err)

	runtime.Gosched()
}
