package dht

import (
	"context"
	"net"
	"sync/atomic"
	"testing"

	ic "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
)

func TestIsRelay(t *testing.T) {
	a, _ := ma.NewMultiaddr("/ip4/127.0.0.1/tcp/5002/p2p/QmdPU7PfRyKehdrP5A3WqmjyD6bhVpU1mLGKppa2FjGDjZ/p2p-circuit/p2p/QmVT6GYwjeeAF5TR485Yc58S3xRF5EFsZ5YAF4VcP3URHt")
	if !isRelayAddr(a) {
		t.Fatalf("thought %s was not a relay", a)
	}
	a, _ = ma.NewMultiaddr("/p2p-circuit/p2p/QmVT6GYwjeeAF5TR485Yc58S3xRF5EFsZ5YAF4VcP3URHt")
	if !isRelayAddr(a) {
		t.Fatalf("thought %s was not a relay", a)
	}
	a, _ = ma.NewMultiaddr("/ip4/127.0.0.1/tcp/5002/p2p/QmdPU7PfRyKehdrP5A3WqmjyD6bhVpU1mLGKppa2FjGDjZ")
	if isRelayAddr(a) {
		t.Fatalf("thought %s was a relay", a)
	}

}

type mockConn struct {
	local  peer.AddrInfo
	remote peer.AddrInfo

	isClosed atomic.Bool
}

var _ network.Conn = (*mockConn)(nil)

func (m *mockConn) ID() string { return "0" }
func (m *mockConn) Close() error {
	m.isClosed.Store(true)
	return nil
}
func (m *mockConn) NewStream(context.Context) (network.Stream, error) { return nil, nil }
func (m *mockConn) GetStreams() []network.Stream                      { return []network.Stream{} }
func (m *mockConn) Stat() network.ConnStats {
	return network.ConnStats{Stats: network.Stats{Direction: network.DirOutbound}}
}
func (m *mockConn) Scope() network.ConnScope           { return &network.NullScope{} }
func (m *mockConn) LocalMultiaddr() ma.Multiaddr       { return m.local.Addrs[0] }
func (m *mockConn) RemoteMultiaddr() ma.Multiaddr      { return m.remote.Addrs[0] }
func (m *mockConn) LocalPeer() peer.ID                 { return m.local.ID }
func (m *mockConn) LocalPrivateKey() ic.PrivKey        { return nil }
func (m *mockConn) RemotePeer() peer.ID                { return m.remote.ID }
func (m *mockConn) RemotePublicKey() ic.PubKey         { return nil }
func (m *mockConn) ConnState() network.ConnectionState { return network.ConnectionState{} }
func (m *mockConn) IsClosed() bool                     { return m.isClosed.Load() }

func TestFilterCaching(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	d := setupDHT(ctx, t, true)

	remote, _ := manet.FromIP(net.IPv4(8, 8, 8, 8))
	if privRTFilter(d, []network.Conn{&mockConn{
		local:  d.Host().Peerstore().PeerInfo(d.Host().ID()),
		remote: peer.AddrInfo{ID: "", Addrs: []ma.Multiaddr{remote}},
	}}) {
		t.Fatal("filter should prevent public remote peers.")
	}

	r1 := getCachedRouter()
	r2 := getCachedRouter()
	if r1 != r2 {
		t.Fatal("router should be returned multiple times.")
	}
}
