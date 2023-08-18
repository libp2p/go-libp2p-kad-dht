package dht

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	pb "github.com/libp2p/go-libp2p-kad-dht/v2/pb"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var rng = rand.New(rand.NewSource(150))

func newTestDHT(t testing.TB) *DHT {
	t.Helper()

	h, err := libp2p.New(libp2p.NoListenAddrs)
	if err != nil {
		t.Fatalf("new libp2p host: %s", err)
	}

	d, err := New(h, DefaultConfig())
	if err != nil {
		t.Fatalf("new dht: %s", err)
	}

	t.Cleanup(func() {
		if err = d.Close(); err != nil {
			t.Logf("closing dht: %s", err)
		}

		if err = h.Close(); err != nil {
			t.Logf("closing host: %s", err)
		}
	})

	return d
}

func newPeerID(t testing.TB) peer.ID {
	_, pub, err := crypto.GenerateEd25519Key(rng)
	require.NoError(t, err)
	id, err := peer.IDFromPublicKey(pub)
	require.NoError(t, err)
	return id
}

func TestMessage_noKey(t *testing.T) {
	d := newTestDHT(t)
	for _, typ := range []pb.Message_MessageType{
		pb.Message_FIND_NODE,
		pb.Message_PUT_VALUE,
		pb.Message_GET_VALUE,
		pb.Message_ADD_PROVIDER,
		pb.Message_GET_PROVIDERS,
	} {
		t.Run(fmt.Sprintf("%s", typ), func(t *testing.T) {
			msg := &pb.Message{Type: typ} // no key
			_, err := d.handleMsg(context.Background(), peer.ID(""), msg)
			if err == nil {
				t.Error("expected processing message to fail")
			}
		})
	}
}

func BenchmarkDHT_handleFindPeer(b *testing.B) {
	d := newTestDHT(b)

	// build routing table
	var peers []peer.ID
	for i := 0; i < 250; i++ {

		// generate peer ID
		pid := newPeerID(b)

		// add peer to routing table
		d.rt.AddNode(nodeID(pid))

		// keep track of peer
		peers = append(peers, pid)

		// craft network address for peer
		a, err := ma.NewMultiaddr(fmt.Sprintf("/ip4/127.0.0.1/tcp/%d", 2000+i))
		if err != nil {
			b.Fatal(err)
		}

		// add peer information to peer store
		d.host.Peerstore().AddAddr(pid, a, time.Hour)
	}

	// build requests
	reqs := make([]*pb.Message, b.N)
	for i := 0; i < b.N; i++ {
		reqs[i] = &pb.Message{
			Key: []byte("random-key-" + strconv.Itoa(i)),
		}
	}

	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := d.handleFindPeer(ctx, peers[0], reqs[i])
		if err != nil {
			b.Error(err)
		}
	}
}

func TestDHT_handleFindPeer_happy_path(t *testing.T) {
	d := newTestDHT(t)

	// build routing table
	peers := make([]peer.ID, 250)
	for i := 0; i < 250; i++ {
		// generate peer ID
		pid := newPeerID(t)

		// add peer to routing table but don't add first peer. The first peer
		// will be the one who's making the request below. If we added it to
		// the routing table it could be among the closest peers to the random
		// key below. We filter out the requesting peer from the response of
		// closer peers. This means we can't assert for exactly 20 closer peers
		// below.
		if i > 0 {
			d.rt.AddNode(nodeID(pid))
		}

		// keep track of peer
		peers[i] = pid

		// craft network address for peer
		a, err := ma.NewMultiaddr(fmt.Sprintf("/ip4/127.0.0.1/tcp/%d", 2000+i))
		require.NoError(t, err)

		// add peer information to peer store
		d.host.Peerstore().AddAddr(pid, a, time.Hour)
	}

	req := &pb.Message{
		Type: pb.Message_FIND_NODE,
		Key:  []byte("random-key"),
	}

	resp, err := d.handleFindPeer(context.Background(), peers[0], req)
	require.NoError(t, err)

	assert.Equal(t, pb.Message_FIND_NODE, resp.Type)
	assert.Nil(t, resp.Record)
	assert.Equal(t, req.Key, resp.Key)
	assert.Len(t, resp.CloserPeers, d.cfg.BucketSize)
	assert.Len(t, resp.ProviderPeers, 0)
	assert.Equal(t, len(resp.CloserPeers[0].Addrs), 1)
}

func TestDHT_handleFindPeer_self_in_routing_table(t *testing.T) {
	// a case that shouldn't happen
	d := newTestDHT(t)

	d.rt.AddNode(nodeID(d.host.ID()))

	req := &pb.Message{
		Type: pb.Message_FIND_NODE,
		Key:  []byte("random-key"),
	}

	resp, err := d.handleFindPeer(context.Background(), newPeerID(t), req)
	require.NoError(t, err)

	assert.Equal(t, pb.Message_FIND_NODE, resp.Type)
	assert.Nil(t, resp.Record)
	assert.Equal(t, req.Key, resp.Key)
	assert.Len(t, resp.CloserPeers, 0)
	assert.Len(t, resp.ProviderPeers, 0)
}

func TestDHT_handleFindPeer_empty_routing_table(t *testing.T) {
	d := newTestDHT(t)

	req := &pb.Message{
		Type: pb.Message_FIND_NODE,
		Key:  []byte("random-key"),
	}

	resp, err := d.handleFindPeer(context.Background(), newPeerID(t), req)
	require.NoError(t, err)

	assert.Equal(t, pb.Message_FIND_NODE, resp.Type)
	assert.Nil(t, resp.Record)
	assert.Equal(t, req.Key, resp.Key)
	assert.Len(t, resp.CloserPeers, 0)
	assert.Len(t, resp.ProviderPeers, 0)
}

func TestDHT_handleFindPeer_unknown_addresses_but_in_routing_table(t *testing.T) {
	d := newTestDHT(t)

	// build routing table
	peers := make([]peer.ID, d.cfg.BucketSize)
	for i := 0; i < d.cfg.BucketSize; i++ {
		// generate peer ID
		pid := newPeerID(t)

		// add peer to routing table
		d.rt.AddNode(nodeID(pid))

		// keep track of peer
		peers[i] = pid

		if i == 0 {
			continue
		}

		// craft network address for peer
		a, err := ma.NewMultiaddr(fmt.Sprintf("/ip4/127.0.0.1/tcp/%d", 2000+i))
		require.NoError(t, err)

		// add peer information to peer store
		d.host.Peerstore().AddAddr(pid, a, time.Hour)
	}

	_, pubk, _ := crypto.GenerateEd25519Key(rng)
	requester, err := peer.IDFromPublicKey(pubk)
	require.NoError(t, err)

	req := &pb.Message{
		Type: pb.Message_FIND_NODE,
		Key:  []byte("random-key"),
	}

	resp, err := d.handleFindPeer(context.Background(), requester, req)
	require.NoError(t, err)

	assert.Equal(t, pb.Message_FIND_NODE, resp.Type)
	assert.Nil(t, resp.Record)
	assert.Equal(t, req.Key, resp.Key)
	assert.Len(t, resp.ProviderPeers, 0)
	// should not return peer whose addresses we don't know
	require.Len(t, resp.CloserPeers, d.cfg.BucketSize-1)
	for _, cp := range resp.CloserPeers {
		assert.NotEqual(t, peer.ID(cp.Id), peers[0])
	}
}

func TestDHT_handleFindPeer_request_for_server(t *testing.T) {
	d := newTestDHT(t)

	req := &pb.Message{
		Type: pb.Message_FIND_NODE,
		Key:  []byte(d.host.ID()),
	}

	resp, err := d.handleFindPeer(context.Background(), newPeerID(t), req)
	require.NoError(t, err)

	assert.Equal(t, pb.Message_FIND_NODE, resp.Type)
	assert.Nil(t, resp.Record)
	assert.Equal(t, req.Key, resp.Key)
	assert.Len(t, resp.CloserPeers, 1)
	assert.Len(t, resp.ProviderPeers, 0)
	assert.Equal(t, d.host.ID(), peer.ID(resp.CloserPeers[0].Id))
}

func TestDHT_handleFindPeer_request_for_self(t *testing.T) {
	d := newTestDHT(t)

	// build routing table
	peers := make([]peer.ID, d.cfg.BucketSize)
	for i := 0; i < d.cfg.BucketSize; i++ {
		// generate peer ID
		pid := newPeerID(t)

		// add peer to routing table
		d.rt.AddNode(nodeID(pid))

		// keep track of peer
		peers[i] = pid

		// craft network address for peer
		a, err := ma.NewMultiaddr(fmt.Sprintf("/ip4/127.0.0.1/tcp/%d", 2000+i))
		if err != nil {
			t.Fatal(err)
		}

		// add peer information to peer store
		d.host.Peerstore().AddAddr(pid, a, time.Hour)
	}

	req := &pb.Message{
		Type: pb.Message_FIND_NODE,
		Key:  []byte("random-key"),
	}

	resp, err := d.handleFindPeer(context.Background(), peers[0], req)
	require.NoError(t, err)

	assert.Equal(t, pb.Message_FIND_NODE, resp.Type)
	assert.Nil(t, resp.Record)
	assert.Equal(t, req.Key, resp.Key)
	assert.Len(t, resp.CloserPeers, d.cfg.BucketSize-1) // don't return requester
	assert.Len(t, resp.ProviderPeers, 0)
}

func TestDHT_handleFindPeer_request_for_known_but_far_peer(t *testing.T) {
	// tests if a peer that we know the addresses of but that isn't in
	// the routing table is returned
	d := newTestDHT(t)

	// build routing table
	peers := make([]peer.ID, 250)
	for i := 0; i < 250; i++ {
		// generate peer ID
		pid := newPeerID(t)

		// keep track of peer
		peers[i] = pid

		// craft network address for peer
		a, err := ma.NewMultiaddr(fmt.Sprintf("/ip4/127.0.0.1/tcp/%d", 2000+i))
		if err != nil {
			t.Fatal(err)
		}

		// add peer information to peer store
		d.host.Peerstore().AddAddr(pid, a, time.Hour)

		// don't add first peer to routing table -> the one we're asking for
		// don't add second peer -> the one that's requesting
		if i > 1 {
			d.rt.AddNode(nodeID(pid))
		}
	}

	req := &pb.Message{
		Type: pb.Message_FIND_NODE,
		Key:  []byte(peers[0]), // not in routing table but in peer store
	}

	resp, err := d.handleFindPeer(context.Background(), peers[1], req)
	require.NoError(t, err)

	assert.Equal(t, pb.Message_FIND_NODE, resp.Type)
	assert.Nil(t, resp.Record)
	assert.Equal(t, req.Key, resp.Key)
	assert.Len(t, resp.CloserPeers, d.cfg.BucketSize+1) // return peer because we know it's addresses
	assert.Len(t, resp.ProviderPeers, 0)
}

func TestDHT_handlePing(t *testing.T) {
	d := newTestDHT(t)

	req := &pb.Message{Type: pb.Message_PING}
	res, err := d.handlePing(context.Background(), newPeerID(t), req)
	require.NoError(t, err)
	assert.Equal(t, pb.Message_PING, res.Type)
	assert.Len(t, res.CloserPeers, 0)
	assert.Len(t, res.ProviderPeers, 0)
	assert.Nil(t, res.Key)
	assert.Nil(t, res.Record)
}

func BenchmarkDHT_handlePing(b *testing.B) {
	d := newTestDHT(b)
	requester := newPeerID(b)

	ctx := context.Background()
	req := &pb.Message{Type: pb.Message_PING}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := d.handlePing(ctx, requester, req)
		if err != nil {
			b.Error(err)
		}
	}
}
