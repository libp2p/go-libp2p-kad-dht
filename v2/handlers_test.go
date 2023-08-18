package dht

import (
	"context"
	"fmt"
	"math/rand"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/ipfs/boxo/ipns"
	"github.com/ipfs/boxo/path"
	ds "github.com/ipfs/go-datastore"
	"github.com/libp2p/go-libp2p"
	pb "github.com/libp2p/go-libp2p-kad-dht/v2/pb"
	recpb "github.com/libp2p/go-libp2p-record/pb"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testPath = path.Path("/ipfs/bafkqac3jobxhgidsn5rww4yk")
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
	id, _ := newIdentity(t)
	return id
}

func newIdentity(t testing.TB) (peer.ID, crypto.PrivKey) {
	priv, pub, err := crypto.GenerateEd25519Key(rng)
	require.NoError(t, err)

	id, err := peer.IDFromPublicKey(pub)
	require.NoError(t, err)

	return id, priv
}

func mustUnmarshalIpnsRecord(t *testing.T, data []byte) *ipns.Record {
	r := &recpb.Record{}
	err := r.Unmarshal(data)
	require.NoError(t, err)

	rec, err := ipns.UnmarshalRecord(r.Value)
	require.NoError(t, err)

	return rec
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

func newIPNSRequest(t testing.TB, priv crypto.PrivKey, seq uint64, eol time.Time, ttl time.Duration) *pb.Message {
	rec, err := ipns.NewRecord(priv, testPath, seq, eol, ttl)
	require.NoError(t, err)

	remote, err := peer.IDFromPublicKey(priv.GetPublic())
	require.NoError(t, err)

	data, err := ipns.MarshalRecord(rec)
	require.NoError(t, err)

	key := ipns.NameFromPeer(remote).RoutingKey()
	req := &pb.Message{
		Type: pb.Message_PUT_VALUE,
		Key:  key,
		Record: &recpb.Record{
			Key:   key,
			Value: data,
		},
	}

	return req
}

func BenchmarkDHT_handlePutValue_unique_peers(b *testing.B) {
	d := newTestDHT(b)

	// build requests
	peers := make([]peer.ID, b.N)
	reqs := make([]*pb.Message, b.N)
	for i := 0; i < b.N; i++ {
		remote, priv := newIdentity(b)
		peers[i] = remote
		reqs[i] = newIPNSRequest(b, priv, uint64(i), time.Now().Add(time.Hour), time.Hour)
	}

	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := d.handlePutValue(ctx, peers[i], reqs[i])
		if err != nil {
			b.Error(err)
		}
	}
}

func BenchmarkDHT_handlePutValue_single_peer(b *testing.B) {
	d := newTestDHT(b)

	// build requests
	remote, priv := newIdentity(b)
	reqs := make([]*pb.Message, b.N)
	for i := 0; i < b.N; i++ {
		reqs[i] = newIPNSRequest(b, priv, uint64(i), time.Now().Add(time.Hour), time.Hour)
	}

	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := d.handlePutValue(ctx, remote, reqs[i])
		if err != nil {
			b.Error(err)
		}
	}
}

func TestDHT_handlePutValue_happy_path_ipns_record(t *testing.T) {
	d := newTestDHT(t)

	remote, priv := newIdentity(t)

	// expired record
	req := newIPNSRequest(t, priv, 0, time.Now().Add(time.Hour), time.Hour)

	ctx := context.Background()
	_, err := d.ds.Get(ctx, datastoreKey(req.Key))
	require.ErrorIs(t, err, ds.ErrNotFound)

	cloned := proto.Clone(req).(*pb.Message)
	_, err = d.handlePutValue(ctx, remote, cloned)
	require.NoError(t, err)

	dat, err := d.ds.Get(ctx, datastoreKey(req.Key))
	require.NoError(t, err)

	r := &recpb.Record{}
	err = r.Unmarshal(dat)
	require.NoError(t, err)

	assert.NotEqual(t, r.TimeReceived, req.Record.TimeReceived)

	r.TimeReceived = ""
	req.Record.TimeReceived = ""

	assert.True(t, reflect.DeepEqual(r, req.Record))
}

func TestDHT_handlePutValue_nil_record(t *testing.T) {
	d := newTestDHT(t)

	req := &pb.Message{
		Type:   pb.Message_PUT_VALUE,
		Key:    []byte("random-key"),
		Record: nil, // nil record
	}

	resp, err := d.handlePutValue(context.Background(), newPeerID(t), req)
	assert.Error(t, err)
	assert.Nil(t, resp)
	assert.ErrorContains(t, err, "nil record")
}

func TestDHT_handlePutValue_record_key_mismatch(t *testing.T) {
	d := newTestDHT(t)

	req := &pb.Message{
		Type: pb.Message_PUT_VALUE,
		Key:  []byte("key-1"),
		Record: &recpb.Record{
			Key: []byte("key-2"),
		},
	}

	resp, err := d.handlePutValue(context.Background(), newPeerID(t), req)
	assert.Error(t, err)
	assert.Nil(t, resp)
	assert.ErrorContains(t, err, "key doesn't match record key")
}

func TestDHT_handlePutValue_bad_ipns_record(t *testing.T) {
	d := newTestDHT(t)

	remote, priv := newIdentity(t)

	// expired record
	req := newIPNSRequest(t, priv, 10, time.Now().Add(-time.Hour), -time.Hour)

	resp, err := d.handlePutValue(context.Background(), remote, req)
	assert.Error(t, err)
	assert.Nil(t, resp)
	assert.ErrorContains(t, err, "bad record")
}

func TestDHT_handlePutValue_worse_ipns_record_after_first_put(t *testing.T) {
	d := newTestDHT(t)

	remote, priv := newIdentity(t)

	goodReq := newIPNSRequest(t, priv, 10, time.Now().Add(time.Hour), time.Hour)
	worseReq := newIPNSRequest(t, priv, 0, time.Now().Add(time.Hour), time.Hour)

	for i, req := range []*pb.Message{goodReq, worseReq} {

		resp, err := d.handlePutValue(context.Background(), remote, req)
		switch i {
		case 0:
			assert.NoError(t, err)
			assert.NotNil(t, resp)
		case 1:
			assert.Error(t, err)
			assert.Nil(t, resp)
			assert.ErrorContains(t, err, "received worse record")
		}
	}
}

func TestDHT_handlePutValue_probe_race_condition(t *testing.T) {
	// we're storing two sequential records simultaneously many times in a row.
	// After each insert, we check that indeed the record with the higher
	// sequence number was stored. If the handler didn't use transactions,
	// this test fails.

	d := newTestDHT(t)

	remote, priv := newIdentity(t)

	ipnsKey := ipns.NameFromPeer(remote).RoutingKey()

	for i := 0; i < 100; i++ {

		req1 := newIPNSRequest(t, priv, uint64(2*i), time.Now().Add(time.Hour), time.Hour)
		req2 := newIPNSRequest(t, priv, uint64(2*i+1), time.Now().Add(time.Hour), time.Hour)

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			_, _ = d.handlePutValue(context.Background(), remote, req1)
			wg.Done()
		}()

		wg.Add(1)
		go func() {
			_, err := d.handlePutValue(context.Background(), remote, req2)
			assert.NoError(t, err)
			wg.Done()
		}()
		wg.Wait()

		dat, err := d.ds.Get(context.Background(), datastoreKey(ipnsKey))
		require.NoError(t, err)

		storedRec := mustUnmarshalIpnsRecord(t, dat)

		seq, err := storedRec.Sequence()
		require.NoError(t, err)

		// stored record must always be the newer one!
		assert.EqualValues(t, 2*i+1, seq)
	}
}

func TestDHT_handlePutValue_overwrites_corrupt_stored_ipns_record(t *testing.T) {
	d := newTestDHT(t)

	remote, priv := newIdentity(t)

	req := newIPNSRequest(t, priv, 10, time.Now().Add(time.Hour), time.Hour)

	// store corrupt record
	err := d.ds.Put(context.Background(), datastoreKey(req.Record.GetKey()), []byte("corrupt-record"))
	require.NoError(t, err)

	// put the correct record through handler
	_, err = d.handlePutValue(context.Background(), remote, req)
	require.NoError(t, err)

	// check if the corrupt record was overwritten
	dat, err := d.ds.Get(context.Background(), datastoreKey(req.Record.GetKey()))
	require.NoError(t, err)

	mustUnmarshalIpnsRecord(t, dat)
}
