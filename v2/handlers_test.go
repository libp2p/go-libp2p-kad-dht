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
	record "github.com/libp2p/go-libp2p-record"
	recpb "github.com/libp2p/go-libp2p-record/pb"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var rng = rand.New(rand.NewSource(1337))

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

func fillRoutingTable(t testing.TB, d *DHT) {
	// 250 is a common number of peers to have in the routing table
	for i := 0; i < 250; i++ {
		// generate peer ID
		pid := newPeerID(t)

		// add peer to routing table
		d.rt.AddNode(nodeID(pid))

		// craft network address for peer
		a, err := ma.NewMultiaddr(fmt.Sprintf("/ip4/127.0.0.1/tcp/%d", 2000+i))
		require.NoError(t, err)

		// add peer information to peer store
		d.host.Peerstore().AddAddr(pid, a, time.Hour)
	}
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
			Type: pb.Message_FIND_NODE,
			Key:  []byte("random-key-" + strconv.Itoa(i)),
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

	_, pub, _ := crypto.GenerateEd25519Key(rng)
	requester, err := peer.IDFromPublicKey(pub)
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

func newPutIPNSRequest(t testing.TB, priv crypto.PrivKey, seq uint64, eol time.Time, ttl time.Duration) *pb.Message {
	testPath := path.Path("/ipfs/bafkqac3jobxhgidsn5rww4yk")

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
			Key:          key,
			Value:        data,
			TimeReceived: time.Now().Format(time.RFC3339Nano),
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
		reqs[i] = newPutIPNSRequest(b, priv, uint64(i), time.Now().Add(time.Hour), time.Hour)
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
		reqs[i] = newPutIPNSRequest(b, priv, uint64(i), time.Now().Add(time.Hour), time.Hour)
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
	ctx := context.Background()

	// init new DHT
	d := newTestDHT(t)

	// generate new identity for the peer that issues the request
	remote, priv := newIdentity(t)

	// expired record
	req := newPutIPNSRequest(t, priv, 0, time.Now().Add(time.Hour), time.Hour)
	ns, suffix, err := record.SplitKey(string(req.Key))
	require.NoError(t, err)

	_, err = d.backends[ns].Fetch(ctx, suffix)
	require.ErrorIs(t, err, ds.ErrNotFound)

	cloned := proto.Clone(req).(*pb.Message)
	_, err = d.handlePutValue(ctx, remote, cloned)
	require.NoError(t, err)

	dat, err := d.backends[ns].Fetch(ctx, suffix)
	require.NoError(t, err)

	r, ok := dat.(*recpb.Record)
	require.True(t, ok)

	assert.NotEqual(t, r.TimeReceived, req.Record.TimeReceived)

	r.TimeReceived = ""
	req.Record.TimeReceived = ""

	assert.True(t, reflect.DeepEqual(r, req.Record))
}

func TestDHT_handlePutValue_nil_records(t *testing.T) {
	d := newTestDHT(t)

	for _, ns := range []string{namespaceIPNS, namespacePublicKey} {
		req := &pb.Message{
			Type:   pb.Message_PUT_VALUE,
			Key:    []byte(fmt.Sprintf("/%s/random-key", ns)),
			Record: nil, // nil record
		}

		resp, err := d.handlePutValue(context.Background(), newPeerID(t), req)
		assert.Error(t, err)
		assert.Nil(t, resp)
		assert.ErrorContains(t, err, "nil record")
	}
}

func TestDHT_handlePutValue_record_key_mismatch(t *testing.T) {
	d := newTestDHT(t)

	for _, ns := range []string{namespaceIPNS, namespacePublicKey} {
		t.Run(ns, func(t *testing.T) {
			key1 := []byte(fmt.Sprintf("/%s/key-1", ns))
			key2 := []byte(fmt.Sprintf("/%s/key-2", ns))

			req := &pb.Message{
				Type: pb.Message_PUT_VALUE,
				Key:  key1,
				Record: &recpb.Record{
					Key: key2,
				},
			}

			resp, err := d.handlePutValue(context.Background(), newPeerID(t), req)
			assert.Error(t, err)
			assert.Nil(t, resp)
			assert.ErrorContains(t, err, "key doesn't match record key")
		})
	}
}

func TestDHT_handlePutValue_bad_ipns_record(t *testing.T) {
	d := newTestDHT(t)

	remote, priv := newIdentity(t)

	// expired record
	req := newPutIPNSRequest(t, priv, 10, time.Now().Add(-time.Hour), -time.Hour)

	resp, err := d.handlePutValue(context.Background(), remote, req)
	assert.Error(t, err)
	assert.Nil(t, resp)
	assert.ErrorContains(t, err, "bad record")
}

func TestDHT_handlePutValue_worse_ipns_record_after_first_put(t *testing.T) {
	d := newTestDHT(t)

	remote, priv := newIdentity(t)

	goodReq := newPutIPNSRequest(t, priv, 10, time.Now().Add(time.Hour), time.Hour)
	worseReq := newPutIPNSRequest(t, priv, 0, time.Now().Add(time.Hour), time.Hour)

	for i, req := range []*pb.Message{goodReq, worseReq} {
		resp, err := d.handlePutValue(context.Background(), remote, req)
		switch i {
		case 0:
			assert.NoError(t, err)
			assert.Nil(t, resp)
		case 1:
			assert.Error(t, err)
			assert.Nil(t, resp)
			assert.ErrorContains(t, err, "received worse record")
		}
	}
}

func TestDHT_handlePutValue_probe_race_condition(t *testing.T) {
	// we're storing two sequential records simultaneously many times in a row.
	// After each insert, we check that indeed, the record with the higher
	// sequence number was stored. If the handler didn't use transactions,
	// this test fails.

	d := newTestDHT(t)

	remote, priv := newIdentity(t)

	for i := 0; i < 100; i++ {

		req1 := newPutIPNSRequest(t, priv, uint64(2*i), time.Now().Add(time.Hour), time.Hour)
		req2 := newPutIPNSRequest(t, priv, uint64(2*i+1), time.Now().Add(time.Hour), time.Hour)

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

		// an IPNS record key has the form /ipns/$binary_id where $binary_id
		// is just the peer ID of the peer that belongs to the IPNS record.
		// Therefore, we can just string-cast the remote peer.ID here.
		val, err := d.backends[namespaceIPNS].Fetch(context.Background(), string(remote))
		require.NoError(t, err)

		r, ok := val.(*recpb.Record)
		require.True(t, ok)

		storedRec, err := ipns.UnmarshalRecord(r.Value)
		require.NoError(t, err)

		seq, err := storedRec.Sequence()
		require.NoError(t, err)

		// stored record must always be the newer one!
		assert.EqualValues(t, 2*i+1, seq)
	}
}

func TestDHT_handlePutValue_overwrites_corrupt_stored_ipns_record(t *testing.T) {
	d := newTestDHT(t)

	remote, priv := newIdentity(t)

	req := newPutIPNSRequest(t, priv, 10, time.Now().Add(time.Hour), time.Hour)

	dsKey := newDatastoreKey(namespaceIPNS, string(remote)) // string(remote) is the key suffix

	rbe, ok := d.backends[namespaceIPNS].(*RecordBackend)
	require.True(t, ok)

	err := rbe.datastore.Put(context.Background(), dsKey, []byte("corrupt-record"))
	require.NoError(t, err)

	// put the correct record through handler
	_, err = d.handlePutValue(context.Background(), remote, req)
	require.NoError(t, err)

	value, err := rbe.datastore.Get(context.Background(), dsKey)
	require.NoError(t, err)

	r := &recpb.Record{}
	require.NoError(t, r.Unmarshal(value))

	_, err = ipns.UnmarshalRecord(r.Value)
	require.NoError(t, err)
}

func TestDHT_handlePutValue_malformed_key(t *testing.T) {
	d := newTestDHT(t)

	keys := []string{
		"malformed-key",
		"     ",
		"/ipns/",
		"/pk/",
		"/ipns",
		"/pk",
		"ipns/",
		"pk/",
	}
	for _, k := range keys {
		t.Run("malformed key: "+k, func(t *testing.T) {
			req := &pb.Message{
				Type: pb.Message_PUT_VALUE,
				Key:  []byte(k),
				Record: &recpb.Record{
					Key: []byte(k),
				},
			}

			resp, err := d.handlePutValue(context.Background(), newPeerID(t), req)
			assert.Error(t, err)
			assert.Nil(t, resp)
			assert.ErrorContains(t, err, "invalid key")
		})
	}
}

func TestDHT_handlePutValue_unknown_backend(t *testing.T) {
	d := newTestDHT(t)

	req := &pb.Message{
		Type: pb.Message_PUT_VALUE,
		Key:  []byte("/other-namespace/record-key"),
		Record: &recpb.Record{
			Key: []byte("/other-namespace/record-key"),
		},
	}

	resp, err := d.handlePutValue(context.Background(), newPeerID(t), req)
	assert.Error(t, err)
	assert.Nil(t, resp)
	assert.ErrorContains(t, err, "unsupported record type")
}

func BenchmarkDHT_handleGetValue(b *testing.B) {
	d := newTestDHT(b)

	fillRoutingTable(b, d)

	rbe, ok := d.backends[namespaceIPNS].(*RecordBackend)
	require.True(b, ok)

	// fill datastore and build requests
	reqs := make([]*pb.Message, b.N)
	peers := make([]peer.ID, b.N)
	for i := 0; i < b.N; i++ {
		pid, priv := newIdentity(b)

		putReq := newPutIPNSRequest(b, priv, 0, time.Now().Add(time.Hour), time.Hour)

		data, err := putReq.Record.Marshal()
		require.NoError(b, err)

		dsKey := newDatastoreKey(namespaceIPNS, string(pid))

		err = rbe.datastore.Put(context.Background(), dsKey, data)
		require.NoError(b, err)

		peers[i] = pid
		reqs[i] = &pb.Message{
			Type: pb.Message_GET_VALUE,
			Key:  putReq.GetKey(),
		}
	}

	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := d.handleGetValue(ctx, peers[b.N-i-1], reqs[i])
		if err != nil {
			b.Error(err)
		}
	}
}

func TestDHT_handleGetValue_happy_path_ipns_record(t *testing.T) {
	d := newTestDHT(t)

	fillRoutingTable(t, d)

	remote, priv := newIdentity(t)

	putReq := newPutIPNSRequest(t, priv, 0, time.Now().Add(time.Hour), time.Hour)

	rbe, ok := d.backends[namespaceIPNS].(*RecordBackend)
	require.True(t, ok)

	data, err := putReq.Record.Marshal()
	require.NoError(t, err)

	dsKey := newDatastoreKey(namespaceIPNS, string(remote))
	err = rbe.datastore.Put(context.Background(), dsKey, data)
	require.NoError(t, err)

	getReq := &pb.Message{
		Type: pb.Message_GET_VALUE,
		Key:  putReq.GetKey(),
	}

	resp, err := d.handleGetValue(context.Background(), remote, getReq)
	require.NoError(t, err)

	assert.Equal(t, pb.Message_GET_VALUE, resp.Type)
	assert.Equal(t, putReq.Key, resp.Key)
	require.NotNil(t, putReq.Record)
	assert.Equal(t, putReq.Record.String(), resp.Record.String())
	assert.Len(t, resp.CloserPeers, 20)
	assert.Len(t, resp.ProviderPeers, 0)
}

func TestDHT_handleGetValue_record_not_found(t *testing.T) {
	d := newTestDHT(t)

	fillRoutingTable(t, d)

	for _, ns := range []string{namespaceIPNS, namespacePublicKey} {
		t.Run(ns, func(t *testing.T) {
			req := &pb.Message{
				Type: pb.Message_GET_VALUE,
				Key:  []byte(fmt.Sprintf("/%s/unknown-record-key", ns)),
			}

			resp, err := d.handleGetValue(context.Background(), newPeerID(t), req)
			require.NoError(t, err)

			assert.Equal(t, pb.Message_GET_VALUE, resp.Type)
			assert.Equal(t, req.Key, resp.Key)
			assert.Nil(t, resp.Record)
			assert.Len(t, resp.CloserPeers, 20)
			assert.Len(t, resp.ProviderPeers, 0)
		})
	}
}

func TestDHT_handleGetValue_corrupt_record_in_datastore(t *testing.T) {
	d := newTestDHT(t)

	fillRoutingTable(t, d)

	for _, ns := range []string{namespaceIPNS, namespacePublicKey} {
		t.Run(ns, func(t *testing.T) {
			rbe, ok := d.backends[ns].(*RecordBackend)
			require.True(t, ok)

			key := []byte(fmt.Sprintf("/%s/record-key", ns))

			dsKey := newDatastoreKey(ns, "record-key")
			err := rbe.datastore.Put(context.Background(), dsKey, []byte("corrupt-data"))
			require.NoError(t, err)

			req := &pb.Message{
				Type: pb.Message_GET_VALUE,
				Key:  key,
			}

			resp, err := d.handleGetValue(context.Background(), newPeerID(t), req)
			require.NoError(t, err)

			assert.Equal(t, pb.Message_GET_VALUE, resp.Type)
			assert.Equal(t, req.Key, resp.Key)
			assert.Nil(t, resp.Record)
			assert.Len(t, resp.CloserPeers, 20)
			assert.Len(t, resp.ProviderPeers, 0)

			// check that the record was deleted from the datastore
			data, err := rbe.datastore.Get(context.Background(), dsKey)
			assert.ErrorIs(t, err, ds.ErrNotFound)
			assert.Len(t, data, 0)
		})
	}
}

func TestDHT_handleGetValue_ipns_max_age_exceeded_in_datastore(t *testing.T) {
	d := newTestDHT(t)

	fillRoutingTable(t, d)

	remote, priv := newIdentity(t)

	putReq := newPutIPNSRequest(t, priv, 0, time.Now().Add(time.Hour), time.Hour)

	rbe, ok := d.backends[namespaceIPNS].(*RecordBackend)
	require.True(t, ok)

	dsKey := newDatastoreKey(namespaceIPNS, string(remote))

	data, err := putReq.Record.Marshal()
	require.NoError(t, err)

	err = rbe.datastore.Put(context.Background(), dsKey, data)
	require.NoError(t, err)

	req := &pb.Message{
		Type: pb.Message_GET_VALUE,
		Key:  putReq.GetKey(),
	}

	rbe.cfg.MaxRecordAge = 0

	resp, err := d.handleGetValue(context.Background(), remote, req)
	require.NoError(t, err)

	assert.Equal(t, pb.Message_GET_VALUE, resp.Type)
	assert.Equal(t, req.Key, resp.Key)
	assert.Nil(t, resp.Record)
	assert.Len(t, resp.CloserPeers, 20)
	assert.Len(t, resp.ProviderPeers, 0)

	// check that the record was deleted from the datastore
	data, err = rbe.datastore.Get(context.Background(), dsKey)
	assert.ErrorIs(t, err, ds.ErrNotFound)
	assert.Len(t, data, 0)
}

func TestDHT_handleGetValue_does_not_validate_stored_record(t *testing.T) {
	d := newTestDHT(t)

	fillRoutingTable(t, d)

	rbe, ok := d.backends[namespaceIPNS].(*RecordBackend)
	require.True(t, ok)

	remote, priv := newIdentity(t)

	// generate expired record (doesn't pass validation)
	putReq := newPutIPNSRequest(t, priv, 0, time.Now().Add(-time.Hour), -time.Hour)

	data, err := putReq.Record.Marshal()
	require.NoError(t, err)

	dsKey := newDatastoreKey(namespaceIPNS, string(remote))

	err = rbe.datastore.Put(context.Background(), dsKey, data)
	require.NoError(t, err)

	req := &pb.Message{
		Type: pb.Message_GET_VALUE,
		Key:  putReq.GetKey(),
	}

	resp, err := d.handleGetValue(context.Background(), remote, req)
	require.NoError(t, err)

	assert.Equal(t, pb.Message_GET_VALUE, resp.Type)
	assert.Equal(t, req.Key, resp.Key)
	require.NotNil(t, putReq.Record)
	assert.Equal(t, putReq.Record.String(), resp.Record.String())
	assert.Len(t, resp.CloserPeers, 20)
	assert.Len(t, resp.ProviderPeers, 0)
}

func TestDHT_handleGetValue_malformed_key(t *testing.T) {
	d := newTestDHT(t)

	keys := []string{
		"malformed-key",
		"     ",
		"/ipns/",
		"/pk/",
		"/ipns",
		"/pk",
		"ipns/",
		"pk/",
	}
	for _, k := range keys {
		t.Run("malformed key: "+k, func(t *testing.T) {
			req := &pb.Message{
				Type: pb.Message_GET_VALUE,
				Key:  []byte(k),
			}

			resp, err := d.handleGetValue(context.Background(), newPeerID(t), req)
			assert.Error(t, err)
			assert.Nil(t, resp)
			assert.ErrorContains(t, err, "invalid key")
		})
	}
}

func TestDHT_handleGetValue_unknown_backend(t *testing.T) {
	d := newTestDHT(t)

	req := &pb.Message{
		Type: pb.Message_GET_VALUE,
		Key:  []byte("/other-namespace/record-key"),
		Record: &recpb.Record{
			Key: []byte("/other-namespace/record-key"),
		},
	}

	resp, err := d.handleGetValue(context.Background(), newPeerID(t), req)
	assert.Error(t, err)
	assert.Nil(t, resp)
	assert.ErrorContains(t, err, "unsupported record type")
}

func TestDHT_handleGetValue_supports_providers(t *testing.T) {
	t.Skip("TODO")
}
