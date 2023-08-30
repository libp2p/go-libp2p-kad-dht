package dht

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p-kad-dht/internal/net"
	"github.com/libp2p/go-libp2p-kad-dht/providers"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/event"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-libp2p/core/routing"
	ma "github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
	"github.com/multiformats/go-multihash"
	"github.com/multiformats/go-multistream"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	test "github.com/libp2p/go-libp2p-kad-dht/internal/testing"
	pb "github.com/libp2p/go-libp2p-kad-dht/pb"

	u "github.com/ipfs/boxo/util"
	"github.com/ipfs/go-cid"
	detectrace "github.com/ipfs/go-detect-race"
	kb "github.com/libp2p/go-libp2p-kbucket"
	record "github.com/libp2p/go-libp2p-record"
	bhost "github.com/libp2p/go-libp2p/p2p/host/basic"
	swarmt "github.com/libp2p/go-libp2p/p2p/net/swarm/testing"
)

var testCaseCids []cid.Cid

func init() {
	for i := 0; i < 100; i++ {
		v := fmt.Sprintf("%d -- value", i)

		var newCid cid.Cid
		switch i % 3 {
		case 0:
			mhv := u.Hash([]byte(v))
			newCid = cid.NewCidV0(mhv)
		case 1:
			mhv := u.Hash([]byte(v))
			newCid = cid.NewCidV1(cid.DagCBOR, mhv)
		case 2:
			rawMh := make([]byte, 12)
			binary.PutUvarint(rawMh, cid.Raw)
			binary.PutUvarint(rawMh[1:], 10)
			copy(rawMh[2:], []byte(v)[:10])
			_, mhv, err := multihash.MHFromBytes(rawMh)
			if err != nil {
				panic(err)
			}
			newCid = cid.NewCidV1(cid.Raw, mhv)
		}
		testCaseCids = append(testCaseCids, newCid)
	}
}

type blankValidator struct{}

func (blankValidator) Validate(_ string, _ []byte) error        { return nil }
func (blankValidator) Select(_ string, _ [][]byte) (int, error) { return 0, nil }

type testAtomicPutValidator struct {
	test.TestValidator
}

// selects the entry with the 'highest' last byte
func (testAtomicPutValidator) Select(_ string, bs [][]byte) (int, error) {
	index := -1
	max := uint8(0)
	for i, b := range bs {
		if bytes.Equal(b, []byte("valid")) {
			if index == -1 {
				index = i
			}
			continue
		}

		str := string(b)
		n := str[len(str)-1]
		if n > max {
			max = n
			index = i
		}

	}
	if index == -1 {
		return -1, errors.New("no rec found")
	}
	return index, nil
}

var testPrefix = ProtocolPrefix("/test")

func setupDHT(ctx context.Context, t *testing.T, client bool, options ...Option) *IpfsDHT {
	baseOpts := []Option{
		testPrefix,
		NamespacedValidator("v", blankValidator{}),
		DisableAutoRefresh(),
	}

	if client {
		baseOpts = append(baseOpts, Mode(ModeClient))
	} else {
		baseOpts = append(baseOpts, Mode(ModeServer))
	}

	host, err := bhost.NewHost(swarmt.GenSwarm(t, swarmt.OptDisableReuseport), new(bhost.HostOpts))
	require.NoError(t, err)
	host.Start()
	t.Cleanup(func() { host.Close() })

	d, err := New(ctx, host, append(baseOpts, options...)...)
	require.NoError(t, err)
	t.Cleanup(func() { d.Close() })
	return d
}

func setupDHTS(t *testing.T, ctx context.Context, n int, options ...Option) []*IpfsDHT {
	addrs := make([]ma.Multiaddr, n)
	dhts := make([]*IpfsDHT, n)
	peers := make([]peer.ID, n)

	sanityAddrsMap := make(map[string]struct{})
	sanityPeersMap := make(map[string]struct{})

	for i := 0; i < n; i++ {
		dhts[i] = setupDHT(ctx, t, false, options...)
		peers[i] = dhts[i].PeerID()
		addrs[i] = dhts[i].host.Addrs()[0]

		if _, lol := sanityAddrsMap[addrs[i].String()]; lol {
			t.Fatal("While setting up DHTs address got duplicated.")
		} else {
			sanityAddrsMap[addrs[i].String()] = struct{}{}
		}
		if _, lol := sanityPeersMap[peers[i].String()]; lol {
			t.Fatal("While setting up DHTs peerid got duplicated.")
		} else {
			sanityPeersMap[peers[i].String()] = struct{}{}
		}
	}

	return dhts
}

func connectNoSync(t *testing.T, ctx context.Context, a, b *IpfsDHT) {
	t.Helper()

	idB := b.self
	addrB := b.peerstore.Addrs(idB)
	if len(addrB) == 0 {
		t.Fatal("peers setup incorrectly: no local address")
	}

	if err := a.host.Connect(ctx, peer.AddrInfo{ID: idB, Addrs: addrB}); err != nil {
		t.Fatal(err)
	}
}

func wait(t *testing.T, ctx context.Context, a, b *IpfsDHT) {
	t.Helper()

	// loop until connection notification has been received.
	// under high load, this may not happen as immediately as we would like.
	for a.routingTable.Find(b.self) == "" {
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(time.Millisecond * 5):
		}
	}
}

func connect(t *testing.T, ctx context.Context, a, b *IpfsDHT) {
	t.Helper()
	connectNoSync(t, ctx, a, b)
	wait(t, ctx, a, b)
	wait(t, ctx, b, a)
}

func bootstrap(t *testing.T, ctx context.Context, dhts []*IpfsDHT) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	logger.Debugf("refreshing DHTs routing tables...")

	// tried async. sequential fares much better. compare:
	// 100 async https://gist.github.com/jbenet/56d12f0578d5f34810b2
	// 100 sync https://gist.github.com/jbenet/6c59e7c15426e48aaedd
	// probably because results compound

	start := rand.Intn(len(dhts)) // randomize to decrease bias.
	for i := range dhts {
		dht := dhts[(start+i)%len(dhts)]
		select {
		case err := <-dht.RefreshRoutingTable():
			if err != nil {
				t.Error(err)
			}
		case <-ctx.Done():
			return
		}
	}
}

// Check to make sure we always signal the RefreshRoutingTable channel.
func TestRefreshMultiple(t *testing.T) {
	// TODO: What's with this test? How long should it take and why does RefreshRoutingTable not take a context?
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()

	dhts := setupDHTS(t, ctx, 5)
	defer func() {
		for _, dht := range dhts {
			dht.Close()
			defer dht.host.Close()
		}
	}()

	for _, dht := range dhts[1:] {
		connect(t, ctx, dhts[0], dht)
	}

	a := dhts[0].RefreshRoutingTable()
	time.Sleep(time.Nanosecond)
	b := dhts[0].RefreshRoutingTable()
	time.Sleep(time.Nanosecond)
	c := dhts[0].RefreshRoutingTable()

	// make sure that all of these eventually return
	select {
	case <-a:
	case <-ctx.Done():
		t.Fatal("first channel didn't signal")
	}
	select {
	case <-b:
	case <-ctx.Done():
		t.Fatal("second channel didn't signal")
	}
	select {
	case <-c:
	case <-ctx.Done():
		t.Fatal("third channel didn't signal")
	}
}

func TestValueGetSet(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var dhts [5]*IpfsDHT

	for i := range dhts {
		dhts[i] = setupDHT(ctx, t, false)
		defer dhts[i].Close()
		defer dhts[i].host.Close()
	}

	connect(t, ctx, dhts[0], dhts[1])

	t.Log("adding value on: ", dhts[0].self)
	ctxT, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	err := dhts[0].PutValue(ctxT, "/v/hello", []byte("world"))
	if err != nil {
		t.Fatal(err)
	}

	t.Log("requesting value on dhts: ", dhts[1].self)
	ctxT, cancel = context.WithTimeout(ctx, time.Second*2*60)
	defer cancel()

	val, err := dhts[1].GetValue(ctxT, "/v/hello")
	if err != nil {
		t.Fatal(err)
	}

	if string(val) != "world" {
		t.Fatalf("Expected 'world' got '%s'", string(val))
	}

	connect(t, ctx, dhts[2], dhts[0])
	connect(t, ctx, dhts[2], dhts[1])

	t.Log("requesting value (offline) on dhts: ", dhts[2].self)
	vala, err := dhts[2].GetValue(ctxT, "/v/hello", Quorum(0))
	if err != nil {
		t.Fatal(err)
	}

	if string(vala) != "world" {
		t.Fatalf("Expected 'world' got '%s'", string(vala))
	}
	t.Log("requesting value (online) on dhts: ", dhts[2].self)
	val, err = dhts[2].GetValue(ctxT, "/v/hello")
	if err != nil {
		t.Fatal(err)
	}

	if string(val) != "world" {
		t.Fatalf("Expected 'world' got '%s'", string(val))
	}

	for _, d := range dhts[:3] {
		connect(t, ctx, dhts[3], d)
	}
	connect(t, ctx, dhts[4], dhts[3])

	t.Log("requesting value (requires peer routing) on dhts: ", dhts[4].self)
	val, err = dhts[4].GetValue(ctxT, "/v/hello")
	if err != nil {
		t.Fatal(err)
	}

	if string(val) != "world" {
		t.Fatalf("Expected 'world' got '%s'", string(val))
	}
}

func TestValueSetInvalid(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dhtA := setupDHT(ctx, t, false)
	dhtB := setupDHT(ctx, t, false)

	defer dhtA.Close()
	defer dhtB.Close()
	defer dhtA.host.Close()
	defer dhtB.host.Close()

	dhtA.Validator.(record.NamespacedValidator)["v"] = test.TestValidator{}
	dhtB.Validator.(record.NamespacedValidator)["v"] = blankValidator{}

	connect(t, ctx, dhtA, dhtB)

	testSetGet := func(val string, failset bool, exp string, experr error) {
		t.Helper()

		ctxT, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		err := dhtA.PutValue(ctxT, "/v/hello", []byte(val))
		if failset {
			if err == nil {
				t.Error("expected set to fail")
			}
		} else {
			if err != nil {
				t.Error(err)
			}
		}

		ctxT, cancel = context.WithTimeout(ctx, time.Second*2)
		defer cancel()
		valb, err := dhtB.GetValue(ctxT, "/v/hello")
		if err != experr {
			t.Errorf("Set/Get %v: Expected %v error but got %v", val, experr, err)
		} else if err == nil && string(valb) != exp {
			t.Errorf("Expected '%v' got '%s'", exp, string(valb))
		}
	}

	// Expired records should not be set
	testSetGet("expired", true, "", routing.ErrNotFound)
	// Valid record should be returned
	testSetGet("valid", false, "valid", nil)
	// Newer record should supersede previous record
	testSetGet("newer", false, "newer", nil)
	// Attempt to set older record again should be ignored
	testSetGet("valid", true, "newer", nil)
}

func TestContextShutDown(t *testing.T) {
	t.Skip("This test is flaky, see https://github.com/libp2p/go-libp2p-kad-dht/issues/724.")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dht := setupDHT(ctx, t, false)

	// context is alive
	select {
	case <-dht.Context().Done():
		t.Fatal("context should not be done")
	default:
	}

	// shut down dht
	require.NoError(t, dht.Close())

	// now context should be done
	select {
	case <-dht.Context().Done():
	default:
		t.Fatal("context should be done")
	}
}

func TestSearchValue(t *testing.T) {
	t.Skip("This test is flaky, see https://github.com/libp2p/go-libp2p-kad-dht/issues/723.")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dhtA := setupDHT(ctx, t, false)
	dhtB := setupDHT(ctx, t, false)

	defer dhtA.Close()
	defer dhtB.Close()
	defer dhtA.host.Close()
	defer dhtB.host.Close()

	connect(t, ctx, dhtA, dhtB)

	dhtA.Validator.(record.NamespacedValidator)["v"] = test.TestValidator{}
	dhtB.Validator.(record.NamespacedValidator)["v"] = test.TestValidator{}

	ctxT, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()

	err := dhtA.PutValue(ctxT, "/v/hello", []byte("valid"))
	if err != nil {
		t.Error(err)
	}

	ctxT, cancel = context.WithTimeout(ctx, time.Second*2)
	defer cancel()
	valCh, err := dhtA.SearchValue(ctxT, "/v/hello", Quorum(0))
	if err != nil {
		t.Fatal(err)
	}

	select {
	case v := <-valCh:
		if string(v) != "valid" {
			t.Errorf("expected 'valid', got '%s'", string(v))
		}
	case <-ctxT.Done():
		t.Fatal(ctxT.Err())
	}

	err = dhtB.PutValue(ctxT, "/v/hello", []byte("newer"))
	if err != nil {
		t.Error(err)
	}

	select {
	case v := <-valCh:
		if string(v) != "newer" {
			t.Errorf("expected 'newer', got '%s'", string(v))
		}
	case <-ctxT.Done():
		t.Fatal(ctxT.Err())
	}
}

func TestValueGetInvalid(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dhtA := setupDHT(ctx, t, false)
	dhtB := setupDHT(ctx, t, false)

	defer dhtA.Close()
	defer dhtB.Close()
	defer dhtA.host.Close()
	defer dhtB.host.Close()

	dhtA.Validator.(record.NamespacedValidator)["v"] = blankValidator{}
	dhtB.Validator.(record.NamespacedValidator)["v"] = test.TestValidator{}

	connect(t, ctx, dhtA, dhtB)

	testSetGet := func(val string, exp string, experr error) {
		t.Helper()

		ctxT, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		err := dhtA.PutValue(ctxT, "/v/hello", []byte(val))
		if err != nil {
			t.Error(err)
		}

		ctxT, cancel = context.WithTimeout(ctx, time.Second*2)
		defer cancel()
		valb, err := dhtB.GetValue(ctxT, "/v/hello")
		if err != experr {
			t.Errorf("Set/Get %v: Expected '%v' error but got '%v'", val, experr, err)
		} else if err == nil && string(valb) != exp {
			t.Errorf("Expected '%v' got '%s'", exp, string(valb))
		}
	}

	// Expired records should not be returned
	testSetGet("expired", "", routing.ErrNotFound)
	// Valid record should be returned
	testSetGet("valid", "valid", nil)
	// Newer record should supersede previous record
	testSetGet("newer", "newer", nil)
	// Attempt to set older record again should be ignored
	testSetGet("valid", "newer", nil)
}

func TestProvides(t *testing.T) {
	// t.Skip("skipping test to debug another")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dhts := setupDHTS(t, ctx, 4)
	defer func() {
		for i := 0; i < 4; i++ {
			dhts[i].Close()
			defer dhts[i].host.Close()
		}
	}()

	connect(t, ctx, dhts[0], dhts[1])
	connect(t, ctx, dhts[1], dhts[2])
	connect(t, ctx, dhts[1], dhts[3])

	for _, k := range testCaseCids {
		logger.Debugf("announcing provider for %s", k)
		if err := dhts[3].Provide(ctx, k, true); err != nil {
			t.Fatal(err)
		}
	}

	// what is this timeout for? was 60ms before.
	time.Sleep(time.Millisecond * 6)

	n := 0
	for _, c := range testCaseCids {
		n = (n + 1) % 3

		logger.Debugf("getting providers for %s from %d", c, n)
		ctxT, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		provchan := dhts[n].FindProvidersAsync(ctxT, c, 1)

		select {
		case prov := <-provchan:
			if prov.ID == "" {
				t.Fatal("Got back nil provider")
			}
			if prov.ID != dhts[3].self {
				t.Fatal("Got back wrong provider")
			}
			if len(prov.Addrs) == 0 {
				t.Fatal("Got no addresses back")
			}
		case <-ctxT.Done():
			t.Fatal("Did not get a provider back.")
		}
	}
}

type testMessageSender struct {
	sendRequest func(ctx context.Context, p peer.ID, pmes *pb.Message) (*pb.Message, error)
	sendMessage func(ctx context.Context, p peer.ID, pmes *pb.Message) error
}

var _ pb.MessageSender = (*testMessageSender)(nil)

func (t testMessageSender) SendRequest(ctx context.Context, p peer.ID, pmes *pb.Message) (*pb.Message, error) {
	return t.sendRequest(ctx, p, pmes)
}

func (t testMessageSender) SendMessage(ctx context.Context, p peer.ID, pmes *pb.Message) error {
	return t.sendMessage(ctx, p, pmes)
}

func TestProvideAddressFilter(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dhts := setupDHTS(t, ctx, 2)

	connect(t, ctx, dhts[0], dhts[1])
	testMaddr := ma.StringCast("/ip4/99.99.99.99/tcp/9999")

	done := make(chan struct{})
	impl := net.NewMessageSenderImpl(dhts[0].host, dhts[0].protocols)
	tms := &testMessageSender{
		sendMessage: func(ctx context.Context, p peer.ID, pmes *pb.Message) error {
			defer close(done)
			assert.Equal(t, pmes.Type, pb.Message_ADD_PROVIDER)
			assert.Len(t, pmes.ProviderPeers[0].Addrs, 1)
			assert.True(t, pmes.ProviderPeers[0].Addresses()[0].Equal(testMaddr))
			return impl.SendMessage(ctx, p, pmes)
		},
		sendRequest: func(ctx context.Context, p peer.ID, pmes *pb.Message) (*pb.Message, error) {
			return impl.SendRequest(ctx, p, pmes)
		},
	}
	pm, err := pb.NewProtocolMessenger(tms)
	require.NoError(t, err)

	dhts[0].protoMessenger = pm
	dhts[0].addrFilter = func(multiaddrs []ma.Multiaddr) []ma.Multiaddr {
		return []ma.Multiaddr{testMaddr}
	}

	if err := dhts[0].Provide(ctx, testCaseCids[0], true); err != nil {
		t.Fatal(err)
	}

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout")
	}
}

type testProviderManager struct {
	addProvider  func(ctx context.Context, key []byte, prov peer.AddrInfo) error
	getProviders func(ctx context.Context, key []byte) ([]peer.AddrInfo, error)
	close        func() error
}

var _ providers.ProviderStore = (*testProviderManager)(nil)

func (t *testProviderManager) AddProvider(ctx context.Context, key []byte, prov peer.AddrInfo) error {
	return t.addProvider(ctx, key, prov)
}

func (t *testProviderManager) GetProviders(ctx context.Context, key []byte) ([]peer.AddrInfo, error) {
	return t.getProviders(ctx, key)
}

func (t *testProviderManager) Close() error {
	return t.close()
}

func TestHandleAddProviderAddressFilter(t *testing.T) {
	ctx := context.Background()

	d := setupDHT(ctx, t, false)
	provider := setupDHT(ctx, t, false)

	testMaddr := ma.StringCast("/ip4/99.99.99.99/tcp/9999")

	d.addrFilter = func(multiaddrs []ma.Multiaddr) []ma.Multiaddr {
		return []ma.Multiaddr{testMaddr}
	}

	done := make(chan struct{})
	d.providerStore = &testProviderManager{
		addProvider: func(ctx context.Context, key []byte, prov peer.AddrInfo) error {
			defer close(done)
			assert.True(t, prov.Addrs[0].Equal(testMaddr))
			return nil
		},
		close: func() error { return nil },
	}

	pmes := &pb.Message{
		Type: pb.Message_ADD_PROVIDER,
		Key:  []byte("test-key"),
		ProviderPeers: pb.RawPeerInfosToPBPeers([]peer.AddrInfo{{
			ID: provider.self,
			Addrs: []ma.Multiaddr{
				ma.StringCast("/ip4/55.55.55.55/tcp/5555"),
				ma.StringCast("/ip4/66.66.66.66/tcp/6666"),
			},
		}}),
	}

	_, err := d.handleAddProvider(ctx, provider.self, pmes)
	require.NoError(t, err)

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout")
	}
}

func TestLocalProvides(t *testing.T) {
	// t.Skip("skipping test to debug another")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dhts := setupDHTS(t, ctx, 4)
	defer func() {
		for i := 0; i < 4; i++ {
			dhts[i].Close()
			defer dhts[i].host.Close()
		}
	}()

	connect(t, ctx, dhts[0], dhts[1])
	connect(t, ctx, dhts[1], dhts[2])
	connect(t, ctx, dhts[1], dhts[3])

	for _, k := range testCaseCids {
		logger.Debugf("announcing provider for %s", k)
		if err := dhts[3].Provide(ctx, k, false); err != nil {
			t.Fatal(err)
		}
	}

	time.Sleep(time.Millisecond * 10)

	for _, c := range testCaseCids {
		for i := 0; i < 3; i++ {
			provs, _ := dhts[i].ProviderStore().GetProviders(ctx, c.Hash())
			if len(provs) > 0 {
				t.Fatal("shouldnt know this")
			}
		}
	}
}

func TestAddressFilterProvide(t *testing.T) {
	// t.Skip("skipping test to debug another")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testMaddr := ma.StringCast("/ip4/99.99.99.99/tcp/9999")

	d := setupDHT(ctx, t, false)
	provider := setupDHT(ctx, t, false)

	d.addrFilter = func(maddrs []ma.Multiaddr) []ma.Multiaddr {
		return []ma.Multiaddr{
			testMaddr,
		}
	}

	_, err := d.handleAddProvider(ctx, provider.self, &pb.Message{
		Type: pb.Message_ADD_PROVIDER,
		Key:  []byte("random-key"),
		ProviderPeers: pb.PeerInfosToPBPeers(provider.host.Network(), []peer.AddrInfo{{
			ID:    provider.self,
			Addrs: provider.host.Addrs(),
		}}),
	})
	require.NoError(t, err)

	// because of the identify protocol we add all
	// addresses to the peerstore, although the addresses
	// will be filtered in the above handleAddProvider call
	d.peerstore.AddAddrs(provider.self, provider.host.Addrs(), time.Hour)

	resp, err := d.handleGetProviders(ctx, d.self, &pb.Message{
		Type: pb.Message_GET_PROVIDERS,
		Key:  []byte("random-key"),
	})
	require.NoError(t, err)

	assert.True(t, resp.ProviderPeers[0].Addresses()[0].Equal(testMaddr))
	assert.Len(t, resp.ProviderPeers[0].Addresses(), 1)
}

// if minPeers or avgPeers is 0, dont test for it.
func waitForWellFormedTables(t *testing.T, dhts []*IpfsDHT, minPeers, avgPeers int, timeout time.Duration) {
	// test "well-formed-ness" (>= minPeers peers in every routing table)
	t.Helper()

	timeoutA := time.After(timeout)
	for {
		select {
		case <-timeoutA:
			t.Errorf("failed to reach well-formed routing tables after %s", timeout)
			return
		case <-time.After(5 * time.Millisecond):
			if checkForWellFormedTablesOnce(t, dhts, minPeers, avgPeers) {
				// succeeded
				return
			}
		}
	}
}

func checkForWellFormedTablesOnce(t *testing.T, dhts []*IpfsDHT, minPeers, avgPeers int) bool {
	t.Helper()
	totalPeers := 0
	for _, dht := range dhts {
		rtlen := dht.routingTable.Size()
		totalPeers += rtlen
		if minPeers > 0 && rtlen < minPeers {
			// t.Logf("routing table for %s only has %d peers (should have >%d)", dht.self, rtlen, minPeers)
			return false
		}
	}
	actualAvgPeers := totalPeers / len(dhts)
	t.Logf("avg rt size: %d", actualAvgPeers)
	if avgPeers > 0 && actualAvgPeers < avgPeers {
		t.Logf("avg rt size: %d < %d", actualAvgPeers, avgPeers)
		return false
	}
	return true
}

func printRoutingTables(dhts []*IpfsDHT) {
	// the routing tables should be full now. let's inspect them.
	fmt.Printf("checking routing table of %d\n", len(dhts))
	for _, dht := range dhts {
		fmt.Printf("checking routing table of %s\n", dht.self)
		dht.routingTable.Print()
		fmt.Println("")
	}
}

func TestRefresh(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nDHTs := 30
	dhts := setupDHTS(t, ctx, nDHTs)
	defer func() {
		for i := 0; i < nDHTs; i++ {
			dhts[i].Close()
			defer dhts[i].host.Close()
		}
	}()

	t.Logf("connecting %d dhts in a ring", nDHTs)
	for i := 0; i < nDHTs; i++ {
		connect(t, ctx, dhts[i], dhts[(i+1)%len(dhts)])
	}

	<-time.After(100 * time.Millisecond)
	// bootstrap a few times until we get good tables.
	t.Logf("bootstrapping them so they find each other %d", nDHTs)

	for {
		bootstrap(t, ctx, dhts)

		if checkForWellFormedTablesOnce(t, dhts, 7, 10) {
			break
		}

		time.Sleep(time.Microsecond * 50)
	}

	if u.Debug {
		// the routing tables should be full now. let's inspect them.
		printRoutingTables(dhts)
	}
}

func TestRefreshBelowMinRTThreshold(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	host, err := bhost.NewHost(swarmt.GenSwarm(t, swarmt.OptDisableReuseport), new(bhost.HostOpts))
	require.NoError(t, err)
	host.Start()

	// enable auto bootstrap on A
	dhtA, err := New(
		ctx,
		host,
		testPrefix,
		Mode(ModeServer),
		NamespacedValidator("v", blankValidator{}),
	)
	if err != nil {
		t.Fatal(err)
	}

	dhtB := setupDHT(ctx, t, false)
	dhtC := setupDHT(ctx, t, false)

	defer func() {
		dhtA.Close()
		dhtA.host.Close()

		dhtB.Close()
		dhtB.host.Close()

		dhtC.Close()
		dhtC.host.Close()
	}()

	connect(t, ctx, dhtA, dhtB)
	connect(t, ctx, dhtB, dhtC)

	// we ONLY init bootstrap on A
	dhtA.RefreshRoutingTable()
	// and wait for one round to complete i.e. A should be connected to both B & C
	waitForWellFormedTables(t, []*IpfsDHT{dhtA}, 2, 2, 20*time.Second)

	// now we create two new peers
	dhtD := setupDHT(ctx, t, false)
	dhtE := setupDHT(ctx, t, false)

	// connect them to each other
	connect(t, ctx, dhtD, dhtE)
	defer func() {
		dhtD.Close()
		dhtD.host.Close()

		dhtE.Close()
		dhtE.host.Close()
	}()

	// and then, on connecting the peer D to A, the min RT threshold gets triggered on A which leads to a bootstrap.
	// since the default bootstrap scan interval is 30 mins - 1 hour, we can be sure that if bootstrap happens,
	// it is because of the min RT threshold getting triggered (since default min value is 4 & we only have 2 peers in the RT when D gets connected)
	connect(t, ctx, dhtA, dhtD)

	// and because of the above bootstrap, A also discovers E !
	waitForWellFormedTables(t, []*IpfsDHT{dhtA}, 4, 4, 10*time.Second)
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, dhtE.self, dhtA.routingTable.Find(dhtE.self), "A's routing table should have peer E!")
}

func TestQueryWithEmptyRTShouldNotPanic(t *testing.T) {
	ctx := context.Background()
	d := setupDHT(ctx, t, false)

	// TODO This swallows the error for now, should we change it ?
	// FindProviders
	ps, _ := d.FindProviders(ctx, testCaseCids[0])
	require.Empty(t, ps)

	// GetClosestPeers
	pc, err := d.GetClosestPeers(ctx, "key")
	require.Nil(t, pc)
	require.Equal(t, kb.ErrLookupFailure, err)

	// GetValue
	best, err := d.GetValue(ctx, "key")
	require.Empty(t, best)
	require.Error(t, err)

	// SearchValue
	bchan, err := d.SearchValue(ctx, "key")
	require.Empty(t, bchan)
	require.NoError(t, err)

	// Provide
	err = d.Provide(ctx, testCaseCids[0], true)
	require.Equal(t, kb.ErrLookupFailure, err)
}

func TestPeriodicRefresh(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}
	if runtime.GOOS == "windows" {
		t.Skip("skipping due to #760")
	}
	if detectrace.WithRace() {
		t.Skip("skipping due to race detector max goroutines")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nDHTs := 30
	dhts := setupDHTS(t, ctx, nDHTs)
	defer func() {
		for i := 0; i < nDHTs; i++ {
			dhts[i].Close()
			defer dhts[i].host.Close()
		}
	}()

	t.Logf("dhts are not connected. %d", nDHTs)
	for _, dht := range dhts {
		rtlen := dht.routingTable.Size()
		if rtlen > 0 {
			t.Errorf("routing table for %s should have 0 peers. has %d", dht.self, rtlen)
		}
	}

	for i := 0; i < nDHTs; i++ {
		connect(t, ctx, dhts[i], dhts[(i+1)%len(dhts)])
	}

	t.Logf("DHTs are now connected to 1-2 others. %d", nDHTs)
	for _, dht := range dhts {
		rtlen := dht.routingTable.Size()
		if rtlen > 2 {
			t.Errorf("routing table for %s should have at most 2 peers. has %d", dht.self, rtlen)
		}
	}

	if u.Debug {
		printRoutingTables(dhts)
	}

	t.Logf("bootstrapping them so they find each other. %d", nDHTs)
	var wg sync.WaitGroup
	for _, dht := range dhts {
		wg.Add(1)
		go func(d *IpfsDHT) {
			<-d.RefreshRoutingTable()
			wg.Done()
		}(dht)
	}

	wg.Wait()
	// this is async, and we dont know when it's finished with one cycle, so keep checking
	// until the routing tables look better, or some long timeout for the failure case.
	waitForWellFormedTables(t, dhts, 7, 10, 20*time.Second)

	if u.Debug {
		printRoutingTables(dhts)
	}
}

func TestProvidesMany(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skipping due to #760")
	}
	if detectrace.WithRace() {
		t.Skip("skipping due to race detector max goroutines")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nDHTs := 40
	dhts := setupDHTS(t, ctx, nDHTs)
	defer func() {
		for i := 0; i < nDHTs; i++ {
			dhts[i].Close()
			defer dhts[i].host.Close()
		}
	}()

	t.Logf("connecting %d dhts in a ring", nDHTs)
	for i := 0; i < nDHTs; i++ {
		connect(t, ctx, dhts[i], dhts[(i+1)%len(dhts)])
	}

	<-time.After(100 * time.Millisecond)
	t.Logf("bootstrapping them so they find each other. %d", nDHTs)
	ctxT, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	bootstrap(t, ctxT, dhts)

	if u.Debug {
		// the routing tables should be full now. let's inspect them.
		t.Logf("checking routing table of %d", nDHTs)
		for _, dht := range dhts {
			fmt.Printf("checking routing table of %s\n", dht.self)
			dht.routingTable.Print()
			fmt.Println("")
		}
	}

	providers := make(map[cid.Cid]peer.ID)

	d := 0
	for _, c := range testCaseCids {
		d = (d + 1) % len(dhts)
		dht := dhts[d]
		providers[c] = dht.self

		t.Logf("announcing provider for %s", c)
		if err := dht.Provide(ctx, c, true); err != nil {
			t.Fatal(err)
		}
	}

	// what is this timeout for? was 60ms before.
	time.Sleep(time.Millisecond * 6)

	errchan := make(chan error)

	ctxT, cancel = context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	getProvider := func(dht *IpfsDHT, k cid.Cid) {
		defer wg.Done()

		expected := providers[k]

		provchan := dht.FindProvidersAsync(ctxT, k, 1)
		select {
		case prov := <-provchan:
			actual := prov.ID
			if actual == "" {
				errchan <- fmt.Errorf("Got back nil provider (%s at %s)", k, dht.self)
			} else if actual != expected {
				errchan <- fmt.Errorf("Got back wrong provider (%s != %s) (%s at %s)",
					expected, actual, k, dht.self)
			}
		case <-ctxT.Done():
			errchan <- fmt.Errorf("Did not get a provider back (%s at %s)", k, dht.self)
		}
	}

	for _, c := range testCaseCids {
		// everyone should be able to find it...
		for _, dht := range dhts {
			logger.Debugf("getting providers for %s at %s", c, dht.self)
			wg.Add(1)
			go getProvider(dht, c)
		}
	}

	// we need this because of printing errors
	go func() {
		wg.Wait()
		close(errchan)
	}()

	for err := range errchan {
		t.Error(err)
	}
}

func TestProvidesAsync(t *testing.T) {
	// t.Skip("skipping test to debug another")
	if testing.Short() {
		t.SkipNow()
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dhts := setupDHTS(t, ctx, 4)
	defer func() {
		for i := 0; i < 4; i++ {
			dhts[i].Close()
			defer dhts[i].host.Close()
		}
	}()

	connect(t, ctx, dhts[0], dhts[1])
	connect(t, ctx, dhts[1], dhts[2])
	connect(t, ctx, dhts[1], dhts[3])

	err := dhts[3].Provide(ctx, testCaseCids[0], true)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Millisecond * 60)

	ctxT, cancel := context.WithTimeout(ctx, time.Millisecond*300)
	defer cancel()
	provs := dhts[0].FindProvidersAsync(ctxT, testCaseCids[0], 5)
	select {
	case p, ok := <-provs:
		if !ok {
			t.Fatal("Provider channel was closed...")
		}
		if p.ID == "" {
			t.Fatal("Got back nil provider!")
		}
		if p.ID != dhts[3].self {
			t.Fatalf("got a provider, but not the right one. %s", p)
		}
	case <-ctxT.Done():
		t.Fatal("Didnt get back providers")
	}
}

func TestLayeredGet(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dhts := setupDHTS(t, ctx, 4)
	defer func() {
		for i := 0; i < 4; i++ {
			dhts[i].Close()
			defer dhts[i].host.Close()
		}
	}()

	connect(t, ctx, dhts[0], dhts[1])
	connect(t, ctx, dhts[1], dhts[2])
	connect(t, ctx, dhts[2], dhts[3])

	err := dhts[3].PutValue(ctx, "/v/hello", []byte("world"))
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Millisecond * 6)

	ctxT, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	val, err := dhts[0].GetValue(ctxT, "/v/hello")
	if err != nil {
		t.Fatal(err)
	}

	if string(val) != "world" {
		t.Error("got wrong value")
	}
}

func TestUnfindablePeer(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dhts := setupDHTS(t, ctx, 4)
	defer func() {
		for i := 0; i < 4; i++ {
			dhts[i].Close()
			dhts[i].Host().Close()
		}
	}()

	connect(t, ctx, dhts[0], dhts[1])
	connect(t, ctx, dhts[1], dhts[2])
	connect(t, ctx, dhts[2], dhts[3])

	// Give DHT 1 a bad addr for DHT 2.
	dhts[1].host.Peerstore().ClearAddrs(dhts[2].PeerID())
	dhts[1].host.Peerstore().AddAddr(dhts[2].PeerID(), dhts[0].Host().Addrs()[0], time.Minute)

	ctxT, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	_, err := dhts[0].FindPeer(ctxT, dhts[3].PeerID())
	if err == nil {
		t.Error("should have failed to find peer")
	}
	if ctxT.Err() != nil {
		t.Error("FindPeer should have failed before context expired")
	}
}

func TestFindPeer(t *testing.T) {
	// t.Skip("skipping test to debug another")
	if testing.Short() {
		t.SkipNow()
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dhts := setupDHTS(t, ctx, 4)
	defer func() {
		for i := 0; i < 4; i++ {
			dhts[i].Close()
			dhts[i].host.Close()
		}
	}()

	connect(t, ctx, dhts[0], dhts[1])
	connect(t, ctx, dhts[1], dhts[2])
	connect(t, ctx, dhts[1], dhts[3])

	ctxT, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	p, err := dhts[0].FindPeer(ctxT, dhts[2].PeerID())
	if err != nil {
		t.Fatal(err)
	}

	if p.ID == "" {
		t.Fatal("Failed to find peer.")
	}

	if p.ID != dhts[2].PeerID() {
		t.Fatal("Didnt find expected peer.")
	}
}

func TestFindPeerWithQueryFilter(t *testing.T) {
	// t.Skip("skipping test to debug another")
	if testing.Short() {
		t.SkipNow()
	}
	if runtime.GOOS == "windows" {
		t.Skip("skipping due to #760")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	filteredPeer, err := bhost.NewHost(swarmt.GenSwarm(t, swarmt.OptDisableReuseport), new(bhost.HostOpts))
	require.NoError(t, err)
	filteredPeer.Start()
	defer filteredPeer.Close()
	dhts := setupDHTS(t, ctx, 4, QueryFilter(func(_ interface{}, ai peer.AddrInfo) bool {
		return ai.ID != filteredPeer.ID()
	}))
	defer func() {
		for i := 0; i < 4; i++ {
			dhts[i].Close()
			dhts[i].host.Close()
		}
	}()

	connect(t, ctx, dhts[0], dhts[1])
	connect(t, ctx, dhts[1], dhts[2])
	connect(t, ctx, dhts[1], dhts[3])

	err = filteredPeer.Connect(ctx, peer.AddrInfo{
		ID:    dhts[2].host.ID(),
		Addrs: dhts[2].host.Addrs(),
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return len(dhts[2].host.Network().ConnsToPeer(filteredPeer.ID())) > 0
	}, 5*time.Millisecond, time.Millisecond, "failed to connect to peer")

	ctxT, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()

	p, err := dhts[0].FindPeer(ctxT, filteredPeer.ID())
	require.NoError(t, err)

	require.NotEmpty(t, p.ID, "Failed to find peer.")
	require.Equal(t, filteredPeer.ID(), p.ID, "Didnt find expected peer.")
}

func TestConnectCollision(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	runTimes := 10

	for rtime := 0; rtime < runTimes; rtime++ {
		logger.Info("Running Time: ", rtime)

		ctx, cancel := context.WithCancel(context.Background())

		dhtA := setupDHT(ctx, t, false)
		dhtB := setupDHT(ctx, t, false)

		addrA := dhtA.peerstore.Addrs(dhtA.self)[0]
		addrB := dhtB.peerstore.Addrs(dhtB.self)[0]

		peerA := dhtA.self
		peerB := dhtB.self

		errs := make(chan error)
		go func() {
			dhtA.peerstore.AddAddr(peerB, addrB, peerstore.TempAddrTTL)
			pi := peer.AddrInfo{ID: peerB}
			err := dhtA.host.Connect(ctx, pi)
			errs <- err
		}()
		go func() {
			dhtB.peerstore.AddAddr(peerA, addrA, peerstore.TempAddrTTL)
			pi := peer.AddrInfo{ID: peerA}
			err := dhtB.host.Connect(ctx, pi)
			errs <- err
		}()

		timeout := time.After(5 * time.Second)
		select {
		case e := <-errs:
			if e != nil {
				t.Fatal(e)
			}
		case <-timeout:
			t.Fatal("Timeout received!")
		}
		select {
		case e := <-errs:
			if e != nil {
				t.Fatal(e)
			}
		case <-timeout:
			t.Fatal("Timeout received!")
		}

		dhtA.Close()
		dhtB.Close()
		dhtA.host.Close()
		dhtB.host.Close()
		cancel()
	}
}

func TestBadProtoMessages(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	d := setupDHT(ctx, t, false)

	nilrec := new(pb.Message)
	if _, err := d.handlePutValue(ctx, "testpeer", nilrec); err == nil {
		t.Fatal("should have errored on nil record")
	}
}

func TestAtomicPut(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	d := setupDHT(ctx, t, false)
	d.Validator = testAtomicPutValidator{}

	// fnc to put a record
	key := "testkey"
	putRecord := func(value []byte) error {
		rec := record.MakePutRecord(key, value)
		pmes := pb.NewMessage(pb.Message_PUT_VALUE, rec.Key, 0)
		pmes.Record = rec
		_, err := d.handlePutValue(ctx, "testpeer", pmes)
		return err
	}

	// put a valid record
	if err := putRecord([]byte("valid")); err != nil {
		t.Fatal("should not have errored on a valid record")
	}

	// simultaneous puts for old & new values
	values := [][]byte{[]byte("newer1"), []byte("newer7"), []byte("newer3"), []byte("newer5")}
	var wg sync.WaitGroup
	for _, v := range values {
		wg.Add(1)
		go func(v []byte) {
			defer wg.Done()
			_ = putRecord(v) // we expect some of these to fail
		}(v)
	}
	wg.Wait()

	// get should return the newest value
	pmes := pb.NewMessage(pb.Message_GET_VALUE, []byte(key), 0)
	msg, err := d.handleGetValue(ctx, "testkey", pmes)
	if err != nil {
		t.Fatalf("should not have errored on final get, but got %+v", err)
	}
	if string(msg.GetRecord().Value) != "newer7" {
		t.Fatalf("Expected 'newer7' got '%s'", string(msg.GetRecord().Value))
	}
}

func TestClientModeConnect(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	a := setupDHT(ctx, t, false)
	b := setupDHT(ctx, t, true)

	connectNoSync(t, ctx, a, b)

	c := testCaseCids[0]
	p := peer.ID("TestPeer")
	a.ProviderStore().AddProvider(ctx, c.Hash(), peer.AddrInfo{ID: p})
	time.Sleep(time.Millisecond * 5) // just in case...

	provs, err := b.FindProviders(ctx, c)
	if err != nil {
		t.Fatal(err)
	}

	if len(provs) == 0 {
		t.Fatal("Expected to get a provider back")
	}

	if provs[0].ID != p {
		t.Fatal("expected it to be our test peer")
	}
	if a.routingTable.Find(b.self) != "" {
		t.Fatal("DHT clients should not be added to routing tables")
	}
	if b.routingTable.Find(a.self) == "" {
		t.Fatal("DHT server should have been added to the dht client's routing table")
	}
}

func TestInvalidServer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	a := setupDHT(ctx, t, false)
	b := setupDHT(ctx, t, true)

	// make b advertise all dht server protocols
	for _, proto := range a.serverProtocols {
		// Hang on every request.
		b.host.SetStreamHandler(proto, func(s network.Stream) {
			defer s.Reset() // nolint
			<-ctx.Done()
		})
	}

	connectNoSync(t, ctx, a, b)

	c := testCaseCids[0]
	p := peer.ID("TestPeer")
	a.ProviderStore().AddProvider(ctx, c.Hash(), peer.AddrInfo{ID: p})
	time.Sleep(time.Millisecond * 5) // just in case...

	provs, err := b.FindProviders(ctx, c)
	if err != nil {
		t.Fatal(err)
	}

	if len(provs) == 0 {
		t.Fatal("Expected to get a provider back")
	}

	if provs[0].ID != p {
		t.Fatal("expected it to be our test peer")
	}
	if a.routingTable.Find(b.self) != "" {
		t.Fatal("DHT clients should not be added to routing tables")
	}
	if b.routingTable.Find(a.self) == "" {
		t.Fatal("DHT server should have been added to the dht client's routing table")
	}
}

func TestClientModeFindPeer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	a := setupDHT(ctx, t, false)
	b := setupDHT(ctx, t, true)
	c := setupDHT(ctx, t, true)

	connectNoSync(t, ctx, b, a)
	connectNoSync(t, ctx, c, a)

	// Can't use `connect` because b and c are only clients.
	wait(t, ctx, b, a)
	wait(t, ctx, c, a)

	pi, err := c.FindPeer(ctx, b.self)
	if err != nil {
		t.Fatal(err)
	}
	if len(pi.Addrs) == 0 {
		t.Fatal("should have found addresses for node b")
	}

	err = c.host.Connect(ctx, pi)
	if err != nil {
		t.Fatal(err)
	}
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func TestFindPeerQueryMinimal(t *testing.T) {
	testFindPeerQuery(t, 2, 22, 1)
}

func TestFindPeerQuery(t *testing.T) {
	if detectrace.WithRace() {
		t.Skip("skipping due to race detector max goroutines")
	}

	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	testFindPeerQuery(t, 5, 40, 3)
}

// NOTE: You must have ATLEAST (minRTRefreshThreshold+1) test peers before using this.
func testFindPeerQuery(t *testing.T,
	bootstrappers, // Number of nodes connected to the querying node
	leafs, // Number of nodes that might be connected to from the bootstrappers
	bootstrapConns int, // Number of bootstrappers each leaf should connect to.
) {
	if runtime.GOOS == "windows" {
		t.Skip("skipping due to #760")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dhts := setupDHTS(t, ctx, 1+bootstrappers+leafs, BucketSize(4))
	defer func() {
		for _, d := range dhts {
			d.Close()
			d.host.Close()
		}
	}()

	t.Log("connecting")

	mrand := rand.New(rand.NewSource(42))
	guy := dhts[0]
	others := dhts[1:]
	for i := 0; i < leafs; i++ {
		for _, v := range mrand.Perm(bootstrappers)[:bootstrapConns] {
			connectNoSync(t, ctx, others[v], others[bootstrappers+i])
		}
	}

	for i := 0; i < bootstrappers; i++ {
		connectNoSync(t, ctx, guy, others[i])
	}

	t.Log("waiting for routing tables")

	// give some time for things to settle down
	waitForWellFormedTables(t, dhts, bootstrapConns, bootstrapConns, 5*time.Second)

	t.Log("refreshing")

	var wg sync.WaitGroup
	for _, dht := range dhts {
		wg.Add(1)
		go func(d *IpfsDHT) {
			<-d.RefreshRoutingTable()
			wg.Done()
		}(dht)
	}

	wg.Wait()

	t.Log("waiting for routing tables again")

	// Wait for refresh to work. At least one bucket should be full.
	waitForWellFormedTables(t, dhts, 4, 0, 5*time.Second)

	var peers []peer.ID
	for _, d := range others {
		peers = append(peers, d.PeerID())
	}

	t.Log("querying")

	val := "foobar"
	rtval := kb.ConvertKey(val)

	outpeers, err := guy.GetClosestPeers(ctx, val)
	require.NoError(t, err)

	sort.Sort(peer.IDSlice(outpeers))

	exp := kb.SortClosestPeers(peers, rtval)[:minInt(guy.bucketSize, len(peers))]
	t.Logf("got %d peers", len(outpeers))
	got := kb.SortClosestPeers(outpeers, rtval)

	assert.EqualValues(t, exp, got)
}

func TestFindClosestPeers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nDHTs := 30
	dhts := setupDHTS(t, ctx, nDHTs)
	defer func() {
		for i := 0; i < nDHTs; i++ {
			dhts[i].Close()
			defer dhts[i].host.Close()
		}
	}()

	t.Logf("connecting %d dhts in a ring", nDHTs)
	for i := 0; i < nDHTs; i++ {
		connect(t, ctx, dhts[i], dhts[(i+1)%len(dhts)])
	}

	querier := dhts[1]
	peers, err := querier.GetClosestPeers(ctx, "foo")
	if err != nil {
		t.Fatal(err)
	}

	if len(peers) < querier.beta {
		t.Fatalf("got wrong number of peers (got %d, expected at least %d)", len(peers), querier.beta)
	}
}

func TestFixLowPeers(t *testing.T) {
	ctx := context.Background()

	dhts := setupDHTS(t, ctx, minRTRefreshThreshold+5)

	defer func() {
		for _, d := range dhts {
			d.Close()
			d.Host().Close()
		}
	}()

	mainD := dhts[0]

	// connect it to everyone else
	for _, d := range dhts[1:] {
		mainD.peerstore.AddAddrs(d.self, d.peerstore.Addrs(d.self), peerstore.TempAddrTTL)
		require.NoError(t, mainD.Host().Connect(ctx, peer.AddrInfo{ID: d.self}))
	}

	waitForWellFormedTables(t, []*IpfsDHT{mainD}, minRTRefreshThreshold, minRTRefreshThreshold+4, 5*time.Second)

	// run a refresh on all of them
	for _, d := range dhts {
		err := <-d.RefreshRoutingTable()
		require.NoError(t, err)
	}

	// now remove peers from RT so threshold gets hit
	for _, d := range dhts[3:] {
		mainD.routingTable.RemovePeer(d.self)
	}

	// but we will still get enough peers in the RT because of fix low Peers
	waitForWellFormedTables(t, []*IpfsDHT{mainD}, minRTRefreshThreshold, minRTRefreshThreshold, 5*time.Second)
}

func TestProvideDisabled(t *testing.T) {
	k := testCaseCids[0]
	kHash := k.Hash()
	for i := 0; i < 3; i++ {
		enabledA := (i & 0x1) > 0
		enabledB := (i & 0x2) > 0
		t.Run(fmt.Sprintf("a=%v/b=%v", enabledA, enabledB), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			var optsA, optsB []Option
			optsA = append(optsA, ProtocolPrefix("/provMaybeDisabled"))
			optsB = append(optsB, ProtocolPrefix("/provMaybeDisabled"))

			if !enabledA {
				optsA = append(optsA, DisableProviders())
			}
			if !enabledB {
				optsB = append(optsB, DisableProviders())
			}

			dhtA := setupDHT(ctx, t, false, optsA...)
			dhtB := setupDHT(ctx, t, false, optsB...)

			defer dhtA.Close()
			defer dhtB.Close()
			defer dhtA.host.Close()
			defer dhtB.host.Close()

			connect(t, ctx, dhtA, dhtB)

			err := dhtB.Provide(ctx, k, true)
			if enabledB {
				if err != nil {
					t.Fatal("put should have succeeded on node B", err)
				}
			} else {
				if err != routing.ErrNotSupported {
					t.Fatal("should not have put the value to node B", err)
				}
				_, err = dhtB.FindProviders(ctx, k)
				if err != routing.ErrNotSupported {
					t.Fatal("get should have failed on node B")
				}
				provs, _ := dhtB.ProviderStore().GetProviders(ctx, kHash)
				if len(provs) != 0 {
					t.Fatal("node B should not have found local providers")
				}
			}

			provs, err := dhtA.FindProviders(ctx, k)
			if enabledA {
				if len(provs) != 0 {
					t.Fatal("node A should not have found providers")
				}
			} else {
				if err != routing.ErrNotSupported {
					t.Fatal("node A should not have found providers")
				}
			}
			provAddrs, _ := dhtA.ProviderStore().GetProviders(ctx, kHash)
			if len(provAddrs) != 0 {
				t.Fatal("node A should not have found local providers")
			}
		})
	}
}

func TestHandleRemotePeerProtocolChanges(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	os := []Option{
		testPrefix,
		Mode(ModeServer),
		NamespacedValidator("v", blankValidator{}),
		DisableAutoRefresh(),
	}

	// start host 1 that speaks dht v1
	hA, err := bhost.NewHost(swarmt.GenSwarm(t, swarmt.OptDisableReuseport), new(bhost.HostOpts))
	require.NoError(t, err)
	hA.Start()
	defer hA.Close()
	dhtA, err := New(ctx, hA, os...)
	require.NoError(t, err)
	defer dhtA.Close()

	// start host 2 that also speaks dht v1
	hB, err := bhost.NewHost(swarmt.GenSwarm(t, swarmt.OptDisableReuseport), new(bhost.HostOpts))
	require.NoError(t, err)
	hB.Start()
	defer hB.Close()
	dhtB, err := New(ctx, hB, os...)
	require.NoError(t, err)
	defer dhtB.Close()

	connect(t, ctx, dhtA, dhtB)

	// now assert both have each other in their RT
	waitForWellFormedTables(t, []*IpfsDHT{dhtA, dhtB}, 1, 1, 10*time.Second)

	// dhtB becomes a client
	require.NoError(t, dhtB.setMode(modeClient))

	// which means that dhtA should evict it from it's RT
	waitForWellFormedTables(t, []*IpfsDHT{dhtA}, 0, 0, 10*time.Second)

	// dhtB becomes a server
	require.NoError(t, dhtB.setMode(modeServer))

	// which means dhtA should have it in the RT again because of fixLowPeers
	waitForWellFormedTables(t, []*IpfsDHT{dhtA}, 1, 1, 10*time.Second)
}

func TestGetSetPluggedProtocol(t *testing.T) {
	t.Run("PutValue/GetValue - same protocol", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		os := []Option{
			ProtocolPrefix("/esh"),
			Mode(ModeServer),
			NamespacedValidator("v", blankValidator{}),
			DisableAutoRefresh(),
		}

		hA, err := bhost.NewHost(swarmt.GenSwarm(t, swarmt.OptDisableReuseport), new(bhost.HostOpts))
		require.NoError(t, err)
		hA.Start()
		defer hA.Close()
		dhtA, err := New(ctx, hA, os...)
		require.NoError(t, err)
		defer dhtA.Close()

		hB, err := bhost.NewHost(swarmt.GenSwarm(t, swarmt.OptDisableReuseport), new(bhost.HostOpts))
		require.NoError(t, err)
		hB.Start()
		defer hB.Close()
		dhtB, err := New(ctx, hB, os...)
		require.NoError(t, err)
		defer dhtB.Close()

		connect(t, ctx, dhtA, dhtB)

		ctxT, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		err = dhtA.PutValue(ctxT, "/v/cat", []byte("meow"))
		require.NoError(t, err)

		value, err := dhtB.GetValue(ctxT, "/v/cat")
		require.NoError(t, err)

		require.Equal(t, "meow", string(value))
	})

	t.Run("DHT routing table for peer A won't contain B if A and B don't use same protocol", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		hA, err := bhost.NewHost(swarmt.GenSwarm(t, swarmt.OptDisableReuseport), new(bhost.HostOpts))
		require.NoError(t, err)
		hA.Start()
		defer hA.Close()
		dhtA, err := New(ctx, hA, []Option{
			ProtocolPrefix("/esh"),
			Mode(ModeServer),
			NamespacedValidator("v", blankValidator{}),
			DisableAutoRefresh(),
		}...)
		require.NoError(t, err)
		defer dhtA.Close()

		hB, err := bhost.NewHost(swarmt.GenSwarm(t, swarmt.OptDisableReuseport), new(bhost.HostOpts))
		require.NoError(t, err)
		hB.Start()
		defer hB.Close()
		dhtB, err := New(ctx, hB, []Option{
			ProtocolPrefix("/lsr"),
			Mode(ModeServer),
			NamespacedValidator("v", blankValidator{}),
			DisableAutoRefresh(),
		}...)
		require.NoError(t, err)
		defer dhtB.Close()

		connectNoSync(t, ctx, dhtA, dhtB)

		// We don't expect connection notifications for A to reach B (or vice-versa), given
		// that they've been configured with different protocols - but we'll give them a
		// chance, anyhow.
		time.Sleep(time.Second * 2)

		err = dhtA.PutValue(ctx, "/v/cat", []byte("meow"))
		if err == nil || !strings.Contains(err.Error(), "failed to find any peer in table") {
			t.Fatalf("put should not have been able to find any peers in routing table, err:'%v'", err)
		}

		v, err := dhtB.GetValue(ctx, "/v/cat")
		if v != nil || err != routing.ErrNotFound {
			t.Fatalf("get should have failed from not being able to find the value, err: '%v'", err)
		}
	})
}

func TestPing(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ds := setupDHTS(t, ctx, 2)
	ds[0].Host().Peerstore().AddAddrs(ds[1].PeerID(), ds[1].Host().Addrs(), peerstore.AddressTTL)
	assert.NoError(t, ds[0].Ping(context.Background(), ds[1].PeerID()))
}

func TestClientModeAtInit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pinger := setupDHT(ctx, t, false)
	client := setupDHT(ctx, t, true)
	pinger.Host().Peerstore().AddAddrs(client.PeerID(), client.Host().Addrs(), peerstore.AddressTTL)
	err := pinger.Ping(context.Background(), client.PeerID())
	assert.True(t, errors.Is(err, multistream.ErrNotSupported[protocol.ID]{}))
}

func TestModeChange(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	clientOnly := setupDHT(ctx, t, true)
	clientToServer := setupDHT(ctx, t, true)
	clientOnly.Host().Peerstore().AddAddrs(clientToServer.PeerID(), clientToServer.Host().Addrs(), peerstore.AddressTTL)
	err := clientOnly.Ping(ctx, clientToServer.PeerID())
	assert.True(t, errors.Is(err, multistream.ErrNotSupported[protocol.ID]{}))
	err = clientToServer.setMode(modeServer)
	assert.Nil(t, err)
	err = clientOnly.Ping(ctx, clientToServer.PeerID())
	assert.Nil(t, err)
	err = clientToServer.setMode(modeClient)
	assert.Nil(t, err)
	err = clientOnly.Ping(ctx, clientToServer.PeerID())
	assert.NotNil(t, err)
}

func TestDynamicModeSwitching(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	prober := setupDHT(ctx, t, true)               // our test harness
	node := setupDHT(ctx, t, true, Mode(ModeAuto)) // the node under test
	prober.Host().Peerstore().AddAddrs(node.PeerID(), node.Host().Addrs(), peerstore.AddressTTL)
	if _, err := prober.Host().Network().DialPeer(ctx, node.PeerID()); err != nil {
		t.Fatal(err)
	}

	emitter, err := node.host.EventBus().Emitter(new(event.EvtLocalReachabilityChanged))
	if err != nil {
		t.Fatal(err)
	}

	assertDHTClient := func() {
		err = prober.Ping(ctx, node.PeerID())
		assert.True(t, errors.Is(err, multistream.ErrNotSupported[protocol.ID]{}))
		if l := len(prober.RoutingTable().ListPeers()); l != 0 {
			t.Errorf("expected routing table length to be 0; instead is %d", l)
		}
	}

	assertDHTServer := func() {
		err = prober.Ping(ctx, node.PeerID())
		assert.Nil(t, err)
		// the node should be in the RT for the prober
		// because the prober will call fixLowPeers when the node updates it's protocols
		if l := len(prober.RoutingTable().ListPeers()); l != 1 {
			t.Errorf("expected routing table length to be 1; instead is %d", l)
		}
	}

	err = emitter.Emit(event.EvtLocalReachabilityChanged{Reachability: network.ReachabilityPrivate})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(500 * time.Millisecond)

	assertDHTClient()

	err = emitter.Emit(event.EvtLocalReachabilityChanged{Reachability: network.ReachabilityPublic})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(500 * time.Millisecond)

	assertDHTServer()

	err = emitter.Emit(event.EvtLocalReachabilityChanged{Reachability: network.ReachabilityUnknown})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(500 * time.Millisecond)

	assertDHTClient()
}

func TestInvalidKeys(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nDHTs := 2
	dhts := setupDHTS(t, ctx, nDHTs)
	defer func() {
		for i := 0; i < nDHTs; i++ {
			dhts[i].Close()
			defer dhts[i].host.Close()
		}
	}()

	t.Logf("connecting %d dhts in a ring", nDHTs)
	for i := 0; i < nDHTs; i++ {
		connect(t, ctx, dhts[i], dhts[(i+1)%len(dhts)])
	}

	querier := dhts[0]
	_, err := querier.GetClosestPeers(ctx, "")
	if err == nil {
		t.Fatal("get closest peers should have failed")
	}

	_, err = querier.FindProviders(ctx, cid.Cid{})
	switch err {
	case routing.ErrNotFound, routing.ErrNotSupported, kb.ErrLookupFailure:
		t.Fatal("failed with the wrong error: ", err)
	case nil:
		t.Fatal("find providers should have failed")
	}

	_, err = querier.FindPeer(ctx, peer.ID(""))
	if err != peer.ErrEmptyPeerID {
		t.Fatal("expected to fail due to the empty peer ID")
	}

	_, err = querier.GetValue(ctx, "")
	if err == nil {
		t.Fatal("expected to have failed")
	}

	err = querier.PutValue(ctx, "", []byte("foobar"))
	if err == nil {
		t.Fatal("expected to have failed")
	}
}

func TestV1ProtocolOverride(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	d1 := setupDHT(ctx, t, false, V1ProtocolOverride("/myproto"))
	d2 := setupDHT(ctx, t, false, V1ProtocolOverride("/myproto"))
	d3 := setupDHT(ctx, t, false, V1ProtocolOverride("/myproto2"))
	d4 := setupDHT(ctx, t, false)

	dhts := []*IpfsDHT{d1, d2, d3, d4}

	for i, dout := range dhts {
		for _, din := range dhts[i+1:] {
			connectNoSync(t, ctx, dout, din)
		}
	}

	wait(t, ctx, d1, d2)
	wait(t, ctx, d2, d1)

	time.Sleep(time.Second)

	if d1.RoutingTable().Size() != 1 || d2.routingTable.Size() != 1 {
		t.Fatal("should have one peer in the routing table")
	}

	if d3.RoutingTable().Size() > 0 || d4.RoutingTable().Size() > 0 {
		t.Fatal("should have an empty routing table")
	}
}

func TestRoutingFilter(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nDHTs := 2
	dhts := setupDHTS(t, ctx, nDHTs)
	defer func() {
		for i := 0; i < nDHTs; i++ {
			dhts[i].Close()
			defer dhts[i].host.Close()
		}
	}()
	dhts[0].routingTablePeerFilter = PublicRoutingTableFilter

	connectNoSync(t, ctx, dhts[0], dhts[1])
	wait(t, ctx, dhts[1], dhts[0])

	select {
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	case <-time.After(time.Millisecond * 200):
	}
}

func TestBootStrapWhenRTIsEmpty(t *testing.T) {
	if detectrace.WithRace() {
		t.Skip("skipping timing dependent test when race detector is running")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// create three boostrap peers each of which is connected to 1 other peer.
	nBootStraps := 3
	bootstrappers := setupDHTS(t, ctx, nBootStraps)
	defer func() {
		for i := 0; i < nBootStraps; i++ {
			bootstrappers[i].Close()
			defer bootstrappers[i].host.Close()
		}
	}()

	bootstrapcons := setupDHTS(t, ctx, nBootStraps)
	defer func() {
		for i := 0; i < nBootStraps; i++ {
			bootstrapcons[i].Close()
			defer bootstrapcons[i].host.Close()
		}
	}()
	for i := 0; i < nBootStraps; i++ {
		connect(t, ctx, bootstrappers[i], bootstrapcons[i])
	}

	// convert the bootstrap addresses to a p2p address
	bootstrapAddrs := make([]peer.AddrInfo, nBootStraps)
	for i := 0; i < nBootStraps; i++ {
		b := peer.AddrInfo{
			ID:    bootstrappers[i].self,
			Addrs: bootstrappers[i].host.Addrs(),
		}
		bootstrapAddrs[i] = b
	}

	{

		// ----------------
		// We will initialize a DHT with 1 bootstrapper, connect it to another DHT,
		// then remove the latter from the Routing Table
		// This should add the bootstrap peer and the peer that the bootstrap peer is conencted to
		// to it's Routing Table.
		// AutoRefresh needs to be enabled for this.

		h1, err := bhost.NewHost(swarmt.GenSwarm(t, swarmt.OptDisableReuseport), new(bhost.HostOpts))
		require.NoError(t, err)
		h1.Start()
		dht1, err := New(
			ctx,
			h1,
			testPrefix,
			NamespacedValidator("v", blankValidator{}),
			Mode(ModeServer),
			BootstrapPeers(bootstrapAddrs[0]),
		)
		require.NoError(t, err)
		dht2 := setupDHT(ctx, t, false)
		defer func() {
			dht1.host.Close()
			dht2.host.Close()
			dht1.Close()
			dht2.Close()
		}()
		connect(t, ctx, dht1, dht2)
		require.NoError(t, dht2.Close())
		require.NoError(t, dht2.host.Close())
		require.NoError(t, dht1.host.Network().ClosePeer(dht2.self))
		dht1.routingTable.RemovePeer(dht2.self)
		require.NotContains(t, dht2.self, dht1.routingTable.ListPeers())
		require.Eventually(t, func() bool {
			return dht1.routingTable.Size() == 2 && dht1.routingTable.Find(bootstrappers[0].self) != "" &&
				dht1.routingTable.Find(bootstrapcons[0].self) != ""
		}, 5*time.Second, 500*time.Millisecond)

	}

	{

		// ----------------
		// We will initialize a DHT with 2 bootstrappers, connect it to another DHT,
		// then remove the DHT handler from the other DHT which should make the first DHT's
		// routing table empty.
		// This should add the bootstrap peers and the peer thats the bootstrap peers are connected to
		// to it's Routing Table.
		// AutoRefresh needs to be enabled for this.
		h1, err := bhost.NewHost(swarmt.GenSwarm(t, swarmt.OptDisableReuseport), new(bhost.HostOpts))
		require.NoError(t, err)
		h1.Start()
		dht1, err := New(
			ctx,
			h1,
			testPrefix,
			NamespacedValidator("v", blankValidator{}),
			Mode(ModeServer),
			BootstrapPeers(bootstrapAddrs[1], bootstrapAddrs[2]),
		)
		require.NoError(t, err)

		dht2 := setupDHT(ctx, t, false)
		connect(t, ctx, dht1, dht2)
		defer func() {
			dht1.host.Close()
			dht2.host.Close()
			dht1.Close()
			dht2.Close()
		}()
		connect(t, ctx, dht1, dht2)
		require.NoError(t, dht2.setMode(modeClient))

		require.Eventually(t, func() bool {
			rt := dht1.routingTable

			return rt.Size() == 4 && rt.Find(bootstrappers[1].self) != "" &&
				rt.Find(bootstrappers[2].self) != "" && rt.Find(bootstrapcons[1].self) != "" && rt.Find(bootstrapcons[2].self) != ""
		}, 5*time.Second, 500*time.Millisecond)
	}
}

func TestBootstrapPeersFunc(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var lock sync.Mutex

	bootstrapFuncA := func() []peer.AddrInfo {
		return []peer.AddrInfo{}
	}
	dhtA := setupDHT(ctx, t, false, BootstrapPeersFunc(bootstrapFuncA))

	bootstrapPeersB := []peer.AddrInfo{}
	bootstrapFuncB := func() []peer.AddrInfo {
		lock.Lock()
		defer lock.Unlock()
		return bootstrapPeersB
	}

	dhtB := setupDHT(ctx, t, false, BootstrapPeersFunc(bootstrapFuncB))
	require.Equal(t, 0, len(dhtB.host.Network().Peers()))

	addrA := peer.AddrInfo{
		ID:    dhtA.self,
		Addrs: dhtA.host.Addrs(),
	}

	lock.Lock()
	bootstrapPeersB = []peer.AddrInfo{addrA}
	lock.Unlock()

	dhtB.fixLowPeers()
	require.NotEqual(t, 0, len(dhtB.host.Network().Peers()))
}

func TestPreconnectedNodes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opts := []Option{
		testPrefix,
		DisableAutoRefresh(),
		Mode(ModeServer),
	}

	// Create hosts
	h1, err := bhost.NewHost(swarmt.GenSwarm(t, swarmt.OptDisableReuseport), new(bhost.HostOpts))
	require.NoError(t, err)
	h1.Start()
	defer h1.Close()
	h2, err := bhost.NewHost(swarmt.GenSwarm(t, swarmt.OptDisableReuseport), new(bhost.HostOpts))
	require.NoError(t, err)
	h2.Start()
	defer h2.Close()

	// Setup first DHT
	d1, err := New(ctx, h1, opts...)
	require.NoError(t, err)
	defer d1.Close()

	// Connect the first host to the second
	err = h1.Connect(ctx, peer.AddrInfo{ID: h2.ID(), Addrs: h2.Addrs()})
	require.NoError(t, err)

	// Wait until we know identify has completed by checking for supported protocols
	// TODO: Is this needed? Could we do h2.Connect(h1) and that would wait for identify to complete.
	require.Eventually(t, func() bool {
		h1Protos, err := h2.Peerstore().SupportsProtocols(h1.ID(), d1.protocols...)
		require.NoError(t, err)

		return len(h1Protos) > 0
	}, 10*time.Second, time.Millisecond)

	// Setup the second DHT
	d2, err := New(ctx, h2, opts...)
	require.NoError(t, err)
	defer h2.Close()

	connect(t, ctx, d1, d2)

	// See if it works
	peers, err := d2.GetClosestPeers(ctx, "testkey")
	require.NoError(t, err)

	require.Equal(t, len(peers), 1, "why is there more than one peer?")
	require.Equal(t, h1.ID(), peers[0], "could not find peer")
}

func TestAddrFilter(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// generate a bunch of addresses
	publicAddrs := []ma.Multiaddr{
		ma.StringCast("/ip4/1.2.3.1/tcp/123"),
		ma.StringCast("/ip4/160.160.160.160/tcp/1600"),
		ma.StringCast("/ip6/2001::10/tcp/123"),
	}
	privAddrs := []ma.Multiaddr{
		ma.StringCast("/ip4/192.168.1.100/tcp/123"),
		ma.StringCast("/ip4/172.16.10.10/tcp/123"),
		ma.StringCast("/ip4/10.10.10.10/tcp/123"),
		ma.StringCast("/ip6/fc00::10/tcp/123"),
	}
	loopbackAddrs := []ma.Multiaddr{
		ma.StringCast("/ip4/127.0.0.100/tcp/123"),
		ma.StringCast("/ip6/::1/tcp/123"),
	}

	allAddrs := append(publicAddrs, privAddrs...)
	allAddrs = append(allAddrs, loopbackAddrs...)

	// generate different address filters
	acceptAllFilter := AddressFilter(func(addrs []ma.Multiaddr) []ma.Multiaddr {
		return addrs
	})
	rejectAllFilter := AddressFilter(func(addrs []ma.Multiaddr) []ma.Multiaddr {
		return []ma.Multiaddr{}
	})
	publicIpFilter := AddressFilter(func(addrs []ma.Multiaddr) []ma.Multiaddr {
		return ma.FilterAddrs(addrs, manet.IsPublicAddr)
	})
	localIpFilter := AddressFilter(func(addrs []ma.Multiaddr) []ma.Multiaddr {
		return ma.FilterAddrs(addrs, func(a ma.Multiaddr) bool { return !manet.IsIPLoopback(a) })
	})

	// generate peerid for "remote" peer
	_, pub, err := crypto.GenerateKeyPair(
		crypto.Ed25519, // Select your key type. Ed25519 are nice short
		-1,             // Select key length when possible (i.e. RSA).
	)
	require.NoError(t, err)
	peerid, err := peer.IDFromPublicKey(pub)
	require.NoError(t, err)

	// DHT accepting all addresses
	d0 := setupDHT(ctx, t, false, acceptAllFilter)

	// peerstore should only contain self
	require.Equal(t, 1, d0.host.Peerstore().Peers().Len())

	d0.maybeAddAddrs(peerid, allAddrs, time.Minute)
	require.Equal(t, 2, d0.host.Peerstore().Peers().Len())
	for _, a := range allAddrs {
		// check that the peerstore contains all addresses of the remote peer
		require.Contains(t, d0.host.Peerstore().Addrs(peerid), a)
	}

	// DHT rejecting all addresses
	d1 := setupDHT(ctx, t, false, rejectAllFilter)
	d1.maybeAddAddrs(peerid, allAddrs, time.Minute)
	// remote peer should not be added to peerstore (all addresses rejected)
	require.Equal(t, 1, d1.host.Peerstore().Peers().Len())

	// DHT accepting only public addresses
	d2 := setupDHT(ctx, t, false, publicIpFilter)
	d2.maybeAddAddrs(peerid, allAddrs, time.Minute)
	for _, a := range publicAddrs {
		// check that the peerstore contains only public addresses of the remote peer
		require.Contains(t, d2.host.Peerstore().Addrs(peerid), a)
	}
	require.Equal(t, len(publicAddrs), len(d2.host.Peerstore().Addrs(peerid)))

	// DHT accepting only non-loopback addresses
	d3 := setupDHT(ctx, t, false, localIpFilter)
	d3.maybeAddAddrs(peerid, allAddrs, time.Minute)
	for _, a := range publicAddrs {
		// check that the peerstore contains only non-loopback addresses of the remote peer
		require.Contains(t, d3.host.Peerstore().Addrs(peerid), a)
	}
	for _, a := range privAddrs {
		// check that the peerstore contains only non-loopback addresses of the remote peer
		require.Contains(t, d3.host.Peerstore().Addrs(peerid), a)
	}
	require.Equal(t, len(publicAddrs)+len(privAddrs), len(d3.host.Peerstore().Addrs(peerid)))
}
