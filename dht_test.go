package dht

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p-core/event"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/peerstore"
	"github.com/libp2p/go-libp2p-core/routing"
	"github.com/multiformats/go-multihash"
	"github.com/multiformats/go-multistream"

	"golang.org/x/xerrors"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pb "github.com/libp2p/go-libp2p-kad-dht/pb"

	"github.com/ipfs/go-cid"
	u "github.com/ipfs/go-ipfs-util"
	kb "github.com/libp2p/go-libp2p-kbucket"
	"github.com/libp2p/go-libp2p-record"
	swarmt "github.com/libp2p/go-libp2p-swarm/testing"
	"github.com/libp2p/go-libp2p-testing/ci"
	travisci "github.com/libp2p/go-libp2p-testing/ci/travis"
	bhost "github.com/libp2p/go-libp2p/p2p/host/basic"
	ma "github.com/multiformats/go-multiaddr"
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

type testValidator struct{}

func (testValidator) Select(_ string, bs [][]byte) (int, error) {
	index := -1
	for i, b := range bs {
		if bytes.Equal(b, []byte("newer")) {
			index = i
		} else if bytes.Equal(b, []byte("valid")) {
			if index == -1 {
				index = i
			}
		}
	}
	if index == -1 {
		return -1, errors.New("no rec found")
	}
	return index, nil
}
func (testValidator) Validate(_ string, b []byte) error {
	if bytes.Equal(b, []byte("expired")) {
		return errors.New("expired")
	}
	return nil
}

type testAtomicPutValidator struct {
	testValidator
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

	d, err := New(
		ctx,
		bhost.New(swarmt.GenSwarm(t, ctx, swarmt.OptDisableReuseport)),
		append(baseOpts, options...)...,
	)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func setupDHTS(t *testing.T, ctx context.Context, n int) []*IpfsDHT {
	addrs := make([]ma.Multiaddr, n)
	dhts := make([]*IpfsDHT, n)
	peers := make([]peer.ID, n)

	sanityAddrsMap := make(map[string]struct{})
	sanityPeersMap := make(map[string]struct{})

	for i := 0; i < n; i++ {
		dhts[i] = setupDHT(ctx, t, false)
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

	a.peerstore.AddAddrs(idB, addrB, peerstore.TempAddrTTL)
	pi := peer.AddrInfo{ID: idB}
	if err := a.host.Connect(ctx, pi); err != nil {
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

	// late connect

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

	dhtA.Validator.(record.NamespacedValidator)["v"] = testValidator{}
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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dhtA := setupDHT(ctx, t, false)
	dhtB := setupDHT(ctx, t, false)

	defer dhtA.Close()
	defer dhtB.Close()
	defer dhtA.host.Close()
	defer dhtB.host.Close()

	connect(t, ctx, dhtA, dhtB)

	dhtA.Validator.(record.NamespacedValidator)["v"] = testValidator{}
	dhtB.Validator.(record.NamespacedValidator)["v"] = testValidator{}

	ctxT, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()

	err := dhtA.PutValue(ctxT, "/v/hello", []byte("valid"))
	if err != nil {
		t.Error(err)
	}

	ctxT, cancel = context.WithTimeout(ctx, time.Second*2)
	defer cancel()
	valCh, err := dhtA.SearchValue(ctxT, "/v/hello", Quorum(-1))
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

func TestGetValues(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dhtA := setupDHT(ctx, t, false)
	dhtB := setupDHT(ctx, t, false)

	defer dhtA.Close()
	defer dhtB.Close()
	defer dhtA.host.Close()
	defer dhtB.host.Close()

	connect(t, ctx, dhtA, dhtB)

	ctxT, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()

	err := dhtB.PutValue(ctxT, "/v/hello", []byte("newer"))
	if err != nil {
		t.Error(err)
	}

	err = dhtA.PutValue(ctxT, "/v/hello", []byte("valid"))
	if err != nil {
		t.Error(err)
	}

	ctxT, cancel = context.WithTimeout(ctx, time.Second*2)
	defer cancel()
	vals, err := dhtA.GetValues(ctxT, "/v/hello", 16)
	if err != nil {
		t.Fatal(err)
	}

	if len(vals) != 2 {
		t.Fatalf("expected to get 2 values, got %d", len(vals))
	}

	sort.Slice(vals, func(i, j int) bool { return string(vals[i].Val) < string(vals[j].Val) })

	if string(vals[0].Val) != "valid" {
		t.Errorf("unexpected vals[0]: %s", string(vals[0].Val))
	}
	if string(vals[1].Val) != "valid" {
		t.Errorf("unexpected vals[1]: %s", string(vals[1].Val))
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
	dhtB.Validator.(record.NamespacedValidator)["v"] = testValidator{}

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

func TestInvalidMessageSenderTracking(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dht := setupDHT(ctx, t, false)
	defer dht.Close()

	foo := peer.ID("asdasd")
	_, err := dht.messageSenderForPeer(ctx, foo)
	if err == nil {
		t.Fatal("that shouldnt have succeeded")
	}

	dht.smlk.Lock()
	mscnt := len(dht.strmap)
	dht.smlk.Unlock()

	if mscnt > 0 {
		t.Fatal("should have no message senders in map")
	}
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
		case <-ctxT.Done():
			t.Fatal("Did not get a provider back.")
		}
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
			provs := dhts[i].providers.GetProviders(ctx, c.Hash())
			if len(provs) > 0 {
				t.Fatal("shouldnt know this")
			}
		}
	}
}

// if minPeers or avgPeers is 0, dont test for it.
func waitForWellFormedTables(t *testing.T, dhts []*IpfsDHT, minPeers, avgPeers int, timeout time.Duration) bool {
	// test "well-formed-ness" (>= minPeers peers in every routing table)

	checkTables := func() bool {
		totalPeers := 0
		for _, dht := range dhts {
			rtlen := dht.routingTable.Size()
			totalPeers += rtlen
			if minPeers > 0 && rtlen < minPeers {
				//t.Logf("routing table for %s only has %d peers (should have >%d)", dht.self, rtlen, minPeers)
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

	timeoutA := time.After(timeout)
	for {
		select {
		case <-timeoutA:
			logger.Debugf("did not reach well-formed routing tables by %s", timeout)
			return false // failed
		case <-time.After(5 * time.Millisecond):
			if checkTables() {
				return true // succeeded
			}
		}
	}
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
	ctxT, cancelT := context.WithTimeout(ctx, 5*time.Second)
	defer cancelT()

	go func() {
		for ctxT.Err() == nil {
			bootstrap(t, ctxT, dhts)

			// wait a bit.
			select {
			case <-time.After(50 * time.Millisecond):
				continue // being explicit
			case <-ctxT.Done():
				return
			}
		}
	}()

	waitForWellFormedTables(t, dhts, 7, 10, 20*time.Second)
	cancelT()

	if u.Debug {
		// the routing tables should be full now. let's inspect them.
		printRoutingTables(dhts)
	}
}

func TestRefreshBelowMinRTThreshold(t *testing.T) {
	ctx := context.Background()

	// enable auto bootstrap on A
	dhtA, err := New(
		ctx,
		bhost.New(swarmt.GenSwarm(t, ctx, swarmt.OptDisableReuseport)),
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
	waitForWellFormedTables(t, []*IpfsDHT{dhtA}, 4, 4, 20*time.Second)
	assert.Equal(t, dhtE.self, dhtA.routingTable.Find(dhtE.self), "A's routing table should have peer E!")
}

func TestPeriodicRefresh(t *testing.T) {
	if ci.IsRunning() {
		t.Skip("skipping on CI. highly timing dependent")
	}
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
	for _, dht := range dhts {
		dht.RefreshRoutingTable()
	}

	// this is async, and we dont know when it's finished with one cycle, so keep checking
	// until the routing tables look better, or some long timeout for the failure case.
	waitForWellFormedTables(t, dhts, 7, 10, 20*time.Second)

	if u.Debug {
		printRoutingTables(dhts)
	}
}

func TestProvidesMany(t *testing.T) {
	t.Skip("this test doesn't work")
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

func TestConnectCollision(t *testing.T) {
	// t.Skip("skipping test to debug another")
	if testing.Short() {
		t.SkipNow()
	}
	if travisci.IsRunning() {
		t.Skip("Skipping on Travis-CI.")
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
			putRecord(v)
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
	a.providers.AddProvider(ctx, c.Hash(), p)
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
	} else {
		return b
	}
}

func TestFindPeerQueryMinimal(t *testing.T) {
	testFindPeerQuery(t, 2, 22, 11)
}

func TestFindPeerQuery(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	if curFileLimit() < 1024 {
		t.Skip("insufficient file descriptors available")
	}
	testFindPeerQuery(t, 20, 80, 16)
}

func testFindPeerQuery(t *testing.T,
	bootstrappers, // Number of nodes connected to the querying node
	leafs, // Number of nodes that might be connected to from the bootstrappers
	bootstrapperLeafConns int, // Number of connections each bootstrapper has to the leaf nodes
) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dhts := setupDHTS(t, ctx, 1+bootstrappers+leafs)
	defer func() {
		for _, d := range dhts {
			d.Close()
			d.host.Close()
		}
	}()

	mrand := rand.New(rand.NewSource(42))
	guy := dhts[0]
	others := dhts[1:]
	for i := 0; i < bootstrappers; i++ {
		for j := 0; j < bootstrapperLeafConns; j++ {
			v := mrand.Intn(leafs)
			connect(t, ctx, others[i], others[bootstrappers+v])
		}
	}

	for i := 0; i < bootstrappers; i++ {
		connect(t, ctx, guy, others[i])
	}

	var reachableIds []peer.ID
	for i, d := range dhts {
		lp := len(d.host.Network().Peers())
		//t.Log(i, lp)
		if i != 0 && lp > 0 {
			reachableIds = append(reachableIds, d.PeerID())
		}
	}
	t.Logf("%d reachable ids", len(reachableIds))

	val := "foobar"
	rtval := kb.ConvertKey(val)

	rtablePeers := guy.routingTable.NearestPeers(rtval, guy.alpha)
	assert.Len(t, rtablePeers, minInt(bootstrappers, guy.alpha))

	assert.Len(t, guy.host.Network().Peers(), bootstrappers)

	out, err := guy.GetClosestPeers(ctx, val)
	require.NoError(t, err)

	var outpeers []peer.ID
	for p := range out {
		outpeers = append(outpeers, p)
	}

	sort.Sort(peer.IDSlice(outpeers))

	exp := kb.SortClosestPeers(reachableIds, rtval)[:minInt(guy.bucketSize, len(reachableIds))]
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

	var out []peer.ID
	for p := range peers {
		out = append(out, p)
	}

	if len(out) != querier.bucketSize {
		t.Fatalf("got wrong number of peers (got %d, expected %d)", len(out), querier.bucketSize)
	}
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

			var (
				optsA, optsB []Option
			)
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
				provs := dhtB.providers.GetProviders(ctx, kHash)
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
			provAddrs := dhtA.providers.GetProviders(ctx, kHash)
			if len(provAddrs) != 0 {
				t.Fatal("node A should not have found local providers")
			}
		})
	}
}

func TestHandleRemotePeerProtocolChanges(t *testing.T) {
	ctx := context.Background()
	os := []Option{
		testPrefix,
		Mode(ModeServer),
		NamespacedValidator("v", blankValidator{}),
		DisableAutoRefresh(),
	}

	// start host 1 that speaks dht v1
	dhtA, err := New(ctx, bhost.New(swarmt.GenSwarm(t, ctx, swarmt.OptDisableReuseport)), os...)
	require.NoError(t, err)
	defer dhtA.Close()

	// start host 2 that also speaks dht v1
	dhtB, err := New(ctx, bhost.New(swarmt.GenSwarm(t, ctx, swarmt.OptDisableReuseport)), os...)
	require.NoError(t, err)
	defer dhtB.Close()

	connect(t, ctx, dhtA, dhtB)

	// now assert both have each other in their RT
	require.True(t, waitForWellFormedTables(t, []*IpfsDHT{dhtA, dhtB}, 1, 1, 10*time.Second), "both RT should have one peer each")

	// dhtB becomes a client
	require.NoError(t, dhtB.setMode(modeClient))

	// which means that dhtA should evict it from it's RT
	require.True(t, waitForWellFormedTables(t, []*IpfsDHT{dhtA}, 0, 0, 10*time.Second), "dHTA routing table should have 0 peers")

	// dhtB becomes a server
	require.NoError(t, dhtB.setMode(modeServer))

	// which means dhtA should have it in the RT again
	require.True(t, waitForWellFormedTables(t, []*IpfsDHT{dhtA}, 1, 1, 10*time.Second), "dHTA routing table should have 1 peers")
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

		dhtA, err := New(ctx, bhost.New(swarmt.GenSwarm(t, ctx, swarmt.OptDisableReuseport)), os...)
		if err != nil {
			t.Fatal(err)
		}

		dhtB, err := New(ctx, bhost.New(swarmt.GenSwarm(t, ctx, swarmt.OptDisableReuseport)), os...)
		if err != nil {
			t.Fatal(err)
		}

		connect(t, ctx, dhtA, dhtB)

		ctxT, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		if err := dhtA.PutValue(ctxT, "/v/cat", []byte("meow")); err != nil {
			t.Fatal(err)
		}

		value, err := dhtB.GetValue(ctxT, "/v/cat")
		if err != nil {
			t.Fatal(err)
		}

		if string(value) != "meow" {
			t.Fatalf("Expected 'meow' got '%s'", string(value))
		}
	})

	t.Run("DHT routing table for peer A won't contain B if A and B don't use same protocol", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		dhtA, err := New(ctx, bhost.New(swarmt.GenSwarm(t, ctx, swarmt.OptDisableReuseport)), []Option{
			ProtocolPrefix("/esh"),
			Mode(ModeServer),
			NamespacedValidator("v", blankValidator{}),
			DisableAutoRefresh(),
		}...)
		if err != nil {
			t.Fatal(err)
		}

		dhtB, err := New(ctx, bhost.New(swarmt.GenSwarm(t, ctx, swarmt.OptDisableReuseport)), []Option{
			ProtocolPrefix("/lsr"),
			Mode(ModeServer),
			NamespacedValidator("v", blankValidator{}),
			DisableAutoRefresh(),
		}...)
		if err != nil {
			t.Fatal(err)
		}

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
	assert.True(t, xerrors.Is(err, multistream.ErrNotSupported))
}

func TestModeChange(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	clientOnly := setupDHT(ctx, t, true)
	clientToServer := setupDHT(ctx, t, true)
	clientOnly.Host().Peerstore().AddAddrs(clientToServer.PeerID(), clientToServer.Host().Addrs(), peerstore.AddressTTL)
	err := clientOnly.Ping(ctx, clientToServer.PeerID())
	assert.True(t, xerrors.Is(err, multistream.ErrNotSupported))
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
		assert.True(t, xerrors.Is(err, multistream.ErrNotSupported))
		if l := len(prober.RoutingTable().ListPeers()); l != 0 {
			t.Errorf("expected routing table length to be 0; instead is %d", l)
		}
	}

	assertDHTServer := func() {
		err = prober.Ping(ctx, node.PeerID())
		assert.Nil(t, err)
		if l := len(prober.RoutingTable().ListPeers()); l != 1 {
			t.Errorf("expected routing table length to be 1; instead is %d", l)
		}
	}

	emitter.Emit(event.EvtLocalReachabilityChanged{Reachability: network.ReachabilityPrivate})
	time.Sleep(500 * time.Millisecond)

	assertDHTClient()

	emitter.Emit(event.EvtLocalReachabilityChanged{Reachability: network.ReachabilityPublic})
	time.Sleep(500 * time.Millisecond)

	assertDHTServer()

	emitter.Emit(event.EvtLocalReachabilityChanged{Reachability: network.ReachabilityUnknown})
	time.Sleep(500 * time.Millisecond)

	assertDHTClient()
}

func TestProtocolUpgrade(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	os := []Option{
		Mode(ModeServer),
		NamespacedValidator("v", blankValidator{}),
		DisableAutoRefresh(),
		DisjointPaths(1),
	}

	// This test verifies that we can have a node serving both old and new DHTs that will respond as a server to the old
	// DHT, but only act as a client of the new DHT. In it's capacity as a server it should also only tell queriers
	// about other DHT servers in the new DHT.

	dhtA, err := New(ctx, bhost.New(swarmt.GenSwarm(t, ctx, swarmt.OptDisableReuseport)),
		append([]Option{testPrefix}, os...)...)
	if err != nil {
		t.Fatal(err)
	}

	dhtB, err := New(ctx, bhost.New(swarmt.GenSwarm(t, ctx, swarmt.OptDisableReuseport)),
		append([]Option{testPrefix}, os...)...)
	if err != nil {
		t.Fatal(err)
	}

	dhtC, err := New(ctx, bhost.New(swarmt.GenSwarm(t, ctx, swarmt.OptDisableReuseport)),
		append([]Option{testPrefix, customProtocols(kad1)}, os...)...)
	if err != nil {
		t.Fatal(err)
	}

	connect(t, ctx, dhtA, dhtB)
	connectNoSync(t, ctx, dhtA, dhtC)
	wait(t, ctx, dhtC, dhtA)

	if sz := dhtA.RoutingTable().Size(); sz != 1 {
		t.Fatalf("Expected routing table to be of size %d got %d", 1, sz)
	}

	ctxT, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	if err := dhtB.PutValue(ctxT, "/v/bat", []byte("screech")); err != nil {
		t.Fatal(err)
	}

	value, err := dhtC.GetValue(ctxT, "/v/bat")
	if err != nil {
		t.Fatal(err)
	}

	if string(value) != "screech" {
		t.Fatalf("Expected 'screech' got '%s'", string(value))
	}

	if err := dhtC.PutValue(ctxT, "/v/cat", []byte("meow")); err != nil {
		t.Fatal(err)
	}

	value, err = dhtB.GetValue(ctxT, "/v/cat")
	if err != nil {
		t.Fatal(err)
	}

	if string(value) != "meow" {
		t.Fatalf("Expected 'meow' got '%s'", string(value))
	}

	// Add record into local DHT only
	rec := record.MakePutRecord("/v/crow", []byte("caw"))
	rec.TimeReceived = u.FormatRFC3339(time.Now())
	err = dhtC.putLocal(string(rec.Key), rec)
	if err != nil {
		t.Fatal(err)
	}

	value, err = dhtB.GetValue(ctxT, "/v/crow")
	switch err {
	case nil:
		t.Fatalf("should not have been able to find value for %s", "/v/crow")
	case routing.ErrNotFound:
	default:
		t.Fatal(err)
	}

	// Add record into local DHT only
	rec = record.MakePutRecord("/v/bee", []byte("buzz"))
	rec.TimeReceived = u.FormatRFC3339(time.Now())
	err = dhtB.putLocal(string(rec.Key), rec)
	if err != nil {
		t.Fatal(err)
	}

	value, err = dhtC.GetValue(ctxT, "/v/bee")
	if err != nil {
		t.Fatal(err)
	}

	if string(value) != "buzz" {
		t.Fatalf("Expected 'buzz' got '%s'", string(value))
	}
}
