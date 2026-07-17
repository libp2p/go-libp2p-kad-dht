package fullrt

import (
	"context"
	"crypto/rand"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/libp2p/go-libp2p"
	kaddht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p-kad-dht/amino"
	"github.com/libp2p/go-libp2p-kad-dht/crawler"
	dht_pb "github.com/libp2p/go-libp2p-kad-dht/pb"
	"github.com/libp2p/go-libp2p-kad-dht/records"
	record "github.com/libp2p/go-libp2p-record"
	kadkey "github.com/libp2p/go-libp2p-xor/key"
	"github.com/libp2p/go-libp2p-xor/trie"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDivideByChunkSize(t *testing.T) {
	var keys []peer.ID
	for i := range 10 {
		keys = append(keys, peer.ID(strconv.Itoa(i)))
	}

	convertToStrings := func(peers []peer.ID) []string {
		var out []string
		for _, p := range peers {
			out = append(out, string(p))
		}
		return out
	}

	pidsEquals := func(a, b []string) bool {
		if len(a) != len(b) {
			return false
		}
		for i, v := range a {
			if v != b[i] {
				return false
			}
		}
		return true
	}

	t.Run("Divides", func(t *testing.T) {
		gr := divideByChunkSize(keys, 5)
		if len(gr) != 2 {
			t.Fatal("incorrect number of groups")
		}
		if g1, expected := convertToStrings(gr[0]), []string{"0", "1", "2", "3", "4"}; !pidsEquals(g1, expected) {
			t.Fatalf("expected %v, got %v", expected, g1)
		}
		if g2, expected := convertToStrings(gr[1]), []string{"5", "6", "7", "8", "9"}; !pidsEquals(g2, expected) {
			t.Fatalf("expected %v, got %v", expected, g2)
		}
	})
	t.Run("Remainder", func(t *testing.T) {
		gr := divideByChunkSize(keys, 3)
		if len(gr) != 4 {
			t.Fatal("incorrect number of groups")
		}
		if g, expected := convertToStrings(gr[0]), []string{"0", "1", "2"}; !pidsEquals(g, expected) {
			t.Fatalf("expected %v, got %v", expected, g)
		}
		if g, expected := convertToStrings(gr[1]), []string{"3", "4", "5"}; !pidsEquals(g, expected) {
			t.Fatalf("expected %v, got %v", expected, g)
		}
		if g, expected := convertToStrings(gr[2]), []string{"6", "7", "8"}; !pidsEquals(g, expected) {
			t.Fatalf("expected %v, got %v", expected, g)
		}
		if g, expected := convertToStrings(gr[3]), []string{"9"}; !pidsEquals(g, expected) {
			t.Fatalf("expected %v, got %v", expected, g)
		}
	})
	t.Run("OneEach", func(t *testing.T) {
		gr := divideByChunkSize(keys, 1)
		if len(gr) != 10 {
			t.Fatal("incorrect number of groups")
		}
		for i := range 10 {
			if g, expected := convertToStrings(gr[i]), []string{strconv.Itoa(i)}; !pidsEquals(g, expected) {
				t.Fatalf("expected %v, got %v", expected, g)
			}
		}
	})
	t.Run("ChunkSizeLargerThanKeys", func(t *testing.T) {
		gr := divideByChunkSize(keys, 11)
		if len(gr) != 1 {
			t.Fatal("incorrect number of groups")
		}
		if g, expected := convertToStrings(gr[0]), convertToStrings(keys); !pidsEquals(g, expected) {
			t.Fatalf("expected %v, got %v", expected, g)
		}
	})
}

func TestIPDiversityFilter(t *testing.T) {
	ctx := context.Background()
	h, err := libp2p.New()
	require.NoError(t, err)
	dht, err := NewFullRT(h, "", DHTOption(kaddht.BootstrapPeers(kaddht.GetDefaultBootstrapPeerAddrInfos()...)))
	require.NoError(t, err)

	dht.bucketSize = 3
	dht.ipDiversityFilterLimit = 1

	// peer id whose kadid starts with 15 0's
	target, err := peer.Decode("QmNLfyis4M4iAWth8ApJwbCfuQaaaXKWECGAHQfXKUG6C7")
	require.NoError(t, err)

	type addr struct {
		ipv6 bool
		addr string
	}
	// setDhtPeers replaces the dht's routing table with the provided addresses
	// assigned with random new peer ids. The provided order of addresses is also
	// the kademlia distance order to the key requested later.
	setDhtPeers := func(peerMaddrs ...[]addr) []peer.ID {
		newTrie := trie.New()
		peerAddrs := make(map[peer.ID][]ma.Multiaddr)
		kPeerMap := make(map[string]peer.ID)
		pids := make([]peer.ID, 0, len(peerMaddrs))
		for i, ips := range peerMaddrs {
			_, pubKey, err := crypto.GenerateEd25519Key(rand.Reader)
			require.NoError(t, err)
			pid, err := peer.IDFromPublicKey(pubKey)
			require.NoError(t, err)
			p := &peer.AddrInfo{ID: pid, Addrs: make([]ma.Multiaddr, 0, len(ips))}
			for _, ip := range ips {
				var a ma.Multiaddr
				var err error
				if ip.ipv6 {
					a, err = ma.NewMultiaddr("/ip6/" + ip.addr + "/tcp/4001")
				} else {
					a, err = ma.NewMultiaddr("/ip4/" + ip.addr + "/tcp/4001")
				}
				require.NoError(t, err)
				p.Addrs = append(p.Addrs, a)
			}
			k := [32]byte{}
			k[0] = byte(i)
			kadKey := kadkey.Key(k[:])
			_, ok := newTrie.Add(kadKey)
			require.True(t, ok)
			kPeerMap[string(kadKey)] = p.ID
			peerAddrs[p.ID] = p.Addrs
			pids = append(pids, p.ID)
		}
		dht.rt = newTrie
		dht.peerAddrsLk.Lock()
		dht.peerAddrs = peerAddrs
		dht.peerAddrsLk.Unlock()
		dht.kMapLk.Lock()
		dht.keyToPeerMap = kPeerMap
		dht.kMapLk.Unlock()
		return pids
	}

	t.Run("Different IPv4 blocks", func(t *testing.T) {
		pids := setDhtPeers([][]addr{
			{{ipv6: false, addr: "1.1.1.1"}},
			{{ipv6: false, addr: "2.2.2.2"}},
			{{ipv6: false, addr: "3.3.3.3"}},
			{{ipv6: false, addr: "4.4.4.4"}},
		}...)
		cp, err := dht.GetClosestPeers(ctx, string(target))
		require.NoError(t, err)
		require.Len(t, cp, dht.bucketSize)
		require.Equal(t, pids[:dht.bucketSize], cp)
	})

	t.Run("Duplicate address from IPv4 block", func(t *testing.T) {
		pids := setDhtPeers([][]addr{
			{{ipv6: false, addr: "1.1.1.1"}},
			{{ipv6: false, addr: "1.1.2.2"}},
			{{ipv6: false, addr: "3.3.3.3"}},
			{{ipv6: false, addr: "4.4.4.4"}},
		}...)
		cp, err := dht.GetClosestPeers(ctx, string(target))
		require.NoError(t, err)
		require.Len(t, cp, dht.bucketSize)
		require.Contains(t, cp, pids[0])
		require.Contains(t, cp, pids[2])
		require.Contains(t, cp, pids[3])
	})

	t.Run("Duplicate address from 2 IPv4 blocks", func(t *testing.T) {
		pids := setDhtPeers([][]addr{
			{{ipv6: false, addr: "1.1.1.1"}},
			{{ipv6: false, addr: "1.1.2.2"}},
			{{ipv6: false, addr: "3.3.3.3"}},
			{{ipv6: false, addr: "3.3.4.4"}},
			{{ipv6: false, addr: "5.5.5.5"}},
		}...)
		cp, err := dht.GetClosestPeers(ctx, string(target))
		require.NoError(t, err)
		require.Len(t, cp, dht.bucketSize)
		require.Contains(t, cp, pids[0])
		require.Contains(t, cp, pids[2])
		require.Contains(t, cp, pids[4])
	})

	t.Run("Different IPv6 blocks", func(t *testing.T) {
		pids := setDhtPeers([][]addr{
			{{ipv6: true, addr: "2001:4860:4860::1"}},
			{{ipv6: true, addr: "2606:4700:4700::2"}},
			{{ipv6: true, addr: "2620:fe::3"}},
			{{ipv6: true, addr: "2a02:6b8::4"}},
		}...)
		cp, err := dht.GetClosestPeers(ctx, string(target))
		require.NoError(t, err)
		require.Len(t, cp, dht.bucketSize)
		require.Equal(t, pids[:dht.bucketSize], cp)
	})

	t.Run("Duplicate address from IPv6 block", func(t *testing.T) {
		pids := setDhtPeers([][]addr{
			{{ipv6: true, addr: "2001:4860:4860::1"}},
			{{ipv6: true, addr: "2001:4860:4860::2"}},
			{{ipv6: true, addr: "2620:fe::3"}},
			{{ipv6: true, addr: "2a02:6b8::4"}},
		}...)
		cp, err := dht.GetClosestPeers(ctx, string(target))
		require.NoError(t, err)
		require.Len(t, cp, dht.bucketSize)
		require.Contains(t, cp, pids[0])
		require.Contains(t, cp, pids[2])
		require.Contains(t, cp, pids[3])
	})

	t.Run("Duplicate address from 2 IPv6 blocks", func(t *testing.T) {
		pids := setDhtPeers([][]addr{
			{{ipv6: true, addr: "2001:4860:4860::1"}},
			{{ipv6: true, addr: "2001:4860:4860::2"}},
			{{ipv6: true, addr: "2606:4700:4700::3"}},
			{{ipv6: true, addr: "2606:4700:4700::4"}},
			{{ipv6: true, addr: "2620:fe::5"}},
		}...)
		cp, err := dht.GetClosestPeers(ctx, string(target))
		require.NoError(t, err)
		require.Len(t, cp, dht.bucketSize)
		require.Contains(t, cp, pids[0])
		require.Contains(t, cp, pids[2])
		require.Contains(t, cp, pids[4])
	})

	t.Run("IPv4+IPv6 acceptable representation", func(t *testing.T) {
		pids := setDhtPeers([][]addr{
			{{ipv6: false, addr: "1.1.1.1"}, {ipv6: true, addr: "2001:4860:4860::1"}},
			{{ipv6: false, addr: "2.2.2.2"}, {ipv6: true, addr: "2606:4700:4700::2"}},
			{{ipv6: false, addr: "3.3.3.3"}, {ipv6: true, addr: "2620:fe::3"}},
			{{ipv6: false, addr: "4.4.4.4"}, {ipv6: true, addr: "2a02:6b8::4"}},
			{{ipv6: false, addr: "5.5.5.5"}, {ipv6: true, addr: "2620:fe::5"}},
		}...)
		cp, err := dht.GetClosestPeers(ctx, string(target))
		require.NoError(t, err)
		require.Len(t, cp, dht.bucketSize)
		require.Equal(t, pids[:dht.bucketSize], cp)
	})

	t.Run("IPv4+IPv6 overrepresentation", func(t *testing.T) {
		pids := setDhtPeers([][]addr{
			{{ipv6: false, addr: "1.1.1.1"}, {ipv6: true, addr: "2001:4860:4860::1"}},
			{{ipv6: false, addr: "1.1.2.2"}, {ipv6: true, addr: "2606:4700:4700::2"}},
			{{ipv6: false, addr: "3.3.3.3"}, {ipv6: true, addr: "2606:4700:4700::3"}},
			{{ipv6: false, addr: "4.4.4.4"}, {ipv6: true, addr: "2001:4860:4860::4"}},
			{{ipv6: false, addr: "5.5.5.5"}, {ipv6: true, addr: "2620:fe::5"}},
		}...)
		cp, err := dht.GetClosestPeers(ctx, string(target))
		require.NoError(t, err)
		require.Len(t, cp, dht.bucketSize)
		require.Contains(t, cp, pids[0])
		require.Contains(t, cp, pids[2])
		require.Contains(t, cp, pids[4])
	})

	dht.ipDiversityFilterLimit = 0
	t.Run("Disabled IP Diversity Filter", func(t *testing.T) {
		pids := setDhtPeers([][]addr{
			{{ipv6: false, addr: "1.1.1.1"}, {ipv6: true, addr: "2606:4700:4700::1"}},
			{{ipv6: false, addr: "1.1.2.2"}, {ipv6: true, addr: "2606:4700:4700::2"}},
			{{ipv6: false, addr: "1.1.3.3"}, {ipv6: true, addr: "2606:4700:4700::3"}},
			{{ipv6: false, addr: "1.1.4.4"}, {ipv6: true, addr: "2606:4700:4700::4"}},
			{{ipv6: false, addr: "1.1.5.5"}, {ipv6: true, addr: "2606:4700:4700::5"}},
		}...)
		cp, err := dht.GetClosestPeers(ctx, string(target))
		require.NoError(t, err)
		require.Len(t, cp, dht.bucketSize)
		require.Equal(t, pids[:dht.bucketSize], cp)
	})
}

// blockingCrawler parks inside Run until the context is cancelled. runCrawler
// only rebuilds the routing table after Run returns, so a test that hand-installs
// a routing table cannot have it overwritten by the crawl that NewFullRT starts.
type blockingCrawler struct{}

var _ crawler.Crawler = blockingCrawler{}

func (blockingCrawler) Run(ctx context.Context, _ []*peer.AddrInfo, _ crawler.HandleQueryResult, _ crawler.HandleQueryFail) {
	<-ctx.Done()
}

// newTestFullRT builds a FullRT that neither crawls nor dials. It uses the empty
// protocol prefix, which skips Config.Validate, so callers may disable
// subsystems. BootstrapPeers is passed explicitly because NewFullRT calls the
// config's BootstrapPeers func unconditionally and nothing else sets it.
func newTestFullRT(t *testing.T, opts ...Option) *FullRT {
	t.Helper()

	h, err := libp2p.New(libp2p.NoListenAddrs)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, h.Close()) })

	base := []Option{
		WithCrawler(blockingCrawler{}),
		DHTOption(kaddht.BootstrapPeers()),
	}
	frt, err := NewFullRT(h, "", append(base, opts...)...)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, frt.Close()) })

	// The hand-rolled config in NewFullRT never sets BucketSize, so GetClosestPeers
	// would otherwise return no peers at all.
	frt.bucketSize = 1
	return frt
}

// setTriePeers replaces the routing table with exactly the given peers, none of
// which carry addresses. GetClosestPeers then returns them without consulting
// the IP diversity filter and without dialing.
func setTriePeers(t *testing.T, frt *FullRT, pids ...peer.ID) {
	t.Helper()

	newTrie := trie.New()
	kPeerMap := make(map[string]peer.ID, len(pids))
	for i, pid := range pids {
		k := [32]byte{}
		k[0] = byte(i)
		kadKey := kadkey.Key(k[:])
		_, ok := newTrie.Add(kadKey)
		require.True(t, ok)
		kPeerMap[string(kadKey)] = pid
	}

	frt.rtLk.Lock()
	frt.rt = newTrie
	frt.rtLk.Unlock()

	frt.kMapLk.Lock()
	frt.keyToPeerMap = kPeerMap
	frt.kMapLk.Unlock()

	frt.peerAddrsLk.Lock()
	frt.peerAddrs = make(map[peer.ID][]ma.Multiaddr, len(pids))
	frt.peerAddrsLk.Unlock()
}

// blankValidator accepts everything, so a test can store records under any
// namespace without building a real validator.
type blankValidator struct{}

func (blankValidator) Validate(_ string, _ []byte) error        { return nil }
func (blankValidator) Select(_ string, _ [][]byte) (int, error) { return 0, nil }

// testMessageSender is a local copy of the fake sender in package dht, which is
// package-private there.
type testMessageSender struct {
	sendRequest func(ctx context.Context, p peer.ID, pmes *dht_pb.Message) (*dht_pb.Message, error)
	sendMessage func(ctx context.Context, p peer.ID, pmes *dht_pb.Message) error
}

var _ dht_pb.MessageSender = (*testMessageSender)(nil)

func (t testMessageSender) SendRequest(ctx context.Context, p peer.ID, pmes *dht_pb.Message) (*dht_pb.Message, error) {
	return t.sendRequest(ctx, p, pmes)
}

func (t testMessageSender) SendMessage(ctx context.Context, p peer.ID, pmes *dht_pb.Message) error {
	return t.sendMessage(ctx, p, pmes)
}

// TestFindProvidersAsyncShufflesRemoteProviders verifies the receive-side
// reorder: providers returned by a queried peer are shuffled before the
// count-capped selection loop, so the subset we keep and surface is spread
// across the returned providers rather than always the first ones listed. A
// deterministic reversing shuffle makes the assertion exact and also proves the
// shuffle runs BEFORE the count cap: with a cap of three, we must keep the
// reversed prefix (the tail of the returned ordering), not its first three
// entries.
//
// The routing table holds exactly one peer so that execOnMany, which queries
// every peer concurrently, runs the shuffle on a single goroutine.
func TestFindProvidersAsyncShufflesRemoteProviders(t *testing.T) {
	frt := newTestFullRT(t)

	remote := make([]peer.AddrInfo, 16)
	for i := range remote {
		remote[i] = peer.AddrInfo{ID: peer.ID(fmt.Sprintf("provider-%02d", i))}
	}

	_, pubKey, err := crypto.GenerateEd25519Key(rand.Reader)
	require.NoError(t, err)
	responder, err := peer.IDFromPublicKey(pubKey)
	require.NoError(t, err)
	setTriePeers(t, frt, responder)

	// The single queried peer always answers GET_PROVIDERS with the same fixed
	// order, so the shuffle runs exactly once and deterministically.
	frt.protoMessenger, err = dht_pb.NewProtocolMessenger(&testMessageSender{
		sendRequest: func(_ context.Context, _ peer.ID, req *dht_pb.Message) (*dht_pb.Message, error) {
			assert.Equal(t, dht_pb.Message_GET_PROVIDERS, req.Type)
			resp := dht_pb.NewMessage(req.Type, req.Key, 0)
			resp.ProviderPeers = dht_pb.RawPeerInfosToPBPeers(remote)
			return resp, nil
		},
	})
	require.NoError(t, err)

	// A reversing shuffle stands in for the injected rand source. It must not be
	// a globally seeded *rand.Rand: execOnMany may call this concurrently.
	frt.shuffle = func(k int, swap func(i, j int)) {
		for i := range k / 2 {
			swap(i, k-1-i)
		}
	}

	mhash, err := multihash.Sum([]byte("shuffle-me"), multihash.SHA2_256, -1)
	require.NoError(t, err)
	key := cid.NewCidV1(cid.Raw, mhash)

	var got []peer.ID
	for ai := range frt.FindProvidersAsync(t.Context(), key, 3) {
		got = append(got, ai.ID)
	}

	want := []peer.ID{peer.ID("provider-15"), peer.ID("provider-14"), peer.ID("provider-13")}
	require.Equal(t, want, got)
}

func testCid(t *testing.T) cid.Cid {
	t.Helper()
	mhash, err := multihash.Sum([]byte("store-presence"), multihash.SHA2_256, -1)
	require.NoError(t, err)
	return cid.NewCidV1(cid.Raw, mhash)
}

// TestCloseWithSubsystemsDisabled pins the nil-store contract at its sharpest
// edge: a FullRT that constructed neither store must still close cleanly.
func TestCloseWithSubsystemsDisabled(t *testing.T) {
	frt := newTestFullRT(t, DHTOption(kaddht.DisableProviders(), kaddht.DisableValues()))

	require.Nil(t, frt.ProviderManager)
	require.Nil(t, frt.valueStore)
	// Close runs via t.Cleanup, which fails the test if it errors or panics.
}

// TestGatesOnStorePresence verifies that an absent store makes its RPCs report
// unsupported, rather than the removed enableValues/enableProviders bools doing so.
func TestGatesOnStorePresence(t *testing.T) {
	key := testCid(t)

	t.Run("values disabled", func(t *testing.T) {
		frt := newTestFullRT(t, DHTOption(kaddht.DisableValues()))
		require.Nil(t, frt.valueStore)
		require.NotNil(t, frt.ProviderManager)

		require.ErrorIs(t, frt.PutValue(t.Context(), "/pk/k", []byte("v")), routing.ErrNotSupported)
		_, err := frt.GetValue(t.Context(), "/pk/k")
		require.ErrorIs(t, err, routing.ErrNotSupported)
		_, err = frt.SearchValue(t.Context(), "/pk/k")
		require.ErrorIs(t, err, routing.ErrNotSupported)
		require.ErrorIs(t, frt.PutMany(t.Context(), []string{"/pk/k"}, [][]byte{[]byte("v")}), routing.ErrNotSupported)
	})

	t.Run("providers disabled", func(t *testing.T) {
		frt := newTestFullRT(t, DHTOption(kaddht.DisableProviders()))
		require.Nil(t, frt.ProviderManager)
		require.NotNil(t, frt.valueStore)

		require.ErrorIs(t, frt.Provide(t.Context(), key, true), routing.ErrNotSupported)
		require.ErrorIs(t, frt.ProvideMany(t.Context(), []multihash.Multihash{key.Hash()}), routing.ErrNotSupported)
		_, err := frt.FindProviders(t.Context(), key)
		require.ErrorIs(t, err, routing.ErrNotSupported)

		ch := frt.FindProvidersAsync(t.Context(), key, 1)
		_, ok := <-ch
		require.False(t, ok, "FindProvidersAsync must return a closed, empty channel")
	})
}

// TestDefaultPrefixRequiresValueAndProviderStores documents why nil stores stay
// unreachable on the Amino DHT: Validate rejects disabling either subsystem, so
// only a fork on another prefix ever sees an absent store.
func TestDefaultPrefixRequiresValueAndProviderStores(t *testing.T) {
	for _, tc := range []struct {
		name string
		opt  kaddht.Option
		want string
	}{
		{"providers", kaddht.DisableProviders(), "must have providers enabled"},
		{"values", kaddht.DisableValues(), "must have values enabled"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, err := libp2p.New(libp2p.NoListenAddrs)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, h.Close()) })

			_, err = NewFullRT(h, amino.ProtocolPrefix, WithCrawler(blockingCrawler{}),
				DHTOption(kaddht.BootstrapPeers(), kaddht.BucketSize(amino.DefaultBucketSize), tc.opt))
			require.ErrorContains(t, err, tc.want)
		})
	}
}

func recordKeys(t *testing.T, ctx context.Context, dstore ds.Datastore) []string {
	t.Helper()
	res, err := dstore.Query(ctx, dsq.Query{KeysOnly: true})
	require.NoError(t, err)
	defer res.Close()

	var keys []string
	for e := range res.Next() {
		require.NoError(t, e.Error)
		keys = append(keys, e.Key)
	}
	return keys
}

// TestValueDatastoreOverride checks that fullrt honors ValueDatastore, sending
// value records to the override while the main datastore stays empty.
func TestValueDatastoreOverride(t *testing.T) {
	ctx := t.Context()
	main := dssync.MutexWrap(ds.NewMapDatastore())
	values := dssync.MutexWrap(ds.NewMapDatastore())

	frt := newTestFullRT(t, DHTOption(
		kaddht.Datastore(main),
		kaddht.ValueDatastore(values),
		kaddht.Validator(blankValidator{}),
	))

	key := "/v/somekey"
	require.NoError(t, frt.valueStore.Put(ctx, key, record.MakePutRecord(key, []byte("hello"))))

	got, err := frt.valueStore.Get(ctx, key)
	require.NoError(t, err)
	require.Equal(t, []byte("hello"), got.GetValue())

	require.NotEmpty(t, recordKeys(t, ctx, values))
	require.Empty(t, recordKeys(t, ctx, main))
}

// TestProviderDatastoreOverride checks that fullrt honors ProviderDatastore,
// sending provider records to the override while the main datastore stays empty.
func TestProviderDatastoreOverride(t *testing.T) {
	ctx := t.Context()
	main := dssync.MutexWrap(ds.NewMapDatastore())
	providers := dssync.MutexWrap(ds.NewMapDatastore())

	frt := newTestFullRT(t, DHTOption(
		kaddht.Datastore(main),
		kaddht.ProviderDatastore(providers),
	))

	key := []byte("provider-override-key")
	frt.ProviderManager.AddProvider(ctx, key, peer.AddrInfo{ID: frt.self})

	provs, err := frt.ProviderManager.GetProviders(ctx, key)
	require.NoError(t, err)
	require.Len(t, provs, 1)

	require.NotEmpty(t, recordKeys(t, ctx, providers))
	require.Empty(t, recordKeys(t, ctx, main))
}

// TestProviderManagerOptsReachTheManager checks that provider manager options
// routed through DHTOption are applied, and that fullrt's own
// WithProviderManagerOptions still wins on conflict because it is applied last.
//
// ProvideValidity is asserted through behavior: a negative validity expires a
// provider record the moment it is written, so GetProviders serves nothing. A
// tiny positive validity would not do. The record round-trips through the
// datastore, which drops the monotonic reading, so expiry compares the wall
// clocks of the write and the read — and Windows advances its wall clock only
// every ~15ms, making both readings identical and the record still valid.
func TestProviderManagerOptsReachTheManager(t *testing.T) {
	addAndGet := func(t *testing.T, frt *FullRT) []peer.AddrInfo {
		t.Helper()
		key := []byte("provider-manager-opts-key")
		frt.ProviderManager.AddProvider(t.Context(), key, peer.AddrInfo{ID: frt.self})
		provs, err := frt.ProviderManager.GetProviders(t.Context(), key)
		require.NoError(t, err)
		return provs
	}

	t.Run("applied from DHTOption", func(t *testing.T) {
		frt := newTestFullRT(t, DHTOption(kaddht.ProviderManagerOpts(records.ProvideValidity(-time.Second))))
		require.Empty(t, addAndGet(t, frt), "records must expire immediately, so the option reached the manager")
	})

	t.Run("fullrt-native option wins", func(t *testing.T) {
		frt := newTestFullRT(t,
			DHTOption(kaddht.ProviderManagerOpts(records.ProvideValidity(-time.Second))),
			WithProviderManagerOptions(records.ProvideValidity(time.Hour)),
		)
		require.Len(t, addAndGet(t, frt), 1, "the fullrt-native validity must override the DHTOption one")
	})
}
