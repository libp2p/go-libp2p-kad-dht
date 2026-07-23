package dht

import (
	"context"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
	crypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/network"
	peer "github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestBadMessage(t *testing.T) {
	ctx := t.Context()

	dht := setupDHT(ctx, t, false)

	for _, typ := range []pb.Message_MessageType{
		pb.Message_PUT_VALUE, pb.Message_GET_VALUE, pb.Message_ADD_PROVIDER,
		pb.Message_GET_PROVIDERS, pb.Message_FIND_NODE,
	} {
		msg := &pb.Message{
			Type: typ,
			// explicitly avoid the key.
		}
		_, err := dht.handlerForMsgType(typ)(ctx, dht.Host().ID(), msg)
		if err == nil {
			t.Fatalf("expected processing message to fail for type %s", pb.Message_FIND_NODE)
		}
	}
}

// TestHandlerForMsgTypeGatedByStorePresence checks that value/provider RPC
// support follows the presence of the corresponding store: a disabled subsystem
// has no store, so its message types resolve to no handler (unsupported RPC).
func TestHandlerForMsgTypeGatedByStorePresence(t *testing.T) {
	ctx := t.Context()

	valueTypes := []pb.Message_MessageType{pb.Message_GET_VALUE, pb.Message_PUT_VALUE}
	providerTypes := []pb.Message_MessageType{pb.Message_ADD_PROVIDER, pb.Message_GET_PROVIDERS}

	assertSupport := func(t *testing.T, d *IpfsDHT, types []pb.Message_MessageType, want bool) {
		t.Helper()
		for _, mt := range types {
			if supported := d.handlerForMsgType(mt) != nil; supported != want {
				t.Fatalf("handler for %s: supported=%v, want %v", mt, supported, want)
			}
		}
	}

	t.Run("all enabled", func(t *testing.T) {
		d := setupDHT(ctx, t, false)
		if d.valueStore == nil || d.providerStore == nil {
			t.Fatal("both stores should be present when enabled")
		}
		assertSupport(t, d, valueTypes, true)
		assertSupport(t, d, providerTypes, true)
	})

	t.Run("values disabled", func(t *testing.T) {
		d := setupDHT(ctx, t, false, DisableValues())
		if d.valueStore != nil {
			t.Fatal("value store should be absent when disabled")
		}
		assertSupport(t, d, valueTypes, false)
		assertSupport(t, d, providerTypes, true)
	})

	t.Run("providers disabled", func(t *testing.T) {
		d := setupDHT(ctx, t, false, DisableProviders())
		if d.providerStore != nil {
			t.Fatal("provider store should be absent when disabled")
		}
		assertSupport(t, d, providerTypes, false)
		assertSupport(t, d, valueTypes, true)
	})

	t.Run("both disabled", func(t *testing.T) {
		// Both stores absent: only FIND_NODE/PING remain; Close (via setupDHT's
		// cleanup) must tolerate the both-nil case.
		d := setupDHT(ctx, t, false, DisableValues(), DisableProviders())
		if d.valueStore != nil || d.providerStore != nil {
			t.Fatal("both stores should be absent when disabled")
		}
		assertSupport(t, d, valueTypes, false)
		assertSupport(t, d, providerTypes, false)
	})
}

func TestStripPeerRecords(t *testing.T) {
	pmes := &pb.Message{
		Type:          pb.Message_PING,
		CloserPeers:   []*pb.Message_Peer{{Id: []byte("closer")}},
		ProviderPeers: []*pb.Message_Peer{{Id: []byte("provider")}},
	}

	stripPeerRecords(pmes)

	require.Nilf(t, pmes.CloserPeers, "closer peers must be dropped")
	require.Nilf(t, pmes.ProviderPeers, "provider peers must be dropped")
	require.Equalf(t, pb.Message_PING, pmes.Type, "other fields must be left intact")
}

// TestHandlePingDropsStuffedPeerRecords checks that handlePing, which echoes the
// request back, does not re-emit peer records a peer stuffed into the request,
// keeping every record this node emits bounded.
func TestHandlePingDropsStuffedPeerRecords(t *testing.T) {
	ctx := t.Context()
	d := setupDHT(ctx, t, false)

	stuffed := []*pb.Message_Peer{{
		Id:    []byte("stuffed-peer-id"),
		Addrs: [][]byte{[]byte("/ip4/1.2.3.4/tcp/1")},
	}}
	req := &pb.Message{
		Type:          pb.Message_PING,
		CloserPeers:   stuffed,
		ProviderPeers: stuffed,
	}

	resp, err := d.handlePing(ctx, d.Host().ID(), req)
	require.NoError(t, err)
	require.Emptyf(t, resp.CloserPeers, "ping response must not echo closer peers")
	require.Emptyf(t, resp.ProviderPeers, "ping response must not echo provider peers")
}

func BenchmarkHandleFindPeer(b *testing.B) {
	ctx := b.Context()
	h, err := libp2p.New()
	if err != nil {
		b.Fatal(err)
	}
	defer h.Close()

	d, err := New(h)
	if err != nil {
		b.Fatal(err)
	}

	rng := rand.NewChaCha8([32]byte{150})
	var peers []peer.ID
	for i := range 1000 {
		_, pubk, _ := crypto.GenerateEd25519Key(rng)
		id, err := peer.IDFromPublicKey(pubk)
		if err != nil {
			panic(err)
		}

		d.peerFound(id)

		peers = append(peers, id)
		a, err := ma.NewMultiaddr(fmt.Sprintf("/ip4/127.0.0.1/tcp/%d", 2000+i))
		if err != nil {
			panic(err)
		}

		d.host.Peerstore().AddAddr(id, a, time.Minute*50)
	}

	var reqs []*pb.Message
	for i := 0; i < b.N; i++ {
		reqs = append(reqs, &pb.Message{
			Key: []byte("asdasdasd"),
		})
	}
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err = d.handleFindPeer(ctx, peers[0], reqs[i])
		if err != nil {
			b.Error(err)
		}
	}
}

// serveGetProviders runs handleGetProviders against d with a mock store that
// returns the given providers verbatim (bypassing the real shuffle so ordering
// assertions stay deterministic).
func serveGetProviders(ctx context.Context, t *testing.T, d *IpfsDHT, from peer.ID, providers []peer.AddrInfo) *pb.Message {
	t.Helper()
	d.providerStore = &testProviderManager{
		getProviders: func(context.Context, []byte) ([]peer.AddrInfo, error) {
			return providers, nil
		},
		close: func() error { return nil },
	}
	resp, err := d.handleGetProviders(ctx, from, &pb.Message{
		Type: pb.Message_GET_PROVIDERS,
		Key:  []byte("some-key"),
	})
	require.NoError(t, err)
	return resp
}

// hugeAddrs returns n distinct multiaddrs of ~2 KiB each, used to inflate a
// record past what a normal provider would carry. The addresses must be unique:
// a peerstore deduplicates identical addresses, which would otherwise collapse a
// closer peer's address list to a single entry.
func hugeAddrs(n int) []ma.Multiaddr {
	addrs := make([]ma.Multiaddr, n)
	for i := range addrs {
		addrs[i] = ma.StringCast(fmt.Sprintf("/dns4/%s%d/tcp/1", strings.Repeat("a", 2000), i))
	}
	return addrs
}

// fatAddrs returns a ~6 KiB address list: big enough that a few hundred records
// carrying it overflow the message cap, yet small enough to stay well inside the
// 8 KiB per-record size bound the pb layer will apply once #1281 lands, so these
// records reach the handler untrimmed either way. Message pressure therefore
// comes from the NUMBER of records rather than from any single oversized one,
// which is the only regime in which the response bound stays reachable once a
// record can no longer exceed the cap on its own.
func fatAddrs() []ma.Multiaddr { return hugeAddrs(3) }

// fatProviderCount is comfortably more than the ~690 fatAddrs records that fill
// the 4 MiB message cap, so a set this size is always truncated.
const fatProviderCount = 900

// TestHandleGetProvidersTruncatesToMessageSizeMax verifies the serve-side
// response bound: when the providers for a key carry enough address bytes to
// blow past the transport's soft message maximum, handleGetProviders drops a
// tail of provider records so the serialized response stays within the cap. A
// small provider offered first (real reads are shuffled; this test fixes the
// order to keep the assertion deterministic) must survive the truncation. The
// truncation must also be tight: adding back the first dropped record would
// exceed the cap.
func TestHandleGetProvidersTruncatesToMessageSizeMax(t *testing.T) {
	ctx := t.Context()
	d := setupDHT(ctx, t, false)

	// Keep the synthetic oversized addresses intact through the handler.
	d.addrFilter = func(addrs []ma.Multiaddr) []ma.Multiaddr { return addrs }

	small := peer.ID("small-provider")
	smallAddr := ma.StringCast("/ip4/1.2.3.4/tcp/4001")

	// Each bulk record carries ~6 KiB of addresses, so several hundred of them
	// exceed the 4 MiB cap.
	bigAddrs := fatAddrs()
	providers := []peer.AddrInfo{{ID: small, Addrs: []ma.Multiaddr{smallAddr}}}
	for i := range fatProviderCount {
		providers = append(providers, peer.AddrInfo{
			ID:    peer.ID(fmt.Sprintf("bulk-%03d", i)),
			Addrs: bigAddrs,
		})
	}

	resp := serveGetProviders(ctx, t, d, d.self, providers)

	require.LessOrEqualf(t, proto.Size(resp), network.MessageSizeMax,
		"response (%d bytes) must not exceed the message size max", proto.Size(resp))
	require.Greaterf(t, len(resp.ProviderPeers), 0, "at least one provider must be served")
	require.Lessf(t, len(resp.ProviderPeers), len(providers), "oversized tail must be truncated")
	require.Equalf(t, []byte(small), resp.ProviderPeers[0].Id,
		"the provider offered first must survive truncation")

	// Truncation is tight: re-adding the first dropped record overflows the cap,
	// so we kept the maximal fitting prefix (guards the running-size accounting).
	kept := len(resp.ProviderPeers)
	next := pb.PeerInfosToPBPeers(d.host.Network(), []peer.AddrInfo{{
		ID:    providers[kept].ID,
		Addrs: providers[kept].Addrs,
	}})
	resp.ProviderPeers = append(resp.ProviderPeers, next...)
	require.Greaterf(t, proto.Size(resp), network.MessageSizeMax,
		"the first dropped provider must not have fit; truncation stopped too early")
}

// TestHandleGetProvidersServesAllUnderCap checks the non-truncating paths: a
// small provider set is returned whole, and an empty set yields no provider
// records without panicking.
func TestHandleGetProvidersServesAllUnderCap(t *testing.T) {
	ctx := t.Context()
	d := setupDHT(ctx, t, false)
	d.addrFilter = func(addrs []ma.Multiaddr) []ma.Multiaddr { return addrs }

	addr := ma.StringCast("/ip4/1.2.3.4/tcp/4001")
	providers := make([]peer.AddrInfo, 5)
	for i := range providers {
		providers[i] = peer.AddrInfo{ID: peer.ID(fmt.Sprintf("prov-%d", i)), Addrs: []ma.Multiaddr{addr}}
	}

	resp := serveGetProviders(ctx, t, d, d.self, providers)
	require.LessOrEqual(t, proto.Size(resp), network.MessageSizeMax)
	require.Lenf(t, resp.ProviderPeers, len(providers), "an under-cap set must be served whole, none dropped")

	empty := serveGetProviders(ctx, t, d, d.self, nil)
	require.Empty(t, empty.ProviderPeers, "an empty provider set yields no provider records")
}

// TestAppendFittingProviderPeersDropsOversizedRecord pins the degenerate edge of
// the response bound: a record whose framed size alone exceeds the cap is
// dropped rather than served over-size, and the resulting empty truncation must
// not panic.
//
// The guard is driven directly, with a hand-built record, rather than through
// handleGetProviders: once #1281 lands the pb constructors bound a record's
// address list, so a record this large can no longer be produced from a
// peer.AddrInfo and the handler cannot reach this path from real input. The
// guard is kept as defence in depth — it is what holds the response within the
// cap if a record ever grows past it again.
func TestAppendFittingProviderPeersDropsOversizedRecord(t *testing.T) {
	resp := pb.NewMessage(pb.Message_GET_PROVIDERS, []byte("some-key"), 0)

	// ~5 MiB of addresses on a single record — bigger than the 4 MiB cap.
	whale := &pb.Message_Peer{Id: []byte("whale")}
	for _, a := range hugeAddrs(2600) {
		whale.Addrs = append(whale.Addrs, a.Bytes())
	}
	require.Greaterf(t, proto.Size(whale), network.MessageSizeMax,
		"the record must exceed the cap for this test to be meaningful")

	require.NotPanics(t, func() {
		appendFittingProviderPeers(resp, slices.Values([]*pb.Message_Peer{whale}))
	})
	require.Empty(t, resp.ProviderPeers, "a single over-cap record must not be served")
	require.LessOrEqual(t, proto.Size(resp), network.MessageSizeMax)
}

// TestAppendFittingProviderPeersServesFittingRecords is the positive companion:
// records that fit are appended in order, and the iterator is not drained past
// the cap.
func TestAppendFittingProviderPeersServesFittingRecords(t *testing.T) {
	resp := pb.NewMessage(pb.Message_GET_PROVIDERS, []byte("some-key"), 0)

	recs := make([]*pb.Message_Peer, fatProviderCount)
	addrs := fatAddrs()
	for i := range recs {
		rec := &pb.Message_Peer{Id: []byte(fmt.Sprintf("prov-%03d", i))}
		for _, a := range addrs {
			rec.Addrs = append(rec.Addrs, a.Bytes())
		}
		recs[i] = rec
	}

	var pulled int
	appendFittingProviderPeers(resp, func(yield func(*pb.Message_Peer) bool) {
		for _, rec := range recs {
			pulled++
			if !yield(rec) {
				return
			}
		}
	})

	require.LessOrEqual(t, proto.Size(resp), network.MessageSizeMax)
	require.NotEmpty(t, resp.ProviderPeers, "records that fit must be served")
	require.Lessf(t, len(resp.ProviderPeers), len(recs), "the over-cap tail must be dropped")
	require.Equalf(t, recs[0].Id, resp.ProviderPeers[0].Id, "fitting records keep their order")
	require.Equalf(t, len(resp.ProviderPeers)+1, pulled,
		"the iterator must stop at the first record that does not fit, not drain the set")
}

// TestHandleGetProvidersBudgetIncludesCloserPeers checks that CloserPeers are
// built once BEFORE the provider loop and their serialized size counts against
// the message budget. Providers alone fill the cap, so if the closer peers'
// bytes were not already counted when admitting providers, appending them would
// push the finished message past the cap.
func TestHandleGetProvidersBudgetIncludesCloserPeers(t *testing.T) {
	ctx := t.Context()
	d := setupDHT(ctx, t, false)
	d.addrFilter = func(addrs []ma.Multiaddr) []ma.Multiaddr { return addrs }

	// Routing-table peers carrying a full address list each, so CloserPeers take
	// up a non-trivial slice of the budget.
	closerAddrs := fatAddrs()
	rng := rand.NewChaCha8([32]byte{42})
	for range 20 {
		_, pub, err := crypto.GenerateEd25519Key(rng)
		require.NoError(t, err)
		id, err := peer.IDFromPublicKey(pub)
		require.NoError(t, err)
		d.host.Peerstore().AddAddrs(id, closerAddrs, time.Hour)
		_, err = d.routingTable.TryAddPeer(id, true, false)
		require.NoError(t, err)
	}

	// Far more providers than fit, so the provider loop runs right up to the cap.
	provAddrs := fatAddrs()
	providers := make([]peer.AddrInfo, fatProviderCount)
	for i := range providers {
		providers[i] = peer.AddrInfo{ID: peer.ID(fmt.Sprintf("prov-%03d", i)), Addrs: provAddrs}
	}

	resp := serveGetProviders(ctx, t, d, peer.ID("requester"), providers)

	require.NotEmpty(t, resp.CloserPeers, "closer peers must be present for this test to be meaningful")
	closerOnly := &pb.Message{CloserPeers: resp.CloserPeers}
	closerSize := proto.Size(closerOnly)

	require.LessOrEqualf(t, proto.Size(resp), network.MessageSizeMax,
		"response (%d bytes) must stay within the cap with closer peers counted", proto.Size(resp))
	require.Greaterf(t, len(resp.ProviderPeers), 0, "some providers should fit in the remaining budget")
	require.Lessf(t, len(resp.ProviderPeers), len(providers), "providers must be truncated by the budget")

	// The loop stops within one record of the cap, so the closer peers must
	// outweigh that leftover slack for the assertion below to discriminate.
	require.Greaterf(t, closerSize, 10*proto.Size(resp.ProviderPeers[0]),
		"closer peers must outweigh the loop's leftover slack, else the budget assertion is vacuous")

	// The providers admitted must leave room for the closer peers already in the
	// message: had the loop ignored them, it would have filled the cap with
	// providers alone and the closer-peer bytes would have overflowed it.
	provsOnly := &pb.Message{ProviderPeers: resp.ProviderPeers}
	require.LessOrEqualf(t, proto.Size(provsOnly), network.MessageSizeMax-closerSize,
		"the provider budget must be reduced by the %d bytes of closer peers", closerSize)
}

// providerIDs extracts the provider peer-ID bytes from a GET_PROVIDERS response.
func providerIDs(resp *pb.Message) [][]byte {
	ids := make([][]byte, len(resp.ProviderPeers))
	for i, p := range resp.ProviderPeers {
		ids[i] = p.Id
	}
	return ids
}

// TestHandleGetProvidersTruncationSurvivorDependsOnOrder is a regression test
// for response bounding when a key holds providers whose records carry large
// address lists. The response is always held within network.MessageSizeMax by
// dropping a tail of provider records, so WHICH providers survive the bound
// depends on their order in the set: a target ordered after enough large
// records to fill the budget is dropped, while the same target ordered first
// survives — in both cases within the size bound. This is the reason the
// provider store returns providers in randomized order (records: GetProviders
// shuffle) rather than the datastore query's fixed lexicographic-by-peer-ID
// order; a fixed order would leave a fixed subset permanently unreachable
// whenever a key's records exceed the message size.
func TestHandleGetProvidersTruncationSurvivorDependsOnOrder(t *testing.T) {
	ctx := t.Context()
	d := setupDHT(ctx, t, false)
	// Keep the synthetic oversized addresses intact through the handler.
	d.addrFilter = func(addrs []ma.Multiaddr) []ma.Multiaddr { return addrs }

	target := peer.AddrInfo{
		ID:    peer.ID("target-provider"),
		Addrs: []ma.Multiaddr{ma.StringCast("/ip4/1.2.3.4/tcp/4001")},
	}

	// Enough ~6 KiB records to overflow the 4 MiB cap comfortably.
	bigAddrs := fatAddrs()
	bulky := make([]peer.AddrInfo, fatProviderCount)
	for i := range bulky {
		bulky[i] = peer.AddrInfo{ID: peer.ID(fmt.Sprintf("bulk-%03d", i)), Addrs: bigAddrs}
	}

	// Target last: the large records exhaust the budget before the loop reaches
	// it, so truncation drops the target.
	buried := make([]peer.AddrInfo, 0, fatProviderCount+1)
	buried = append(buried, bulky...)
	buried = append(buried, target)
	resp := serveGetProviders(ctx, t, d, d.self, buried)
	require.LessOrEqualf(t, proto.Size(resp), network.MessageSizeMax,
		"response (%d bytes) must not exceed the message size max", proto.Size(resp))
	require.NotContainsf(t, providerIDs(resp), []byte(target.ID),
		"a target ordered after a budget-filling set of large records is truncated away")

	// Target first: it is admitted before the large records fill the budget, so
	// it survives within the same size bound.
	surfaced := make([]peer.AddrInfo, 0, fatProviderCount+1)
	surfaced = append(surfaced, target)
	surfaced = append(surfaced, bulky...)
	resp = serveGetProviders(ctx, t, d, d.self, surfaced)
	require.LessOrEqualf(t, proto.Size(resp), network.MessageSizeMax,
		"response (%d bytes) must not exceed the message size max", proto.Size(resp))
	require.Equalf(t, []byte(target.ID), resp.ProviderPeers[0].Id,
		"a target ordered first survives truncation within the size bound")
}
