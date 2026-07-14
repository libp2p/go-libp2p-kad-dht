package dht

import (
	"context"
	"fmt"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"

	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
)

// TestFindProvidersAsyncShufflesRemoteProviders verifies the receive-side
// reorder: providers returned by a queried peer are shuffled before the
// count-capped selection loop, so the subset we keep and surface is spread
// across the returned providers rather than always the first ones listed. A
// deterministic reversing shuffle makes the assertion exact and also proves the
// shuffle runs BEFORE the count cap: with a cap of three, we must keep the
// reversed prefix (the tail of the returned ordering), not its first three
// entries.
func TestFindProvidersAsyncShufflesRemoteProviders(t *testing.T) {
	ctx := t.Context()
	dhts := setupDHTS(t, ctx, 2)
	connect(t, ctx, dhts[0], dhts[1])

	const n = 16
	remote := make([]peer.AddrInfo, n)
	for i := range remote {
		remote[i] = peer.AddrInfo{ID: peer.ID(fmt.Sprintf("provider-%02d", i))}
	}

	// The remote always answers GET_PROVIDERS with the same fixed order and no
	// closer peers, so the lookup queries exactly one peer and runs the shuffle
	// once (single-threaded, hence deterministic).
	dhts[0].protoMessenger, _ = pb.NewProtocolMessenger(&testMessageSender{
		sendRequest: func(_ context.Context, _ peer.ID, req *pb.Message) (*pb.Message, error) {
			require.Equal(t, pb.Message_GET_PROVIDERS, req.Type)
			resp := pb.NewMessage(req.Type, req.Key, 0)
			resp.ProviderPeers = pb.RawPeerInfosToPBPeers(remote)
			return resp, nil
		},
	})

	// Deterministic reversing shuffle stands in for the injected rand source.
	dhts[0].shuffle = func(k int, swap func(i, j int)) {
		for i := 0; i < k/2; i++ {
			swap(i, k-1-i)
		}
	}

	mhash, err := multihash.Sum([]byte("shuffle-me"), multihash.SHA2_256, -1)
	require.NoError(t, err)
	key := cid.NewCidV1(cid.Raw, mhash)

	const count = 3
	var got []peer.ID
	for ai := range dhts[0].FindProvidersAsync(ctx, key, count) {
		got = append(got, ai.ID)
	}

	want := []peer.ID{peer.ID("provider-15"), peer.ID("provider-14"), peer.ID("provider-13")}
	require.Equal(t, want, got)
}

// TestFindProvidersAsyncSurfacesProviderListedLast is an end-to-end regression
// test for receive-side order-independence: when a queried peer returns more
// providers than the caller asked for, the providers are randomized before the
// count-capped selection loop, so a provider the peer lists LAST is not
// deterministically excluded from what FindProvidersAsync surfaces.
//
// The two sub-cases pin the behaviour from both sides. With the received order
// reversed before the cap, a target listed last is surfaced. With the received
// order left untouched (identity — a mutation of the real shuffle), the cap
// takes a fixed prefix and the last-listed target is excluded: this proves the
// reorder is load-bearing, not incidental, and is why the order a peer chooses
// for its response must not decide which of its providers a caller keeps.
func TestFindProvidersAsyncSurfacesProviderListedLast(t *testing.T) {
	const target = peer.ID("target-provider")

	run := func(t *testing.T, shuffle func(n int, swap func(i, j int)), wantSurfaced bool) {
		t.Helper()
		ctx := t.Context()
		dhts := setupDHTS(t, ctx, 2)
		connect(t, ctx, dhts[0], dhts[1])

		// The remote answers with a fixed set of fillers followed by the target
		// listed last, and no closer peers, so the lookup queries exactly one peer
		// and runs the shuffle once (single-threaded, hence deterministic).
		const fillers = 16
		remote := make([]peer.AddrInfo, fillers+1)
		for i := range fillers {
			remote[i] = peer.AddrInfo{ID: peer.ID(fmt.Sprintf("filler-%02d", i))}
		}
		remote[fillers] = peer.AddrInfo{ID: target}

		dhts[0].protoMessenger, _ = pb.NewProtocolMessenger(&testMessageSender{
			sendRequest: func(_ context.Context, _ peer.ID, req *pb.Message) (*pb.Message, error) {
				require.Equal(t, pb.Message_GET_PROVIDERS, req.Type)
				resp := pb.NewMessage(req.Type, req.Key, 0)
				resp.ProviderPeers = pb.RawPeerInfosToPBPeers(remote)
				return resp, nil
			},
		})
		dhts[0].shuffle = shuffle

		mhash, err := multihash.Sum([]byte("order-independence"), multihash.SHA2_256, -1)
		require.NoError(t, err)
		key := cid.NewCidV1(cid.Raw, mhash)

		// Ask for fewer providers than the remote returns, so the count cap is the
		// thing that would exclude the last-listed target absent a reorder.
		const count = 3
		surfaced := false
		for ai := range dhts[0].FindProvidersAsync(ctx, key, count) {
			if ai.ID == target {
				surfaced = true
			}
		}
		require.Equalf(t, wantSurfaced, surfaced,
			"target surfaced=%v, want %v", surfaced, wantSurfaced)
	}

	// A reversing shuffle moves the last-listed target into the kept prefix.
	t.Run("randomized order surfaces the last-listed provider", func(t *testing.T) {
		run(t, func(n int, swap func(i, j int)) {
			for i := 0; i < n/2; i++ {
				swap(i, n-1-i)
			}
		}, true)
	})

	// Identity order leaves the count cap taking the first three fillers, so the
	// last-listed target is never surfaced.
	t.Run("received order excludes the last-listed provider", func(t *testing.T) {
		run(t, func(int, func(i, j int)) {}, false)
	})
}
