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
