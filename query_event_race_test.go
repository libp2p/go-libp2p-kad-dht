package dht

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/core/routing"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"

	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
)

// TestFindProvidersAsyncQueryEventResponsesRace reproduces the race
// reported in ipfs/kubo#11287 and ipfs/kubo#11116.
//
// The same []*peer.AddrInfo is both:
//  1. published as routing.QueryEvent.Responses by findProvidersAsyncRoutine
//     in routing.go (read by any RegisterForQueryEvents consumer, e.g.
//     kubo's findprovs HTTP handler which json-marshals events); and
//  2. returned to the query worker which, in queryPeer (query.go), does
//     next.Addrs = append(next.Addrs, dht.peerstore.PeerInfo(next.ID).Addrs...)
//
// Under -race the test reports a read/write race on AddrInfo.Addrs.
// Without -race the torn slice header can crash inside go-multiaddr.
func TestFindProvidersAsyncQueryEventResponsesRace(t *testing.T) {
	ctx := t.Context()
	dhts := setupDHTS(t, ctx, 2)
	connect(t, ctx, dhts[0], dhts[1])

	// Build fake "closer" peers. Each arrives on the wire with a small
	// Addrs slice; dhts[0]'s peerstore carries additional addresses for
	// the same peer IDs so queryPeer's append triggers a realloc and
	// rewrites each AddrInfo.Addrs header (the data being raced on).
	const numCloser = 16
	fakeCloser := make([]peer.AddrInfo, numCloser)
	extra := []ma.Multiaddr{
		ma.StringCast("/ip4/10.0.0.1/tcp/4001"),
		ma.StringCast("/ip4/10.0.0.2/udp/4001/quic-v1"),
		ma.StringCast("/ip4/10.0.0.3/tcp/4001"),
		ma.StringCast("/ip4/10.0.0.4/udp/4001/quic-v1"),
	}
	for i := range fakeCloser {
		_, pub, err := crypto.GenerateKeyPair(crypto.Ed25519, 256)
		require.NoError(t, err)
		pid, err := peer.IDFromPublicKey(pub)
		require.NoError(t, err)
		fakeCloser[i] = peer.AddrInfo{
			ID: pid,
			Addrs: []ma.Multiaddr{
				ma.StringCast("/ip4/1.2.3.4/tcp/4001"),
				ma.StringCast("/ip4/5.6.7.8/udp/4001/quic-v1"),
			},
		}
		dhts[0].peerstore.AddAddrs(pid, extra, peerstore.TempAddrTTL)
	}

	// Make every GET_PROVIDERS return fakeCloser as CloserPeers. This
	// removes dependence on dhts[1]'s real routing table and ensures the
	// queryFn closure always has a non-trivial slice to publish.
	dhts[0].protoMessenger, _ = pb.NewProtocolMessenger(&testMessageSender{
		sendRequest: func(_ context.Context, _ peer.ID, req *pb.Message) (*pb.Message, error) {
			require.Equal(t, pb.Message_GET_PROVIDERS, req.Type)
			resp := pb.NewMessage(req.Type, req.Key, 0)
			resp.CloserPeers = pb.RawPeerInfosToPBPeers(fakeCloser)
			return resp, nil
		},
	})

	mh, err := multihash.Sum([]byte("race-me"), multihash.SHA2_256, -1)
	require.NoError(t, err)
	key := cid.NewCidV1(cid.Raw, mh)

	// Subscribe to query events on the context we pass into
	// FindProvidersAsync, then marshal every PeerResponse to mimic the
	// kubo findprovs handler.
	subCtx, events := routing.RegisterForQueryEvents(ctx)
	consumerDone := make(chan struct{})
	go func() {
		defer close(consumerDone)
		for ev := range events {
			if ev.Type != routing.PeerResponse {
				continue
			}
			for _, pi := range ev.Responses {
				_, _ = json.Marshal(pi)
			}
		}
	}()

	// Hammer FindProvidersAsync for a few seconds; each call emits at
	// least one PeerResponse whose Responses slice queryPeer mutates.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		callCtx, cancel := context.WithTimeout(subCtx, 500*time.Millisecond)
		for range dhts[0].FindProvidersAsync(callCtx, key, 0) {
		}
		cancel()
	}

	// Cancelling the parent context closes the event channel; wait for
	// the consumer so t.Cleanup sees a quiet goroutine set.
	// (The routing package closes the channel when subCtx is done.)
	t.Cleanup(func() { <-consumerDone })
}
