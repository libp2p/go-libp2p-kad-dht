package dht

import (
	"fmt"
	"math/rand/v2"
	"strings"
	"testing"

	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
	crypto "github.com/libp2p/go-libp2p/core/crypto"
	peer "github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/require"
)

// bigAddrBytes returns the wire bytes of n distinct ~2 KiB multiaddrs, enough to
// push a single peer record well past pb.MaxPeerRecordSize.
func bigAddrBytes(n int) [][]byte {
	addrs := make([][]byte, n)
	for i := range addrs {
		addrs[i] = ma.StringCast(fmt.Sprintf("/dns4/%s%d/tcp/1", strings.Repeat("a", 2000), i)).Bytes()
	}
	return addrs
}

// TestHandleAddProviderBoundsIngestedAddrs checks the receive-side effect of the
// per-record size bound: an ADD_PROVIDER whose provider record carries a very
// large address list has that list trimmed to pb.MaxPeerRecordSize before the
// addresses reach the peerstore, so the peerstore records at most a per-record
// cap's worth of addresses for the provider.
func TestHandleAddProviderBoundsIngestedAddrs(t *testing.T) {
	ctx := t.Context()
	d := setupDHT(ctx, t, false)
	// Keep the synthetic oversized addresses intact through the address filter.
	d.addrFilter = func(addrs []ma.Multiaddr) []ma.Multiaddr { return addrs }

	// A provider announcing itself under a valid peer ID with a ~4 MiB addr list.
	_, pub, err := crypto.GenerateEd25519Key(rand.NewChaCha8([32]byte{7}))
	require.NoError(t, err)
	p, err := peer.IDFromPublicKey(pub)
	require.NoError(t, err)

	const sent = 2000
	rec := &pb.Message_Peer{Id: []byte(p), Addrs: bigAddrBytes(sent)}
	pmes := &pb.Message{
		Type:          pb.Message_ADD_PROVIDER,
		Key:           []byte("some-key"),
		ProviderPeers: []*pb.Message_Peer{rec},
	}

	_, err = d.handleAddProvider(ctx, p, pmes)
	require.NoError(t, err)

	stored := d.host.Peerstore().Addrs(p)
	require.NotEmptyf(t, stored, "the provider's addresses must be recorded")
	require.Lessf(t, len(stored), sent,
		"the ingested address list must be trimmed well below what was sent")

	var total int
	for _, a := range stored {
		total += len(a.Bytes())
	}
	require.LessOrEqualf(t, total, pb.MaxPeerRecordSize,
		"stored address bytes for one peer must fit within the per-record cap")
}
