package dht_pb

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestBadAddrsDontReturnNil(t *testing.T) {
	mp := new(Message_Peer)
	mp.Addrs = [][]byte{[]byte("NOT A VALID MULTIADDR")}

	addrs := mp.Addresses()
	if len(addrs) > 0 {
		t.Fatal("shouldnt have any multiaddrs")
	}
}

// rawAddrs returns n byte slices of the given length, standing in for the wire
// bytes of multiaddrs; boundPeerRecordAddrs only reads their lengths.
func rawAddrs(n, size int) [][]byte {
	addrs := make([][]byte, n)
	for i := range addrs {
		addrs[i] = bytes.Repeat([]byte{0xAB}, size)
	}
	return addrs
}

func TestBoundPeerRecordAddrs(t *testing.T) {
	id := []byte("a-peer-id-of-a-realistic-length--")

	tests := []struct {
		name  string
		addrs [][]byte
		// keptAll asserts every address survives; otherwise the record is
		// expected to be trimmed to fit MaxPeerRecordSize.
		keptAll bool
	}{
		{name: "no addresses", addrs: nil, keptAll: true},
		{name: "well under the limit", addrs: rawAddrs(10, 40), keptAll: true},
		{name: "many small addresses over the limit", addrs: rawAddrs(4000, 40)},
		{name: "few large addresses over the limit", addrs: rawAddrs(20, 1024)},
		{name: "single address larger than the limit", addrs: rawAddrs(1, MaxPeerRecordSize*2)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			original := append([][]byte(nil), tt.addrs...)
			pbp := &Message_Peer{Id: id, Addrs: tt.addrs, Connection: Message_CONNECTED}

			boundPeerRecordAddrs(pbp)

			require.LessOrEqualf(t, proto.Size(pbp), MaxPeerRecordSize,
				"record must serialize within MaxPeerRecordSize")
			require.LessOrEqualf(t, len(pbp.Addrs), len(original),
				"bounding must not add addresses")
			require.Equalf(t, original[:len(pbp.Addrs)], pbp.Addrs,
				"kept addresses must be a front prefix of the original")

			if tt.keptAll {
				require.Lenf(t, pbp.Addrs, len(original), "record within the limit must be untouched")
			} else {
				require.Lessf(t, len(pbp.Addrs), len(original), "over-limit record must be trimmed")
			}
		})
	}
}

func TestBoundPeerRecordAddrsNilSafe(t *testing.T) {
	require.NotPanics(t, func() { boundPeerRecordAddrs(nil) })
}

// peerSerializingTo builds a Message_Peer with the given connection flag whose
// serialized size is exactly total bytes. It carries a fixed peer ID, a small
// leading "anchor" address that always survives trimming and a trailing "filler"
// address padded to hit total, so trimming drops the filler first.
func peerSerializingTo(t *testing.T, total int, conn Message_ConnectionType) *Message_Peer {
	t.Helper()
	id := bytes.Repeat([]byte{0x01}, 32)
	anchor := bytes.Repeat([]byte{0xAB}, 64)

	fixed := &Message_Peer{Id: id, Connection: conn, Addrs: [][]byte{anchor}}
	// A filler length in the two-byte varint range frames as tag(1)+len(2)+bytes,
	// so subtract that 3-byte framing to land the whole record exactly on total.
	filler := bytes.Repeat([]byte{0xCD}, total-proto.Size(fixed)-3)

	pbp := &Message_Peer{Id: id, Connection: conn, Addrs: [][]byte{anchor, filler}}
	require.Equalf(t, total, proto.Size(pbp), "constructed record must serialize to %d bytes", total)
	return pbp
}

func TestBoundPeerRecordAddrsAtCapBoundary(t *testing.T) {
	t.Run("exactly at the cap keeps every address", func(t *testing.T) {
		pbp := peerSerializingTo(t, MaxPeerRecordSize, Message_CONNECTED)
		kept := len(pbp.Addrs)

		boundPeerRecordAddrs(pbp)

		require.Lenf(t, pbp.Addrs, kept, "a record exactly at the cap must keep every address")
		require.Equalf(t, MaxPeerRecordSize, proto.Size(pbp), "an untrimmed record keeps its size")
	})

	t.Run("one byte over the cap trims one address", func(t *testing.T) {
		pbp := peerSerializingTo(t, MaxPeerRecordSize+1, Message_CONNECTED)
		kept := len(pbp.Addrs)

		boundPeerRecordAddrs(pbp)

		require.Lenf(t, pbp.Addrs, kept-1, "a record one byte over the cap must trim exactly one address")
		require.LessOrEqualf(t, proto.Size(pbp), MaxPeerRecordSize, "the trimmed record must fit the cap")
	})
}

// TestBoundPeerRecordAddrsReservesConnection covers what PeerInfosToPBPeers does:
// it bounds a record before setting Connection, so the bound must reserve room
// for the connection field. A record bounded right at the cap with Connection
// still unset must keep fitting once Connection is set to a non-default value.
func TestBoundPeerRecordAddrsReservesConnection(t *testing.T) {
	pbp := peerSerializingTo(t, MaxPeerRecordSize, Message_NOT_CONNECTED)

	boundPeerRecordAddrs(pbp)
	pbp.Connection = Message_CANNOT_CONNECT // set after bounding, as PeerInfosToPBPeers does

	require.LessOrEqualf(t, proto.Size(pbp), MaxPeerRecordSize,
		"setting Connection after bounding must not push the record past the cap")
}

// manyMultiaddrs builds n distinct valid multiaddrs whose combined size far
// exceeds MaxPeerRecordSize.
func manyMultiaddrs(t *testing.T, n int) []ma.Multiaddr {
	t.Helper()
	addrs := make([]ma.Multiaddr, n)
	for i := range addrs {
		a, err := ma.NewMultiaddr(fmt.Sprintf("/ip4/1.2.3.4/tcp/%d", 1024+i))
		require.NoErrorf(t, err, "building multiaddr %d", i)
		addrs[i] = a
	}
	return addrs
}

func TestRawPeerInfosToPBPeersBoundsEgress(t *testing.T) {
	const n = 4000
	ai := peer.AddrInfo{ID: peer.ID("provider-peer-id"), Addrs: manyMultiaddrs(t, n)}

	pbps := RawPeerInfosToPBPeers([]peer.AddrInfo{ai})

	require.Len(t, pbps, 1)
	require.LessOrEqualf(t, proto.Size(pbps[0]), MaxPeerRecordSize,
		"outbound record must be bounded")
	require.Lessf(t, len(pbps[0].Addrs), n, "outbound record must drop addresses past the limit")
	require.NotEmptyf(t, pbps[0].Addrs, "a peer with small addresses must keep some")
}

func TestPBPeersToPeerInfosBoundsIngress(t *testing.T) {
	const n = 4000
	addrs := manyMultiaddrs(t, n)
	rawAddrs := make([][]byte, n)
	for i, a := range addrs {
		rawAddrs[i] = a.Bytes()
	}
	pbp := &Message_Peer{Id: []byte("remote-peer-id"), Addrs: rawAddrs, Connection: Message_CONNECTED}

	infos := PBPeersToPeerInfos([]*Message_Peer{pbp})

	require.Len(t, infos, 1)
	require.LessOrEqualf(t, proto.Size(pbp), MaxPeerRecordSize,
		"ingested record must be bounded in place")
	require.Lessf(t, len(infos[0].Addrs), n, "ingested addresses must be trimmed to the limit")
	require.NotEmptyf(t, infos[0].Addrs, "valid small addresses within the limit must survive")
}
