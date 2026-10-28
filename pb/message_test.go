package dht_pb

import (
	"bytes"
	"errors"
	"fmt"
	"runtime"
	"testing"

	"github.com/ipfs/go-test/random"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
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

const maxFrame = 4 << 20

var tcpAddr = ma.StringCast("/ip4/1.2.3.4/tcp/4001").Bytes()

// realPeers builds n well-formed peer entries, each with a real Ed25519 peer ID
// and one binary dial address, as this repo's own encoder emits them.
func realPeers(n int) []*Message_Peer {
	peers := make([]*Message_Peer, n)
	for i, id := range random.Peers(n) {
		peers[i] = &Message_Peer{Id: []byte(id), Addrs: [][]byte{tcpAddr}}
	}
	return peers
}

// shortestPeerIDs returns n peer IDs in their shortest real form: a 34-byte
// sha256 multihash, as RSA keys produce.
func shortestPeerIDs(n int) []peer.ID {
	ids := make([]peer.ID, n)
	for i, mh := range random.Multihashes(n) {
		ids[i] = peer.ID(mh)
	}
	return ids
}

// peerWithAddrs builds a peer entry carrying addr nAddrs times.
func peerWithAddrs(nAddrs int, addr []byte) *Message_Peer {
	p := realPeers(1)[0]
	p.Addrs = make([][]byte, nAddrs)
	for i := range p.Addrs {
		p.Addrs[i] = addr
	}
	return p
}

// singlePeerEntry wraps body as the only closerPeers entry of a message.
func singlePeerEntry(body []byte) []byte {
	frame := protowire.AppendTag(nil, closerPeersField, protowire.BytesType)
	return protowire.AppendBytes(frame, body)
}

func TestUnmarshalRejectsEmptyEntries(t *testing.T) {
	tests := []struct {
		name    string
		payload []byte
	}{
		{"empty closer peers", append([]byte{0x08, 0x04}, bytes.Repeat([]byte{0x42, 0x00}, (maxFrame-2)/2)...)},
		{"empty provider peers", append([]byte{0x08, 0x02}, bytes.Repeat([]byte{0x4a, 0x00}, (maxFrame-2)/2)...)},
		{"empty nested addrs", singlePeerEntry(bytes.Repeat([]byte{0x12, 0x00}, 2_000_000))},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var m Message
			require.ErrorIs(t, Unmarshal(tt.payload, &m), ErrTooManyPeerEntries)
			require.Empty(t, m.CloserPeers)
			require.Empty(t, m.ProviderPeers)
			// The message is rejected before decoding starts.
			allocs := testing.AllocsPerRun(10, func() { _ = Unmarshal(tt.payload, &m) })
			require.Zerof(t, allocs, "rejecting the message allocated %v times", allocs)
		})
	}
}

func TestUnmarshalAcceptsWellFormed(t *testing.T) {
	closerBucket := &Message{Type: Message_FIND_NODE, CloserPeers: realPeers(20)}
	manyProviders := &Message{Type: Message_GET_PROVIDERS, ProviderPeers: realPeers(5000)}
	// The shortest real peer IDs with no addresses sit exactly at the per-peer
	// charge.
	shortIDs := &Message{Type: Message_GET_PROVIDERS}
	for _, id := range shortestPeerIDs(5000) {
		shortIDs.ProviderPeers = append(shortIDs.ProviderPeers, &Message_Peer{Id: []byte(id)})
	}
	// A peer filling MaxPeerRecordSize with real dial addresses, and one filling
	// it with the shortest valid multiaddr, which sits at the per-address charge.
	fatPeer := &Message{Type: Message_FIND_NODE, CloserPeers: []*Message_Peer{peerWithAddrs(800, tcpAddr)}}
	circuitPeer := &Message{Type: Message_FIND_NODE, CloserPeers: []*Message_Peer{
		peerWithAddrs(2000, ma.StringCast("/p2p-circuit").Bytes()),
	}}

	tests := []struct {
		name string
		msg  *Message
	}{
		{"empty message", &Message{Type: Message_FIND_NODE}},
		{"bucket of closer peers", closerBucket},
		{"many real provider peers", manyProviders},
		{"providers with shortest peer IDs", shortIDs},
		{"peer with many real addrs", fatPeer},
		{"peer with many shortest addrs", circuitPeer},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := proto.Marshal(tt.msg)
			require.NoError(t, err)
			var got Message
			require.NoError(t, Unmarshal(data, &got))
			require.Len(t, got.CloserPeers, len(tt.msg.CloserPeers))
			require.Len(t, got.ProviderPeers, len(tt.msg.ProviderPeers))
		})
	}
}

// TestUnmarshalAcceptsShortestValidAddrs checks a response relaying
// third-party records: a valid multiaddr can be as short as two bytes
// (/p2p-circuit, /webrtc), and a server relays whatever addresses its peerstore
// holds, so such a response must decode.
func TestUnmarshalAcceptsShortestValidAddrs(t *testing.T) {
	shortAddrs := []string{"/p2p-circuit", "/webrtc", "/dns4/ab"}
	ids := shortestPeerIDs(60)
	peerWith := func(id peer.ID, addrs ...string) peer.AddrInfo {
		info := peer.AddrInfo{ID: id}
		for _, a := range addrs {
			info.Addrs = append(info.Addrs, ma.StringCast(a))
		}
		return info
	}

	var closer, providers []peer.AddrInfo
	for i := range 20 {
		closer = append(closer, peerWith(ids[i], "/ip4/1.2.3.4/tcp/4001", "/ip4/1.2.3.4/udp/4001/quic-v1"))
	}
	for i := range 40 {
		var addrs []string
		for j := range 64 { // pstoremem's per-peer address cap
			addrs = append(addrs, shortAddrs[j%len(shortAddrs)])
		}
		providers = append(providers, peerWith(ids[20+i], addrs...))
	}
	resp := &Message{
		Type:          Message_GET_PROVIDERS,
		Key:           make([]byte, 34),
		CloserPeers:   RawPeerInfosToPBPeers(closer),
		ProviderPeers: RawPeerInfosToPBPeers(providers),
	}

	data, err := proto.Marshal(resp)
	require.NoError(t, err)
	var got Message
	require.NoError(t, Unmarshal(data, &got))
	require.Len(t, got.ProviderPeers, len(providers))
}

func TestUnmarshalPassesThroughDecodeErrors(t *testing.T) {
	var m Message
	err := Unmarshal([]byte{0xff, 0xff, 0xff}, &m)
	require.Error(t, err)
	require.False(t, errors.Is(err, ErrTooManyPeerEntries), "a decode error must not be reported as the bound")
}

// paddedFrame builds a 4 MB frame from entries plus a key field filling the
// rest.
func paddedFrame(entries []byte) []byte {
	msg := []byte{0x08, 0x04} // type = FIND_NODE
	msg = protowire.AppendTag(msg, 2, protowire.BytesType)
	msg = protowire.AppendBytes(msg, make([]byte, maxFrame-len(entries)-16))
	return append(msg, entries...)
}

// TestUnmarshalDecodedSizeProportional checks that the largest message
// the bound accepts, for each kind of small entry, decodes to a small multiple
// of its frame size.
func TestUnmarshalDecodedSizeProportional(t *testing.T) {
	const maxRetainedMB = 40.0
	unwrapped := func(b []byte) []byte { return b }
	// A peer entry holding only a 34-byte ID is exactly what the bound charges,
	// so it gives the largest entry count a real-shaped message can reach.
	minimalPeer := singlePeerEntry(
		protowire.AppendBytes(protowire.AppendTag(nil, peerIDField, protowire.BytesType), make([]byte, 34)))

	tests := []struct {
		name  string
		entry []byte
		wrap  func([]byte) []byte
	}{
		{"empty peer entries", []byte{0x42, 0x00}, unwrapped},
		{"minimal real-shaped peers", minimalPeer, unwrapped},
		{"empty nested addrs", []byte{0x12, 0x00}, singlePeerEntry},
		{"one-byte nested addrs", []byte{0x12, 0x01, 0x00}, singlePeerEntry},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Largest count the decoder accepts, found by bisection.
			lo, hi := 0, (maxFrame-64)/len(tt.entry)
			for lo < hi {
				mid := (lo + hi + 1) / 2
				var m Message
				if Unmarshal(paddedFrame(tt.wrap(bytes.Repeat(tt.entry, mid))), &m) == nil {
					lo = mid
				} else {
					hi = mid - 1
				}
			}
			payload := paddedFrame(tt.wrap(bytes.Repeat(tt.entry, lo)))

			var before, after runtime.MemStats
			runtime.GC()
			runtime.ReadMemStats(&before)
			m := new(Message)
			require.NoError(t, Unmarshal(payload, m))
			runtime.GC()
			runtime.ReadMemStats(&after)
			runtime.KeepAlive(m)

			got := float64(after.HeapAlloc-before.HeapAlloc) / (1 << 20)
			require.Lessf(t, got, maxRetainedMB, "accepted %d entries decoded to %.1f MB", lo, got)
		})
	}
}
