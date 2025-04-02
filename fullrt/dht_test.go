package fullrt

import (
	"context"
	"crypto/rand"
	"strconv"
	"testing"

	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	kadkey "github.com/libp2p/go-libp2p-xor/key"
	"github.com/libp2p/go-libp2p-xor/trie"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
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
	dht, err := NewFullRT(h, "", DHTOption(dht.BootstrapPeers(dht.GetDefaultBootstrapPeerAddrInfos()...)))
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
