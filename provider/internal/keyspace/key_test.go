package keyspace

import (
	"crypto/rand"
	"strconv"
	"strings"
	"testing"

	"github.com/ipfs/go-test/random"
	kb "github.com/libp2p/go-libp2p-kbucket"
	"github.com/libp2p/go-libp2p/core/peer"
	mh "github.com/multiformats/go-multihash"

	"github.com/ipfs/go-libdht/kad/key"
	"github.com/ipfs/go-libdht/kad/key/bit256"
	"github.com/ipfs/go-libdht/kad/key/bitstr"

	"github.com/stretchr/testify/require"
)

func TestFlipLastBit(t *testing.T) {
	require.Equal(t, FlipLastBit(""), bitstr.Key(""))
	require.Equal(t, FlipLastBit("0"), bitstr.Key("1"))
	require.Equal(t, FlipLastBit("1"), bitstr.Key("0"))
	require.Equal(t, FlipLastBit("00"), bitstr.Key("01"))
	require.Equal(t, FlipLastBit("00000000"), bitstr.Key("00000001"))
}

func TestFirstFullKeyWithPrefix(t *testing.T) {
	zeroKey := bitstr.Key(strings.Repeat("0", KeyLen))
	oneKey := bitstr.Key(strings.Repeat("1", KeyLen))

	require.Equal(t, zeroKey, FirstFullKeyWithPrefix(bitstr.Key(""), zeroKey))
	require.Equal(t, zeroKey, FirstFullKeyWithPrefix(bitstr.Key("0"), zeroKey))
	require.Equal(t, bitstr.Key("000"+strings.Repeat("1", KeyLen-3)), FirstFullKeyWithPrefix(bitstr.Key("000"), oneKey))
	require.Equal(t, zeroKey, FirstFullKeyWithPrefix(zeroKey, zeroKey))
	require.Equal(t, oneKey, FirstFullKeyWithPrefix(oneKey, zeroKey))
	require.Equal(t, zeroKey, FirstFullKeyWithPrefix(zeroKey+"1", zeroKey))
}

func TestIsPrefix(t *testing.T) {
	require.True(t, IsPrefix(bitstr.Key(""), bitstr.Key("")))
	require.True(t, IsPrefix(bitstr.Key(""), bitstr.Key("1")))
	require.True(t, IsPrefix(bitstr.Key("0"), bitstr.Key("0")))
	require.True(t, IsPrefix(bitstr.Key("0"), bitstr.Key("01")))
	require.True(t, IsPrefix(bitstr.Key("1"), bitstr.Key("11")))
	require.True(t, IsPrefix(bitstr.Key("0"), bitstr.Key("00000000")))
	require.True(t, IsPrefix(bitstr.Key("0101010"), bitstr.Key("01010100")))
	require.True(t, IsPrefix(bitstr.Key("0101010"), bitstr.Key("01010101")))

	require.False(t, IsPrefix(bitstr.Key("1"), bitstr.Key("")))
	require.False(t, IsPrefix(bitstr.Key("1"), bitstr.Key("0")))
	require.False(t, IsPrefix(bitstr.Key("0"), bitstr.Key("1")))
	require.False(t, IsPrefix(bitstr.Key("00"), bitstr.Key("0")))
}

func TestIsBitstrPrefix(t *testing.T) {
	fullKey := bitstr.Key("000")
	require.True(t, IsBitstrPrefix(bitstr.Key(""), fullKey))
	require.True(t, IsBitstrPrefix(bitstr.Key("0"), fullKey))
	require.True(t, IsBitstrPrefix(bitstr.Key("00"), fullKey))
	require.True(t, IsBitstrPrefix(bitstr.Key("000"), fullKey))
	require.False(t, IsBitstrPrefix(bitstr.Key("1"), fullKey))
	require.False(t, IsBitstrPrefix(bitstr.Key("01"), fullKey))
	require.False(t, IsBitstrPrefix(bitstr.Key("001"), fullKey))
	require.False(t, IsBitstrPrefix(bitstr.Key("0000"), fullKey))
}

func TestKeyToBytes(t *testing.T) {
	nKeys := 1 << 8
	var buf [bit256.KeyLen]byte
	for range nKeys {
		if _, err := rand.Read(buf[:]); err != nil {
			t.Fatal(err)
		}
		b256 := bit256.NewKeyFromArray(buf)
		bstr := bitstr.Key(key.BitString(b256))
		require.Equal(t, buf[:], KeyToBytes(b256))
		require.Equal(t, buf[:], KeyToBytes(bstr))
	}
}

func TestKeyToBytesPadding(t *testing.T) {
	k := bitstr.Key("")
	bs := KeyToBytes(k)
	require.Equal(t, []byte{}, bs)

	k = bitstr.Key("1")
	bs = KeyToBytes(k)
	require.Equal(t, []byte{0b10000000}, bs)

	k = bitstr.Key("0")
	bs = KeyToBytes(k)
	require.Equal(t, []byte{0b00000000}, bs)

	k = bitstr.Key("111111") // 6 ones
	bs = KeyToBytes(k)
	require.Equal(t, []byte{0b11111100}, bs)

	k = bitstr.Key("00000000") // 8 zeros
	bs = KeyToBytes(k)
	require.Equal(t, []byte{0b00000000}, bs)

	k = bitstr.Key("11111111") // 8 ones
	bs = KeyToBytes(k)
	require.Equal(t, []byte{0b11111111}, bs)

	k = bitstr.Key("000000000") // 9 zeros
	bs = KeyToBytes(k)
	require.Equal(t, []byte{0b00000000, 0b00000000}, bs)

	k = bitstr.Key("111111111") // 9 ones
	bs = KeyToBytes(k)
	require.Equal(t, []byte{0b11111111, 0b10000000}, bs)
}

func TestShortestCoveredPrefix(t *testing.T) {
	// All keys share CPL of 5, except one sharing a CPL of 4
	var target [bit256.KeyLen]byte
	_, err := rand.Read(target[:])
	require.NoError(t, err)
	targetBitstr := bitstr.Key(key.BitString(bit256.NewKeyFromArray(target)))

	cpl := 5
	nPeers := 16
	peers := make([]peer.ID, nPeers)
	for i := range peers {
		peers[i], err = kb.GenRandPeerIDWithCPL(target[:], uint(cpl))
		require.NoError(t, err)
	}

	// This is a corner case.
	// All peers share exactly `cpl` bits with the target, meaning that the
	// prefix with `cpl+1` bits has been fully covered and contains 0 peers. No
	// peers match this covered prefix.
	prefix, coveredPeers := ShortestCoveredPrefix(targetBitstr, peers)
	require.Len(t, coveredPeers, 0)
	require.Equal(t, targetBitstr[:cpl+1], prefix)

	// Last peer has a lower CPL
	peers[len(peers)-1], err = kb.GenRandPeerIDWithCPL(target[:], uint(cpl-1))
	require.NoError(t, err)
	prefix, coveredPeers = ShortestCoveredPrefix(targetBitstr, peers)
	require.Len(t, coveredPeers, len(peers)-1)
	require.Equal(t, targetBitstr[:cpl], prefix)
	peers[len(peers)-1], err = kb.GenRandPeerIDWithCPL(target[:], uint(cpl))
	require.NoError(t, err)

	// First peer has a lower CPL
	peers[0], err = kb.GenRandPeerIDWithCPL(target[:], uint(cpl-1))
	require.NoError(t, err)
	prefix, coveredPeers = ShortestCoveredPrefix(targetBitstr, peers)
	require.Len(t, coveredPeers, len(peers)-1)
	require.Equal(t, targetBitstr[:cpl], prefix)

	// First peer has a much lower CPL
	peers[0], err = kb.GenRandPeerIDWithCPL(target[:], uint(cpl-3))
	require.NoError(t, err)
	prefix, coveredPeers = ShortestCoveredPrefix(targetBitstr, peers)
	require.Len(t, coveredPeers, len(peers)-1)
	require.Equal(t, targetBitstr[:cpl-2], prefix)

	// First peer has a higher CPL
	peers[0], err = kb.GenRandPeerIDWithCPL(target[:], uint(cpl+1))
	require.NoError(t, err)
	prefix, coveredPeers = ShortestCoveredPrefix(targetBitstr, peers)
	require.Len(t, coveredPeers, 1)
	require.Equal(t, targetBitstr[:cpl+1], prefix)

	// First peer has a much higher CPL
	peers[0], err = kb.GenRandPeerIDWithCPL(target[:], uint(cpl+3))
	require.NoError(t, err)
	prefix, coveredPeers = ShortestCoveredPrefix(targetBitstr, peers)
	require.Len(t, coveredPeers, 1)
	require.Equal(t, targetBitstr[:cpl+1], prefix)

	// Test with random peer ids
	nIterations := 64
	for range nIterations {
		minCpl := KeyLen
		largestCplCount := 0
		peers = random.Peers(nPeers)
		peers = kb.SortClosestPeers(peers, target[:])
		for i := range peers {
			cpl = kb.CommonPrefixLen(kb.ConvertPeerID(peers[i]), target[:])
			if cpl < minCpl {
				minCpl = cpl
				largestCplCount = 1
			} else {
				largestCplCount++
			}
		}
		prefix, coveredPeers = ShortestCoveredPrefix(targetBitstr, peers)
		require.Len(t, coveredPeers, len(peers)-largestCplCount)
		require.Equal(t, targetBitstr[:minCpl+1], prefix)
	}

	// Test without supplying peers
	bstrTarget := bitstr.Key("110101111")
	prefix, coveredPeers = ShortestCoveredPrefix(bstrTarget, nil)
	require.Equal(t, bitstr.Key(""), prefix)
	require.Empty(t, coveredPeers)

	// Supply a single peer exactly matching the target, it should be returned
	// and covered prefix should match the target.
	p := random.Peers(1)[0]
	bstrTarget = bitstr.Key(key.BitString(PeerIDToBit256(p)))
	peers = []peer.ID{p}
	prefix, coveredPeers = ShortestCoveredPrefix(bstrTarget, peers)
	require.Equal(t, bstrTarget, prefix)
	require.Equal(t, peers, coveredPeers)

	// Supply a single peer matching a 255 bit target should return the peer and
	// covered prefix should be full 256 bit key
	bstrTarget255 := bstrTarget[:len(bstrTarget)-1]
	prefix, coveredPeers = ShortestCoveredPrefix(bstrTarget255, peers)
	require.Equal(t, bstrTarget, prefix)
	require.Equal(t, peers, coveredPeers)

	// If empty prefix is targetted and a single peer is supplied, it always
	// matches and the covered prefix is the full key of the peer.
	prefix, coveredPeers = ShortestCoveredPrefix("", peers)
	require.Equal(t, bstrTarget, prefix)
	require.Equal(t, peers, coveredPeers)

	// Supply a single peer not matching the target, should return empty covered
	// prefix and no covered peers.
	bstrTarget = FlipLastBit(bstrTarget)
	prefix, coveredPeers = ShortestCoveredPrefix(bstrTarget, peers)
	require.Equal(t, bitstr.Key(""), prefix)
	require.Empty(t, coveredPeers)
}

func TestExtendBinaryPrefix(t *testing.T) {
	prefix := bitstr.Key("")
	l := 1
	require.Equal(t, []bitstr.Key{"0", "1"}, ExtendBinaryPrefix(prefix, l))
	prefix = bitstr.Key("1101")
	l = 6
	require.Equal(t, []bitstr.Key{"110100", "110101", "110110", "110111"}, ExtendBinaryPrefix(prefix, l))
}

func TestSiblingPrefixes(t *testing.T) {
	k := bitstr.Key("")
	require.Empty(t, SiblingPrefixes(k))
	k = bitstr.Key("0")
	require.Equal(t, []bitstr.Key{"1"}, SiblingPrefixes(k))
	k = bitstr.Key("1")
	require.Equal(t, []bitstr.Key{"0"}, SiblingPrefixes(k))
	k = bitstr.Key("00")
	require.Equal(t, []bitstr.Key{"1", "01"}, SiblingPrefixes(k))
	k = bitstr.Key("000")
	require.Equal(t, []bitstr.Key{"1", "01", "001"}, SiblingPrefixes(k))
	k = bitstr.Key("1100")
	require.Equal(t, []bitstr.Key{"0", "10", "111", "1101"}, SiblingPrefixes(k))
}

func genMultihashes(n int) []mh.Multihash {
	mhs := make([]mh.Multihash, n)
	for i := range mhs {
		h, err := mh.Sum([]byte(strconv.Itoa(i)), mh.SHA2_256, -1)
		if err != nil {
			panic(err)
		}
		mhs[i], err = mh.Encode(h, mh.SHA2_256)
		if err != nil {
			panic(err)
		}
	}
	return mhs
}

func TestSortPrefixesBySize(t *testing.T) {
	prefixLen := 6
	allocations := make(map[bitstr.Key][]mh.Multihash, 1<<prefixLen)
	for _, h := range genMultihashes(1 << 10) {
		k := MhToBit256(h)
		prefix := bitstr.Key(key.BitString(k)[:prefixLen])
		allocations[prefix] = append(allocations[prefix], h)
	}

	sorted := SortPrefixesBySize(allocations)

	for i := range len(sorted) - 1 {
		if len(sorted[i].Keys) < len(sorted[i+1].Keys) {
			t.Fatal("PrefixAndKeys not sorted by number of keys")
		}
	}
}
