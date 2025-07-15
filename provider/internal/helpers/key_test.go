package helpers

import (
	"crypto/rand"
	"strconv"
	"testing"

	kb "github.com/libp2p/go-libp2p-kbucket"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	mh "github.com/multiformats/go-multihash"

	"github.com/probe-lab/go-libdht/kad/key"
	"github.com/probe-lab/go-libdht/kad/key/bit256"
	"github.com/probe-lab/go-libdht/kad/key/bitstr"

	"github.com/stretchr/testify/require"
)

func TestFlipLastBit(t *testing.T) {
	require.Equal(t, FlipLastBit(""), bitstr.Key(""))
	require.Equal(t, FlipLastBit("0"), bitstr.Key("1"))
	require.Equal(t, FlipLastBit("1"), bitstr.Key("0"))
	require.Equal(t, FlipLastBit("00"), bitstr.Key("01"))
	require.Equal(t, FlipLastBit("00000000"), bitstr.Key("00000001"))
}

func TestIsPrefix(t *testing.T) {
	require.True(t, isPrefix(bitstr.Key(""), bitstr.Key("")))
	require.True(t, isPrefix(bitstr.Key(""), bitstr.Key("1")))
	require.True(t, isPrefix(bitstr.Key("0"), bitstr.Key("0")))
	require.True(t, isPrefix(bitstr.Key("0"), bitstr.Key("01")))
	require.True(t, isPrefix(bitstr.Key("1"), bitstr.Key("11")))
	require.True(t, isPrefix(bitstr.Key("0"), bitstr.Key("00000000")))
	require.True(t, isPrefix(bitstr.Key("0101010"), bitstr.Key("01010100")))
	require.True(t, isPrefix(bitstr.Key("0101010"), bitstr.Key("01010101")))

	require.False(t, isPrefix(bitstr.Key("1"), bitstr.Key("")))
	require.False(t, isPrefix(bitstr.Key("1"), bitstr.Key("0")))
	require.False(t, isPrefix(bitstr.Key("0"), bitstr.Key("1")))
	require.False(t, isPrefix(bitstr.Key("00"), bitstr.Key("0")))
}

func genRandPeerID(t *testing.T) peer.ID {
	_, pub, err := crypto.GenerateKeyPair(crypto.Ed25519, -1)
	require.NoError(t, err)
	pid, err := peer.IDFromPublicKey(pub)
	require.NoError(t, err)
	return pid
}

func TestShortestCoveredPrefix(t *testing.T) {
	// All keys share CPL of 5, except one sharing a CPL of 4
	var target [32]byte
	_, err := rand.Read(target[:])
	require.NoError(t, err)
	targetBitstr := bitstr.Key(key.BitString(bit256.NewKey(target[:])))

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
		for i := range peers {
			peers[i] = genRandPeerID(t)
		}
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
		keys, ok := allocations[prefix]
		if !ok {
			allocations[prefix] = []mh.Multihash{h}
		} else {
			allocations[prefix] = append(keys, h)
		}
	}

	sorted := SortPrefixesBySize(allocations)

	for i := range len(sorted) - 1 {
		if len(sorted[i].Keys) < len(sorted[i+1].Keys) {
			t.Fatal("PrefixAndKeys not sorted by number of keys")
		}
	}
}
