package reprovider

import (
	"crypto/rand"
	"testing"

	kb "github.com/libp2p/go-libp2p-kbucket"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/probe-lab/go-libdht/kad/key"
	"github.com/probe-lab/go-libdht/kad/key/bit256"
	"github.com/probe-lab/go-libdht/kad/key/bitstr"
	"github.com/stretchr/testify/require"
)

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
	prefix, coveredPeers := shortestCoveredPrefix(targetBitstr, peers)
	require.Len(t, coveredPeers, 0)
	require.Equal(t, targetBitstr[:cpl+1], prefix)

	// Last peer has a lower CPL
	peers[len(peers)-1], err = kb.GenRandPeerIDWithCPL(target[:], uint(cpl-1))
	require.NoError(t, err)
	prefix, coveredPeers = shortestCoveredPrefix(targetBitstr, peers)
	require.Len(t, coveredPeers, len(peers)-1)
	require.Equal(t, targetBitstr[:cpl], prefix)
	peers[len(peers)-1], err = kb.GenRandPeerIDWithCPL(target[:], uint(cpl))
	require.NoError(t, err)

	// First peer has a lower CPL
	peers[0], err = kb.GenRandPeerIDWithCPL(target[:], uint(cpl-1))
	require.NoError(t, err)
	prefix, coveredPeers = shortestCoveredPrefix(targetBitstr, peers)
	require.Len(t, coveredPeers, len(peers)-1)
	require.Equal(t, targetBitstr[:cpl], prefix)

	// First peer has a much lower CPL
	peers[0], err = kb.GenRandPeerIDWithCPL(target[:], uint(cpl-3))
	require.NoError(t, err)
	prefix, coveredPeers = shortestCoveredPrefix(targetBitstr, peers)
	require.Len(t, coveredPeers, len(peers)-1)
	require.Equal(t, targetBitstr[:cpl-2], prefix)

	// First peer has a higher CPL
	peers[0], err = kb.GenRandPeerIDWithCPL(target[:], uint(cpl+1))
	require.NoError(t, err)
	prefix, coveredPeers = shortestCoveredPrefix(targetBitstr, peers)
	require.Len(t, coveredPeers, 1)
	require.Equal(t, targetBitstr[:cpl+1], prefix)

	// First peer has a much higher CPL
	peers[0], err = kb.GenRandPeerIDWithCPL(target[:], uint(cpl+3))
	require.NoError(t, err)
	prefix, coveredPeers = shortestCoveredPrefix(targetBitstr, peers)
	require.Len(t, coveredPeers, 1)
	require.Equal(t, targetBitstr[:cpl+1], prefix)

	// Test with random peer ids
	nIterations := 64
	for range nIterations {
		minCpl := keyLen
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
		prefix, coveredPeers = shortestCoveredPrefix(targetBitstr, peers)
		require.Len(t, coveredPeers, len(peers)-largestCplCount)
		require.Equal(t, targetBitstr[:minCpl+1], prefix)
	}
}

func TestIsBitstrPrefix(t *testing.T) {
	fullKey := bitstr.Key("000")
	require.True(t, isBitstrPrefix(bitstr.Key(""), fullKey))
	require.True(t, isBitstrPrefix(bitstr.Key("0"), fullKey))
	require.True(t, isBitstrPrefix(bitstr.Key("00"), fullKey))
	require.True(t, isBitstrPrefix(bitstr.Key("000"), fullKey))
	require.False(t, isBitstrPrefix(bitstr.Key("1"), fullKey))
	require.False(t, isBitstrPrefix(bitstr.Key("01"), fullKey))
	require.False(t, isBitstrPrefix(bitstr.Key("001"), fullKey))
	require.False(t, isBitstrPrefix(bitstr.Key("0000"), fullKey))
}
