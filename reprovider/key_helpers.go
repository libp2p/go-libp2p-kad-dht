package reprovider

import (
	"crypto/sha256"
	"sort"

	kb "github.com/libp2p/go-libp2p-kbucket"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multihash"
	"github.com/probe-lab/go-libdht/kad"
	"github.com/probe-lab/go-libdht/kad/key"
	"github.com/probe-lab/go-libdht/kad/key/bit256"
	"github.com/probe-lab/go-libdht/kad/key/bitstr"
	"github.com/probe-lab/go-libdht/kad/trie"
)

func mhToBit256(h multihash.Multihash) bit256.Key {
	hash := sha256.Sum256(h)
	return bit256.NewKey(hash[:])
}

func peerIDToBit256(id peer.ID) bit256.Key {
	hash := sha256.Sum256([]byte(id))
	return bit256.NewKey(hash[:])
}

func flipLastBit(k bitstr.Key) bitstr.Key {
	if len(k) == 0 {
		return k
	}
	flipped := byte('0' + '1' - k[len(k)-1])
	return k[:len(k)-1] + bitstr.Key(flipped)
}

// isBitstrPrefix returns true if k0 is a prefix of k1.
func isBitstrPrefix(k0 bitstr.Key, k1 bitstr.Key) bool {
	return len(k0) <= len(k1) && k0 == k1[:len(k0)]
}

const initMask = (byte(1) << 7) // 0x80

// keyToBytes converts a kad.Key to a byte slice. If the provided key has a
// size that isn't a multiple of 8, right pad the resulting byte with 0s.
func keyToBytes[K kad.Key[K]](k K) []byte {
	bitLen := k.BitLen()
	byteLen := (bitLen + 7) / 8
	b := make([]byte, byteLen)

	byteIndex := 0
	mask := initMask
	by := byte(0)

	for i := range bitLen {
		if k.Bit(i) == 1 {
			by |= mask
		}
		mask >>= 1

		if mask == 0 {
			b[byteIndex] = by
			byteIndex++
			by = 0
			mask = initMask
		}
	}
	if mask != initMask {
		b[byteIndex] = by
	}
	return b
}

// shortestCoveredPrefix takes as input the `requested` key and the list of
// closest peers to this key. It returns a prefix of `requested` that is
// covered by these peers.
//
// If every peer shares the same CPL to `requested`, then no deeper zone is
// covered, we learn that the adjacent sibling branch is empty. In this case we
// return the prefix one bit deeper (`minCPL+1`) and an empty peer list.
func shortestCoveredPrefix(requested bitstr.Key, peers []peer.ID) (bitstr.Key, []peer.ID) {
	if len(peers) == 0 {
		return requested, peers
	}
	// Sort the peers by their distance to the requested key.
	peers = kb.SortClosestPeers(peers, keyToBytes(requested))

	minCpl := requested.BitLen() // key bitlen
	coveredCpl := 0
	lastCoveredPeerIndex := 0
	for i, p := range peers {
		cpl := key.CommonPrefixLength(requested, peerIDToBit256(p))
		if cpl < minCpl {
			coveredCpl = cpl + 1
			lastCoveredPeerIndex = i
			minCpl = cpl
		}
	}
	return requested[:coveredCpl], peers[:lastCoveredPeerIndex]
}

type prefixAndCids struct {
	prefix bitstr.Key
	cids   *trie.Trie[bit256.Key, multihash.Multihash]
}

// sortPrefixesBySize sorts the prefixes by the number of CIDs they contain,
// largest first.
func sortPrefixesBySize(prefixes map[bitstr.Key]*trie.Trie[bit256.Key, multihash.Multihash]) []prefixAndCids {
	out := make([]prefixAndCids, 0, len(prefixes))
	for prefix, cids := range prefixes {
		if cids != nil {
			out = append(out, prefixAndCids{prefix: prefix, cids: cids})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].cids.Size() > out[j].cids.Size()
	})
	return out
}
