package keyspace

import (
	"crypto/sha256"
	"sort"

	"github.com/libp2p/go-libp2p/core/peer"
	mh "github.com/multiformats/go-multihash"

	"github.com/probe-lab/go-libdht/kad"
	"github.com/probe-lab/go-libdht/kad/key"
	"github.com/probe-lab/go-libdht/kad/key/bit256"
	"github.com/probe-lab/go-libdht/kad/key/bitstr"
)

// KeyLen is the length of a 256-bit kademlia identifier in bits.
const KeyLen = bit256.KeyLen * 8 // 256

// MhToBit256 converts a multihash to a its 256-bit kademlia identifier by
// hashing it with SHA-256.
func MhToBit256(h mh.Multihash) bit256.Key {
	hash := sha256.Sum256(h)
	return bit256.NewKey(hash[:])
}

// PeerIDToBit256 converts a peer.ID to a its 256-bit kademlia identifier by
// hashing it with SHA-256.
func PeerIDToBit256(id peer.ID) bit256.Key {
	hash := sha256.Sum256([]byte(id))
	return bit256.NewKey(hash[:])
}

// FlipLastBit flips the last bit of the given key.
func FlipLastBit(k bitstr.Key) bitstr.Key {
	if len(k) == 0 {
		return k
	}
	flipped := byte('0' + '1' - k[len(k)-1])
	return k[:len(k)-1] + bitstr.Key(flipped)
}

// FirstFullKeyWithPrefix returns to closest 256-bit key to order, starting
// with the given k as a prefix.
func FirstFullKeyWithPrefix[K kad.Key[K]](k bitstr.Key, order K) bitstr.Key {
	kLen := k.BitLen()
	if kLen > KeyLen {
		return k[:KeyLen]
	}
	return k + bitstr.Key(key.BitString(order))[kLen:]
}

// IsBitstrPrefix returns true if k0 is a prefix of k1.
func IsBitstrPrefix(k0 bitstr.Key, k1 bitstr.Key) bool {
	return len(k0) <= len(k1) && k0 == k1[:len(k0)]
}

// IsPrefix returns true if k0 is a prefix of k1
func IsPrefix[K0 kad.Key[K0], K1 kad.Key[K1]](k0 K0, k1 K1) bool {
	if k0.BitLen() > k1.BitLen() {
		return false
	}
	for i := range k0.BitLen() {
		if k0.Bit(i) != k1.Bit(i) {
			return false
		}
	}
	return true
}

const initMask = (byte(1) << 7) // 0x80

// KeyToBytes converts a kad.Key to a byte slice. If the provided key has a
// size that isn't a multiple of 8, right pad the resulting byte with 0s.
func KeyToBytes[K kad.Key[K]](k K) []byte {
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

// PrefixAndKeys is a struct that holds a prefix and the multihashes whose
// kademlia identifier share the same prefix.
type PrefixAndKeys struct {
	Prefix bitstr.Key
	Keys   []mh.Multihash
}

// SortPrefixesBySize sorts the prefixes by the number of keys they contain,
// largest first.
func SortPrefixesBySize(prefixes map[bitstr.Key][]mh.Multihash) []PrefixAndKeys {
	out := make([]PrefixAndKeys, 0, len(prefixes))
	for prefix, keys := range prefixes {
		if keys != nil {
			out = append(out, PrefixAndKeys{Prefix: prefix, Keys: keys})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return len(out[i].Keys) > len(out[j].Keys)
	})
	return out
}
