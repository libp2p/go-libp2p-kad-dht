package xortrie

import "bytes"

// TrieKey is a vector of bits backed by a Go byte slice in big endian byte order and big-endian bit order.
type TrieKey []byte

func (bs TrieKey) BitAt(offset int) byte {
	if bs[offset/8]&(1<<(offset%8)) == 0 {
		return 0
	} else {
		return 1
	}
}

func (bs TrieKey) BitLen() int {
	return 8 * len(bs)
}

func TrieKeyEqual(x, y TrieKey) bool {
	return bytes.Equal(x, y)
}
