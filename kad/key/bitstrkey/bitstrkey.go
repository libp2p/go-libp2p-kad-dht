package bitstrkey

import (
	"github.com/libp2p/go-libdht/kad"
	"github.com/libp2p/go-libdht/kad/key"
)

// BitStrKey is a key represented by a string of 1's and 0's
type BitStrKey string

var _ kad.Key[BitStrKey] = BitStrKey("1010")

var lengthPanicMsg = "BitStrKey: other key has different length"

func (k BitStrKey) BitLen() int {
	return len(k)
}

func (k BitStrKey) Bit(i int) uint {
	if i < 0 || i > len(k) {
		panic(key.BitPanicMsg)
	}
	if k[i] == '1' {
		return 1
	} else if k[i] == '0' {
		return 0
	}
	panic("BitStrKey: not a binary string")
}

func (k BitStrKey) Xor(o BitStrKey) BitStrKey {
	if len(k) != len(o) {
		if len(k) == 0 && o.isZero() {
			return BitStrKey(o)
		}
		if len(o) == 0 && k.isZero() {
			return BitStrKey(k)
		}
		panic(lengthPanicMsg)
	}
	buf := make([]byte, len(k))
	for i := range buf {
		if k[i] != o[i] {
			buf[i] = '1'
		} else {
			buf[i] = '0'
		}
	}
	return BitStrKey(string(buf))
}

func (k BitStrKey) CommonPrefixLength(o BitStrKey) int {
	if len(k) != len(o) {
		if len(k) == 0 && o.isZero() {
			return len(o)
		}
		if len(o) == 0 && k.isZero() {
			return len(k)
		}
		panic(lengthPanicMsg)
	}
	for i := 0; i < len(k); i++ {
		if k[i] != o[i] {
			return i
		}
	}
	return len(k)
}

func (k BitStrKey) Compare(o BitStrKey) int {
	if len(k) != len(o) {
		if len(k) == 0 && o.isZero() {
			return 0
		}
		if len(o) == 0 && k.isZero() {
			return 0
		}
		panic(lengthPanicMsg)
	}
	for i := 0; i < len(k); i++ {
		if k[i] != o[i] {
			if k[i] < o[i] {
				return -1
			}
			return 1
		}
	}
	return 0
}

func (k BitStrKey) isZero() bool {
	for i := 0; i < len(k); i++ {
		if k[i] != '0' {
			return false
		}
	}
	return true
}
