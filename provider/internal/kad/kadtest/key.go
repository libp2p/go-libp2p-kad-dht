package kadtest

import (
	"fmt"

	"github.com/probe-lab/go-libdht/kad"
)

const bitPanicMsg = "bit index out of range"

// Key32 is a 32-bit Kademlia key, suitable for testing and simulation of small networks.
type Key32 uint32

var _ kad.Key[Key32] = Key32(0)

// BitLen returns the length of the key in bits, which is always 32.
func (Key32) BitLen() int {
	return 32
}

// Bit returns the value of the i'th bit of the key from most significant to least.
func (k Key32) Bit(i int) uint {
	if i < 0 || i > 31 {
		panic(bitPanicMsg)
	}
	return uint((k >> (31 - i)) & 1)
}

// Xor returns the result of the eXclusive OR operation between the key and another key of the same type.
func (k Key32) Xor(o Key32) Key32 {
	return k ^ o
}

// CommonPrefixLength returns the number of leading bits the key shares with another key of the same type.
func (k Key32) CommonPrefixLength(o Key32) int {
	a := uint32(k)
	b := uint32(o)
	for i := 32; i > 0; i-- {
		if a == b {
			return i
		}
		a >>= 1
		b >>= 1
	}
	return 0
}

// Compare compares the numeric value of the key with another key of the same type.
func (k Key32) Compare(o Key32) int {
	if k < o {
		return -1
	} else if k > o {
		return 1
	}
	return 0
}

// HexString returns a string containing the hexadecimal representation of the key.
func (k Key32) HexString() string {
	return fmt.Sprintf("%04x", uint32(k))
}

// BitString returns a string containing the binary representation of the key.
func (k Key32) BitString() string {
	return fmt.Sprintf("%032b", uint32(k))
}

func (k Key32) String() string {
	return k.HexString()
}

// Key8 is an 8-bit Kademlia key, suitable for testing and simulation of very small networks.
type Key8 uint8

var _ kad.Key[Key8] = Key8(0)

// BitLen returns the length of the key in bits, which is always 8.
func (Key8) BitLen() int {
	return 8
}

// Bit returns the value of the i'th bit of the key from most significant to least.
func (k Key8) Bit(i int) uint {
	if i < 0 || i > 7 {
		panic(bitPanicMsg)
	}
	return uint((k >> (7 - i)) & 1)
}

// Xor returns the result of the eXclusive OR operation between the key and another key of the same type.
func (k Key8) Xor(o Key8) Key8 {
	return k ^ o
}

// CommonPrefixLength returns the number of leading bits the key shares with another key of the same type.
func (k Key8) CommonPrefixLength(o Key8) int {
	a := uint8(k)
	b := uint8(o)
	for i := 8; i > 0; i-- {
		if a == b {
			return i
		}
		a >>= 1
		b >>= 1
	}
	return 0
}

// Compare compares the numeric value of the key with another key of the same type.
func (k Key8) Compare(o Key8) int {
	if k < o {
		return -1
	} else if k > o {
		return 1
	}
	return 0
}

// HexString returns a string containing the hexadecimal representation of the key.
func (k Key8) HexString() string {
	return fmt.Sprintf("%x", uint8(k))
}

func (k Key8) String() string {
	return k.HexString()
}

// HexString returns a string containing the binary representation of the key.
func (k Key8) BitString() string {
	return fmt.Sprintf("%08b", uint8(k))
}
