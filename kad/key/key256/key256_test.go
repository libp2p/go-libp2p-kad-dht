package key256

import (
	"testing"

	"github.com/libp2p/go-libdht/kad/key/test"
)

func TestKey256(t *testing.T) {
	tester := &test.KeyTester[Key256]{
		// kt.Key0 is 00000...000
		Key0: ZeroKey256(),

		// key1 is key0 + 1 (00000...001)
		Key1: NewKey256(append(make([]byte, 31), 0x01)),

		// key2 is key0 + 2 (00000...010)
		Key2: NewKey256(append(make([]byte, 31), 0x02)),

		// key1xor2 is key1 ^ key2 (00000...011)
		Key1xor2: NewKey256(append(make([]byte, 31), 0x03)),

		// key100 is key0 with the most significant bit set (10000...000)
		Key100: NewKey256(append([]byte{0x80}, make([]byte, 31)...)),

		// key010 is key0 with the second most significant bit set (01000...000)
		Key010: NewKey256(append([]byte{0x40}, make([]byte, 31)...)),

		KeyX: NewKey256(append([]byte{0x23, 0xe4, 0xdd, 0x03}, make([]byte, 28)...)),
	}

	tester.RunTests(t)

	test.TestBinaryMarshaler(t, tester.KeyX, NewKey256)
}
