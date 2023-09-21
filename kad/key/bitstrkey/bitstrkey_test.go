package bitstrkey

import (
	"testing"

	"github.com/libp2p/go-libdht/kad/key/test"
)

// TestBitStrKey7 tests a strange 7-bit Kademlia key
func TestBitStrKey7(t *testing.T) {
	tester := &test.KeyTester[BitStrKey]{
		Key0:     BitStrKey("0000000"),
		Key1:     BitStrKey("0000001"),
		Key2:     BitStrKey("0000010"),
		Key1xor2: BitStrKey("0000011"),
		Key100:   BitStrKey("1000000"),
		Key010:   BitStrKey("0100000"),
		KeyX:     BitStrKey("1010110"),
	}

	tester.RunTests(t)
}
