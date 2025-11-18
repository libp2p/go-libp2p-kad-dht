package bitstr

import (
	"testing"

	"github.com/libp2p/go-libp2p-kad-dht/provider/internal/kad/key/test"
)

// TestBitStrKey7 tests a strange 7-bit Kademlia key
func TestBitStrKey7(t *testing.T) {
	tester := &test.KeyTester[Key]{
		Key0:     Key("0000000"),
		Key1:     Key("0000001"),
		Key2:     Key("0000010"),
		Key1xor2: Key("0000011"),
		Key100:   Key("1000000"),
		Key010:   Key("0100000"),
		KeyX:     Key("1010110"),
	}

	tester.RunTests(t)
}
