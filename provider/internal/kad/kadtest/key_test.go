package kadtest

import (
	"testing"

	"github.com/libp2p/go-libp2p-kad-dht/provider/internal/kad/key/test"
)

func TestKey32(t *testing.T) {
	tester := &test.KeyTester[Key32]{
		Key0:     Key32(0),
		Key1:     Key32(1),
		Key2:     Key32(2),
		Key1xor2: Key32(3),
		Key100:   Key32(0x80000000),
		Key010:   Key32(0x40000000),
		KeyX:     Key32(0x23e4dd03),
	}

	tester.RunTests(t)
}

func TestKey8(t *testing.T) {
	tester := &test.KeyTester[Key8]{
		Key0:     Key8(0),
		Key1:     Key8(1),
		Key2:     Key8(2),
		Key1xor2: Key8(3),
		Key100:   Key8(0x80),
		Key010:   Key8(0x40),
		KeyX:     Key8(0x23),
	}

	tester.RunTests(t)
}
