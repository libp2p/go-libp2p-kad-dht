package dht_pb

import (
	"testing"
)

func TestMessage_Peer_invalid_maddr(t *testing.T) {
	msg := Message_Peer{
		Addrs: [][]byte{[]byte("invalid-maddr")},
	}

	if len(msg.Addresses()) > 0 {
		t.Fatal("shouldn't have any multiaddrs")
	}
}
