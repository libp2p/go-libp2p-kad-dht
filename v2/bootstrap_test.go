package dht

import (
	"testing"

	"github.com/libp2p/go-libp2p/core/peer"
)

func TestDefaultBootstrapPeers(t *testing.T) {
	bps := DefaultBootstrapPeers()
	if len(bps) != len(defaultBootstrapPeers) {
		t.Errorf("len(DefaultBootstrapPeers()) = %d, want %v", len(bps), len(defaultBootstrapPeers))
	}

	bpmap := make(map[peer.ID]peer.AddrInfo)
	for _, info := range bps {
		bpmap[info.ID] = info
	}

	if len(bpmap) != len(defaultBootstrapPeers) {
		t.Errorf("unique DefaultBootstrapPeers() = %d, want %v", len(bpmap), len(defaultBootstrapPeers))
	}
}
