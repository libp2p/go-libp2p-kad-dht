package dht_pb

import (
	"golang.org/x/exp/slog"

	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

// ContainsCloserPeer returns true if the provided peer ID is among the
// list of closer peers contained in this message.
func (m *Message) ContainsCloserPeer(pid peer.ID) bool {
	b := byteString(pid)
	for _, cp := range m.CloserPeers {
		if cp.Id.Equal(&b) {
			return true
		}
	}
	return false
}

// Addresses returns the Multiaddresses associated with the Message_Peer entry
func (m *Message_Peer) Addresses() []ma.Multiaddr {
	if m == nil {
		return nil
	}

	maddrs := make([]ma.Multiaddr, 0, len(m.Addrs))
	for _, addr := range m.Addrs {
		maddr, err := ma.NewMultiaddrBytes(addr)
		if err != nil {
			slog.Debug("error decoding multiaddr for peer", "peer", peer.ID(m.Id), "error", err)
			continue
		}

		maddrs = append(maddrs, maddr)
	}

	return maddrs
}
