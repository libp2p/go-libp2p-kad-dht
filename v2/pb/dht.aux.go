package dht_pb

import (
	"golang.org/x/exp/slog"

	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

// FromAddrInfo constructs a Message_Peer from the given peer.AddrInfo
func FromAddrInfo(p peer.AddrInfo) Message_Peer {
	mp := Message_Peer{
		Id:    byteString(p.ID),
		Addrs: make([][]byte, len(p.Addrs)),
	}

	for i, maddr := range p.Addrs {
		mp.Addrs[i] = maddr.Bytes() // Bytes, not String. Compressed.
	}

	return mp
}

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

// ProviderAddrInfos returns the peer.AddrInfo's of the provider peers in this
// message.
func (m *Message) ProviderAddrInfos() []peer.AddrInfo {
	if m == nil {
		return nil
	}

	addrInfos := make([]peer.AddrInfo, 0, len(m.ProviderPeers))
	for _, p := range m.ProviderPeers {
		addrInfos = append(addrInfos, peer.AddrInfo{
			ID:    peer.ID(p.Id),
			Addrs: p.Addresses(),
		})
	}

	return addrInfos
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
			slog.Debug("error decoding multiaddr for peer", "peer", peer.ID(m.Id), "err", err)
			continue
		}

		maddrs = append(maddrs, maddr)
	}

	return maddrs
}
