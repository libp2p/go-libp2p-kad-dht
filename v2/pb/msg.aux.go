package pb

import (
	"bytes"
	"crypto/sha256"

	"github.com/libp2p/go-libp2p-kad-dht/v2/kadt"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/plprobelab/go-kademlia/kad"
	"github.com/plprobelab/go-kademlia/key"
	"golang.org/x/exp/slog"
)

// this file contains auxiliary methods to augment the protobuf generated types.
// It is used to let these types conform to interfaces or add convenience methods.

var _ kad.Request[key.Key256, ma.Multiaddr] = (*Message)(nil)

func (m *Message) Target() key.Key256 {
	b := sha256.Sum256(m.Key)
	return key.NewKey256(b[:])
}

func (m *Message) EmptyResponse() kad.Response[key.Key256, ma.Multiaddr] {
	return &Message{
		Type: m.Type,
		Key:  m.Key,
	}
}

// FromAddrInfo constructs a [Message_Peer] from the given [peer.AddrInfo].
func FromAddrInfo(p peer.AddrInfo) *Message_Peer {
	mp := &Message_Peer{
		Id:    []byte(p.ID),
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
	for _, cp := range m.CloserPeers {
		if bytes.Equal(cp.Id, []byte(pid)) {
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

// CloserPeersAddrInfos returns the peer.AddrInfo's of the closer peers in this
// message.
func (m *Message) CloserPeersAddrInfos() []peer.AddrInfo {
	if m == nil {
		return nil
	}

	addrInfos := make([]peer.AddrInfo, 0, len(m.CloserPeers))
	for _, p := range m.CloserPeers {
		addrInfos = append(addrInfos, peer.AddrInfo{
			ID:    peer.ID(p.Id),
			Addrs: p.Addresses(),
		})
	}

	return addrInfos
}

func (m *Message) CloserNodes() []kad.NodeInfo[key.Key256, ma.Multiaddr] {
	if m == nil {
		return nil
	}

	infos := make([]kad.NodeInfo[key.Key256, ma.Multiaddr], 0, len(m.CloserPeers))
	for _, p := range m.CloserPeers {
		infos = append(infos, &kadt.AddrInfo{Info: peer.AddrInfo{
			ID:    peer.ID(p.Id),
			Addrs: p.Addresses(),
		}})
	}

	return infos
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
