package dht_pb

import (
	"bytes"
	"fmt"

	mh "github.com/multiformats/go-multihash"
	mhreg "github.com/multiformats/go-multihash/core"
	"github.com/plprobelab/go-kademlia/kad"
	"github.com/plprobelab/go-kademlia/key"
	"golang.org/x/exp/slog"

	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

// FromAddrInfo constructs a Message_Peer from the given peer.AddrInfo
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
	closerPeers := m.GetCloserPeers()
	if closerPeers == nil {
		return []kad.NodeInfo[key.Key256, ma.Multiaddr]{}
	}
	return ParsePeers(closerPeers)
}

func PBPeerToPeerInfo(pbp *Message_Peer) (*AddrInfo, error) {
	addrs := make([]ma.Multiaddr, 0, len(pbp.Addrs))
	for _, a := range pbp.Addrs {
		addr, err := ma.NewMultiaddrBytes(a)
		if err == nil {
			addrs = append(addrs, addr)
		}
	}
	if len(addrs) == 0 {
		return nil, fmt.Errorf("asdfsdf")
	}

	return NewAddrInfo(peer.AddrInfo{
		ID:    peer.ID(pbp.Id),
		Addrs: addrs,
	}), nil
}

func ParsePeers(pbps []*Message_Peer) []kad.NodeInfo[key.Key256, ma.Multiaddr] {
	peers := make([]kad.NodeInfo[key.Key256, ma.Multiaddr], 0, len(pbps))
	for _, p := range pbps {
		pi, err := PBPeerToPeerInfo(p)
		if err == nil {
			peers = append(peers, pi)
		}
	}
	return peers
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

type KadKey = key.Key256

type AddrInfo struct {
	peer.AddrInfo
	id *PeerID
}

var _ kad.NodeInfo[KadKey, ma.Multiaddr] = (*AddrInfo)(nil)

func NewAddrInfo(ai peer.AddrInfo) *AddrInfo {
	return &AddrInfo{
		AddrInfo: ai,
		id:       NewPeerID(ai.ID),
	}
}

func (ai AddrInfo) Key() KadKey {
	return ai.id.Key()
}

func (ai AddrInfo) String() string {
	return ai.id.String()
}

func (ai AddrInfo) PeerID() *PeerID {
	return ai.id
}

func (ai AddrInfo) ID() kad.NodeID[KadKey] {
	return ai.id
}

func (ai AddrInfo) Addresses() []ma.Multiaddr {
	addrs := make([]ma.Multiaddr, len(ai.Addrs))
	copy(addrs, ai.Addrs)
	return addrs
}

type PeerID struct {
	peer.ID
}

var _ kad.NodeID[KadKey] = (*PeerID)(nil)

func NewPeerID(p peer.ID) *PeerID {
	return &PeerID{p}
}

func (id PeerID) Key() KadKey {
	hasher, _ := mhreg.GetHasher(mh.SHA2_256)
	hasher.Write([]byte(id.ID))
	return key.NewKey256(hasher.Sum(nil))
}

func (id PeerID) NodeID() kad.NodeID[KadKey] {
	return &id
}

func (m *Message) Protocol() string {
	return "/test/1.0.0"
}

func (m *Message) Target() key.Key256 {
	return key.NewKey256(m.Key)
}

func (m *Message) EmptyResponse() kad.Response[key.Key256, ma.Multiaddr] {
	return &Message{}
}
