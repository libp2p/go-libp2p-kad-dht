package dht_pb

import (
	"github.com/libp2p/go-libp2p/core/peer"
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

//type PeerRoutingInfo struct {
//	peer.AddrInfo
//	network.Connectedness
//}

//func peerRoutingInfoToPBPeer(p PeerRoutingInfo) Message_Peer {
//	var pbp Message_Peer
//
//	pbp.Addrs = make([][]byte, len(p.Addrs))
//	for i, maddr := range p.Addrs {
//		pbp.Addrs[i] = maddr.Bytes() // Bytes, not String. Compressed.
//	}
//	pbp.Id = byteString(p.ID)
//	pbp.Connection = ConnectionType(p.Connectedness)
//	return pbp
//}

//// PBPeerToPeer turns a *Message_Peer into its peer.AddrInfo counterpart
//func PBPeerToPeerInfo(pbp Message_Peer) peer.AddrInfo {
//	return peer.AddrInfo{
//		ID:    peer.ID(pbp.Id),
//		Addrs: pbp.Addresses(),
//	}
//}
//
//// RawPeerInfosToPBPeers converts a slice of Peers into a slice of *Message_Peers,
//// ready to go out on the wire.
//func RawPeerInfosToPBPeers(peers []peer.AddrInfo) []Message_Peer {
//	pbpeers := make([]Message_Peer, len(peers))
//	for i, p := range peers {
//		pbpeers[i] = peerInfoToPBPeer(p)
//	}
//	return pbpeers
//}
//
//// PeersToPBPeers converts given []peer.Peer into a set of []*Message_Peer,
//// which can be written to a message and sent out. the key thing this function
//// does (in addition to PeersToPBPeers) is set the ConnectionType with
//// information from the given network.Network.
//func PeerInfosToPBPeers(n network.Network, peers []peer.AddrInfo) []Message_Peer {
//	pbps := RawPeerInfosToPBPeers(peers)
//	for i, pbp := range pbps {
//		c := ConnectionType(n.Connectedness(peers[i].ID))
//		pbp.Connection = c
//	}
//	return pbps
//}
//
//func PeerRoutingInfosToPBPeers(peers []PeerRoutingInfo) []Message_Peer {
//	pbpeers := make([]Message_Peer, len(peers))
//	for i, p := range peers {
//		pbpeers[i] = peerRoutingInfoToPBPeer(p)
//	}
//	return pbpeers
//}
//
//// PBPeersToPeerInfos converts given []*Message_Peer into []peer.AddrInfo
//// Invalid addresses will be silently omitted.
//func PBPeersToPeerInfos(pbps []Message_Peer) []*peer.AddrInfo {
//	peers := make([]*peer.AddrInfo, 0, len(pbps))
//	for _, pbp := range pbps {
//		ai := PBPeerToPeerInfo(pbp)
//		peers = append(peers, &ai)
//	}
//	return peers
//}

//// fromConnectedness returns a Message_ConnectionType associated with the
//// network.Connectedness.
//func fromConnectedness(c network.Connectedness) Message_ConnectionType {
//	switch c {
//	case network.NotConnected:
//		return Message_NOT_CONNECTED
//	case network.Connected:
//		return Message_CONNECTED
//	case network.CanConnect:
//		return Message_CAN_CONNECT
//	case network.CannotConnect:
//		return Message_CANNOT_CONNECT
//	default:
//		return Message_NOT_CONNECTED
//	}
//}
//
//// Connectedness returns a network.Connectedness associated with the given
//// Message_ConnectionType.
//func Connectedness(c Message_ConnectionType) network.Connectedness {
//	switch c {
//	case Message_NOT_CONNECTED:
//		return network.NotConnected
//	case Message_CONNECTED:
//		return network.Connected
//	case Message_CAN_CONNECT:
//		return network.CanConnect
//	case Message_CANNOT_CONNECT:
//		return network.CannotConnect
//	default:
//		return network.NotConnected
//	}
//}
