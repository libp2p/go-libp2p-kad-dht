package coord

import (
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/plprobelab/go-kademlia/kad"

	"github.com/libp2p/go-libp2p-kad-dht/v2/kadt"
)

// KadPeerIDToAddrInfo converts a kad.NodeID to a peer.AddrInfo with no addresses.
// This function will panic if id's underlying type is not kadt.PeerID
func KadPeerIDToAddrInfo(id kad.NodeID[kadt.Key]) peer.AddrInfo {
	peerID := id.(kadt.PeerID)
	return peer.AddrInfo{
		ID: peer.ID(peerID),
	}
}

// AddrInfoToKadPeerID converts a peer.AddrInfo to a kad.NodeID.
func AddrInfoToKadPeerID(ai peer.AddrInfo) kadt.PeerID {
	return kadt.PeerID(ai.ID)
}

// SliceOfPeerIDToSliceOfKadPeerID converts a slice of peer.ID to a slice of kadt.PeerID
func SliceOfPeerIDToSliceOfKadPeerID(peers []peer.ID) []kadt.PeerID {
	nodes := make([]kadt.PeerID, len(peers))
	for i := range peers {
		nodes[i] = kadt.PeerID(peers[i])
	}
	return nodes
}

func SliceOfAddrInfoToSliceOfKadPeerID(ais []peer.AddrInfo) []kadt.PeerID {
	nodes := make([]kadt.PeerID, len(ais))
	for i := range ais {
		nodes[i] = kadt.PeerID(ais[i].ID)
	}
	return nodes
}
