package coord

import (
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/plprobelab/go-kademlia/kad"

	"github.com/libp2p/go-libp2p-kad-dht/v2/kadt"
)

// kadPeerIDToAddrInfo converts a kad.NodeID to a peer.AddrInfo with no addresses.
// This function will panic if id's underlying type is not kadt.PeerID
func kadPeerIDToAddrInfo(id kad.NodeID[kadt.Key]) peer.AddrInfo {
	peerID := id.(kadt.PeerID)
	return peer.AddrInfo{
		ID: peer.ID(peerID),
	}
}

// addrInfoToKadPeerID converts a peer.AddrInfo to a kad.NodeID.
func addrInfoToKadPeerID(addrInfo peer.AddrInfo) kadt.PeerID {
	return kadt.PeerID(addrInfo.ID)
}

// sliceOfPeerIDToSliceOfKadPeerID converts a slice of peer.ID to a slice of kadt.PeerID
func sliceOfPeerIDToSliceOfKadPeerID(peers []peer.ID) []kadt.PeerID {
	nodes := make([]kadt.PeerID, len(peers))
	for i := range peers {
		nodes[i] = kadt.PeerID(peers[i])
	}
	return nodes
}

func sliceOfAddrInfoToSliceOfKadPeerID(addrInfos []peer.AddrInfo) []kadt.PeerID {
	peers := make([]kadt.PeerID, len(addrInfos))
	for i := range addrInfos {
		peers[i] = kadt.PeerID(addrInfos[i].ID)
	}
	return peers
}
