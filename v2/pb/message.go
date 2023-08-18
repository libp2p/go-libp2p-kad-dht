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
