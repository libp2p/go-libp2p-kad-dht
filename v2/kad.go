package dht

import (
	"crypto/sha256"

	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/plprobelab/go-kademlia/kad"
	"github.com/plprobelab/go-kademlia/key"
)

// nodeID is a type alias for peer.ID that implements the kad.NodeID interface.
// This means we can use nodeID for any operation that interfaces with
// go-kademlia.
type nodeID peer.ID

// assertion that nodeID implements the kad.NodeID interface
var _ kad.NodeID[key.Key256] = nodeID("")

// Key returns the Kademlia key of nodeID. The amino DHT operates on SHA256
// hashes of, in this case, peer.IDs. This means this Key method takes
// the peer.ID, hashes it and constructs a 256-bit key.
func (p nodeID) Key() key.Key256 {
	h := sha256.New()
	h.Write([]byte(p))
	return key.NewKey256(h.Sum(nil))
}

// String calls String on the underlying peer.ID and returns a string like
// QmFoo or 12D3KooBar.
func (p nodeID) String() string {
	return peer.ID(p).String()
}

// nodeInfo is a type that wraps peer.AddrInfo and implements the kad.NodeInfo
// interface. This means we can use nodeInfo for any operation that interfaces
// with go-kademlia.
type nodeInfo struct {
	info peer.AddrInfo
}

// assertion that nodeInfo implements the kad.NodeInfo interface
var _ kad.NodeInfo[key.Key256, ma.Multiaddr] = (*nodeInfo)(nil)

// ID returns the kad.NodeID of this peer's information struct.
func (ai nodeInfo) ID() kad.NodeID[key.Key256] {
	return nodeID(ai.info.ID)
}

// Addresses returns all Multiaddresses of this peer.
func (ai nodeInfo) Addresses() []ma.Multiaddr {
	addrs := make([]ma.Multiaddr, len(ai.info.Addrs))
	copy(addrs, ai.info.Addrs)
	return addrs
}
