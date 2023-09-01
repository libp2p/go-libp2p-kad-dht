// Package kadt contains the kademlia types for interacting with go-kademlia.
// It would be nicer to have these types in the top-level DHT package; however,
// we also need these types in, e.g., the pb package to let the
// [pb.Message] type conform to certain interfaces.
package kadt

import (
	"crypto/sha256"

	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/plprobelab/go-kademlia/kad"
	"github.com/plprobelab/go-kademlia/key"
)

// PeerID is a type alias for peer.ID that implements the kad.NodeID interface.
// This means we can use PeerID for any operation that interfaces with
// go-kademlia.
type PeerID peer.ID

// assertion that PeerID implements the kad.NodeID interface
var _ kad.NodeID[key.Key256] = PeerID("")

// Key returns the Kademlia key of PeerID. The amino DHT operates on SHA256
// hashes of, in this case, peer.IDs. This means this Key method takes
// the peer.ID, hashes it and constructs a 256-bit key.
func (p PeerID) Key() key.Key256 {
	h := sha256.Sum256([]byte(p))
	return key.NewKey256(h[:])
}

// String calls String on the underlying peer.ID and returns a string like
// QmFoo or 12D3KooBar.
func (p PeerID) String() string {
	return peer.ID(p).String()
}

// AddrInfo is a type that wraps peer.AddrInfo and implements the kad.NodeInfo
// interface. This means we can use AddrInfo for any operation that interfaces
// with go-kademlia.
//
// A more accurate name would be PeerInfo or NodeInfo. However, for consistency
// and coherence with [peer.AddrInfo] we also name it AddrInfo.
type AddrInfo struct {
	Info peer.AddrInfo
}

// assertion that AddrInfo implements the kad.NodeInfo interface
var _ kad.NodeInfo[key.Key256, ma.Multiaddr] = (*AddrInfo)(nil)

// ID returns the kad.NodeID of this peer's information struct.
func (ai AddrInfo) ID() kad.NodeID[key.Key256] {
	return PeerID(ai.Info.ID)
}

// Addresses returns all Multiaddresses of this peer.
func (ai AddrInfo) Addresses() []ma.Multiaddr {
	addrs := make([]ma.Multiaddr, len(ai.Info.Addrs))
	copy(addrs, ai.Info.Addrs)
	return addrs
}
