// Package kadt contains the kademlia types for interacting with go-kademlia.
package kadt

import (
	"crypto/sha256"

	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/plprobelab/go-kademlia/kad"
	"github.com/plprobelab/go-kademlia/key"
)

// Key is a type alias for the type of key that's used with this DHT
// implementation.
type Key = key.Key256

// PeerID is a type alias for [peer.ID] that implements the [kad.NodeID]
// interface. This means we can use PeerID for any operation that interfaces
// with go-kademlia.
type PeerID peer.ID

// assertion that PeerID implements the kad.NodeID interface
var _ kad.NodeID[Key] = PeerID("")

// Key returns the Kademlia [KadKey] of PeerID. The amino DHT operates on
// SHA256 hashes of, in this case, peer.IDs. This means this Key method takes
// the [peer.ID], hashes it and constructs a 256-bit key.
func (p PeerID) Key() Key {
	h := sha256.Sum256([]byte(p))
	return key.NewKey256(h[:])
}

// String calls String on the underlying [peer.ID] and returns a string like
// QmFoo or 12D3KooBar.
func (p PeerID) String() string {
	return peer.ID(p).String()
}

// Equal compares the [PeerID] with another by comparing the underlying [peer.ID].
func (p PeerID) Equal(o PeerID) bool {
	return peer.ID(p) == peer.ID(o)
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

// assertion that AddrInfo implements the [kad.NodeInfo] interface
var _ kad.NodeInfo[Key, ma.Multiaddr] = (*AddrInfo)(nil)

// ID returns the [kad.NodeID] of this peer's information struct.
func (ai AddrInfo) ID() kad.NodeID[Key] {
	return PeerID(ai.Info.ID)
}

// PeerID returns the peer ID of this peer's information struct as a PeerID.
func (ai AddrInfo) PeerID() PeerID {
	return PeerID(ai.Info.ID)
}

// Addresses returns all Multiaddresses of this peer.
func (ai AddrInfo) Addresses() []ma.Multiaddr {
	addrs := make([]ma.Multiaddr, len(ai.Info.Addrs))
	copy(addrs, ai.Info.Addrs)
	return addrs
}

// RoutingTable is a mapping between [Key] and [PeerID] and provides methods to interact with the mapping
// and find PeerIDs close to a particular Key.
type RoutingTable interface {
	kad.RoutingTable[Key, PeerID]

	// Cpl returns the longest common prefix length the supplied key shares with the table's key.
	Cpl(kk Key) int

	// CplSize returns the number of nodes in the table whose longest common prefix with the table's key is of length cpl.
	CplSize(cpl int) int
}
