// Package kadt contains the kademlia types for interacting with go-kademlia.
// It would be nicer to have these types in the top-level DHT package; however,
// we also need these types in, e.g., the pb package to let the
// [pb.Message] type conform to certain interfaces.
package kadt

import (
	"crypto/sha256"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/plprobelab/go-kademlia/kad"
	"github.com/plprobelab/go-kademlia/key"
)

// Key is a type alias for the type of key that's used with this DHT
// implementation.
type Key = key.Key256

var _ kad.Key[key.Key256] = Key(key.Key256{})

// PeerID is a type alias for [peer.ID] that implements the [kad.NodeID]
// interface. This means we can use PeerID for any operation that interfaces
// with go-kademlia.
type PeerID peer.ID

// assertion that PeerID implements the kad.NodeID interface
var _ kad.NodeID[Key] = PeerID("")

func (p PeerID) ID() peer.ID {
	return peer.ID(p)
}

// Key returns the Kademlia [key.Key256] of PeerID. The amino DHT operates on
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
