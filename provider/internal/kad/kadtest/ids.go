package kadtest

import (
	"crypto/sha256"

	"github.com/libp2p/go-libp2p-kad-dht/provider/internal/kad"
	"github.com/libp2p/go-libp2p-kad-dht/provider/internal/kad/key"
	"github.com/libp2p/go-libp2p-kad-dht/provider/internal/kad/key/bit256"
)

// ID is a concrete implementation of the NodeID interface.
type ID[K kad.Key[K]] struct {
	key K
}

// interface assertion. Using the concrete key type of key.Key8 does not
// limit the validity of the assertion for other key types.
var _ kad.NodeID[Key8] = (*ID[Key8])(nil)

// NewID returns a new Kademlia identifier that implements the NodeID interface.
// Instead of deriving the Kademlia key from a NodeID, this method directly takes
// the Kademlia key.
func NewID[K kad.Key[K]](k K) *ID[K] {
	return &ID[K]{key: k}
}

// Key returns the Kademlia key that is used by, e.g., the routing table
// implementation to group nodes into buckets. The returned key was manually
// defined in the ID constructor NewID and not derived via, e.g., hashing
// a preimage.
func (i ID[K]) Key() K {
	return i.key
}

func (i ID[K]) Equal(other K) bool {
	return i.key.Compare(other) == 0
}

func (i ID[K]) String() string {
	return key.HexString(i.key)
}

type StringID string

var _ kad.NodeID[bit256.Key] = (*StringID)(nil)

func NewStringID(s string) *StringID {
	return (*StringID)(&s)
}

func (s StringID) Key() bit256.Key {
	h := sha256.New()
	h.Write([]byte(s))
	return bit256.NewKey(h.Sum(nil))
}

func (s StringID) NodeID() kad.NodeID[bit256.Key] {
	return &s
}

func (s StringID) Equal(other string) bool {
	return string(s) == other
}

func (s StringID) String() string {
	return string(s)
}
