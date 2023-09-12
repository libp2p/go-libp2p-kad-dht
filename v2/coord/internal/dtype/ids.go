// dtype provides simple types that implement the Key and NodeID types in go-libdht/go-kademlia
package dtype

import (
	// "crypto/sha256"
	"net"

	"github.com/plprobelab/go-kademlia/kad"
	"github.com/plprobelab/go-kademlia/key"
)

// ID is a concrete implementation of the NodeID interface.
type ID[K kad.Key[K]] struct {
	key K
}

// interface assertion. Using the concrete key type of key.Key8 does not
// limit the validity of the assertion for other key types.
var _ kad.NodeID[key.Key8] = (*ID[key.Key8])(nil)

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

type Info[K kad.Key[K], A kad.Address[A]] struct {
	id    *ID[K]
	addrs []A
}

var _ kad.NodeInfo[key.Key8, net.IP] = (*Info[key.Key8, net.IP])(nil)

func NewInfo[K kad.Key[K], A kad.Address[A]](id *ID[K], addrs []A) *Info[K, A] {
	return &Info[K, A]{
		id:    id,
		addrs: addrs,
	}
}

func (a *Info[K, A]) AddAddr(addr A) {
	a.addrs = append(a.addrs, addr)
}

func (a *Info[K, A]) RemoveAddr(addr A) {
	writeIndex := 0
	// remove all occurrences of addr
	for _, ad := range a.addrs {
		if !ad.Equal(addr) {
			a.addrs[writeIndex] = ad
			writeIndex++
		}
	}
	a.addrs = a.addrs[:writeIndex]
}

func (a *Info[K, A]) ID() kad.NodeID[K] {
	return a.id
}

func (a *Info[K, A]) Addresses() []A {
	addresses := make([]A, len(a.addrs))
	copy(addresses, a.addrs)
	return addresses
}
