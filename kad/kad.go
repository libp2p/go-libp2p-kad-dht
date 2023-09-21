package kad

import (
	"context"
)

// Key is the interface all Kademlia key types support.
//
// A Kademlia key is defined as a bit string of arbitrary size. In practice, different Kademlia implementations use
// different key sizes. For instance, the Kademlia paper (https://pdos.csail.mit.edu/~petar/papers/maymounkov-kademlia-lncs.pdf)
// defines keys as 160-bits long and IPFS uses 256-bit keys.
//
// Keys are usually generated using cryptographic hash functions, however the specifics of key generation
// do not matter for key operations.
//
// A Key is not necessarily used to identify a node in the network but a derived
// representation. Implementations may choose to hash a logical node identifier
// to derive a Kademlia Key. Therefore, there also exists the concept of a NodeID
// which just defines a method to return the associated Kademlia Key.
type Key[K any] interface {
	// BitLen returns the length of the key in bits.
	BitLen() int

	// Bit returns the value of the i'th bit of the key from most significant to least. It is equivalent to (key>>(bitlen-i-1))&1.
	// Bit will panic if i is out of the range [0,BitLen()-1].
	Bit(i int) uint

	// Xor returns the result of the eXclusive OR operation between the key and another key of the same type.
	Xor(other K) K

	// CommonPrefixLength returns the number of leading bits the key shares with another key of the same type.
	// The CommonPrefixLength of a key with itself is equal to BitLen.
	CommonPrefixLength(other K) int

	// Compare compares the numeric value of the key with another key of the same type.
	// It returns -1 if the key is numerically less than other, +1 if it is greater
	// and 0 if both keys are equal.
	Compare(other K) int
}

// RoutingTable is the interface all Kademlia Routing Tables types support.
type RoutingTable[K Key[K], N NodeID[K]] interface {
	// AddNode tries to add a peer to the routing table. It returns true if
	// the node was added and false if it wasn't added, e.g., because it
	// was already part of the routing table.
	//
	// Because NodeID[K]'s are often preimages to Kademlia keys K
	// there's no way to derive a NodeID[K] from just K. Therefore, to be
	// able to return NodeID[K]'s from the `NearestNodes` method, this
	// `AddNode` method signature takes a NodeID[K] instead of only K.
	//
	// Nodes added to the routing table are grouped into buckets based on their
	// XOR distance to the local node's identifier. The details of the XOR
	// arithmetics are defined on K.
	AddNode(N) bool

	// RemoveKey tries to remove a node identified by its Kademlia key from the
	// routing table.
	//
	// It returns true if the key existed in the routing table and was removed.
	// It returns false if the key didn't exist in the routing table and
	// therefore, was not removed.
	RemoveKey(K) bool

	// NearestNodes returns the given number of closest nodes to a given
	// Kademlia key that are currently present in the routing table.
	// The returned list of nodes will be ordered from closest to furthest and
	// contain at maximum the given number of entries, but also possibly less
	// if the number exceeds the number of nodes in the routing table.
	NearestNodes(K, int) []N

	// GetNode returns the node identified by the supplied Kademlia key or a zero
	// value if the node is not present in the routing table. The boolean second
	// return value indicates whether the node was found in the table.
	GetNode(K) (N, bool)
}

// NodeID is a generic node identifier and not equal to a Kademlia key. Some
// implementations use NodeID's as preimages for Kademlia keys. Kademlia keys
// are used for calculating distances between nodes while NodeID's are the
// original logical identifier of a node.
//
// The NodeID interface only defines a method that returns the Kademlia key
// for the given NodeID. E.g., the operation to go from a NodeID to a Kademlia key
// can be as simple as hashing the NodeID.
//
// Implementations may choose to equate NodeID's and Kademlia keys.
type NodeID[K Key[K]] interface {
	// Key returns the Kademlia key of the given NodeID. E.g., NodeID's can be
	// preimages to Kademlia keys, in which case, Key() could return the SHA256
	// of NodeID.
	Key() K

	// String returns a string reprensentation for this NodeID.
	// TODO: Try to get rid of this as it's also used for map keys which is not great.
	String() string
}

// NodeInfo is a container type that combines node identification information
// and network addresses at which the node is reachable.
type NodeInfo[K Key[K], A Address[A]] interface {
	// ID returns the node identifier.
	ID() NodeID[K]

	// Addresses returns the network addresses associated with the given node.
	Addresses() []A
}

// Address is an interface that any type must implement that can be used
// to address a node in the DHT network. This can be an IP/Port combination
// or in the case of libp2p a Multiaddress.
type Address[T any] interface {
	// Equal re
	Equal(T) bool
}

type Message interface{}

type Request[K Key[K], A Address[A]] interface {
	Message

	// Target returns the target key and true, or false if no target key has been specfied.
	Target() K

	// EmptyResponse returns an empty response struct for this request message
	// TODO: this is a weird patter, let's try to remove this.
	EmptyResponse() Response[K, A]
}

type Response[K Key[K], A Address[A]] interface {
	Message

	CloserNodes() []NodeInfo[K, A]
}

type RoutingProtocol[K Key[K], N NodeID[K], A Address[A]] interface {
	FindNode(ctx context.Context, to N, target K) (NodeInfo[K, A], []N, error)
	Ping(ctx context.Context, to N) error
}

type RecordProtocol[K Key[K], N NodeID[K]] interface {
	Get(ctx context.Context, to N, target K) ([]Record, []N, error)
	Put(ctx context.Context, to N, record Record) error
}

type Record any
