package coord

import (
	"context"
	"errors"
	"time"

	"github.com/libp2p/go-libp2p-kad-dht/v2/kadt"

	"github.com/libp2p/go-libp2p-kad-dht/v2/pb"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/plprobelab/go-kademlia/network/address"
)

// Value is a value that may be stored in the DHT.
type Value interface {
	Key() kadt.Key
	MarshalBinary() ([]byte, error)
}

// Node represents the local or a remote node participating in the DHT.
type Node interface {
	// ID returns the peer ID identifying this node.
	ID() peer.ID

	// GetClosestNodes requests the n closest nodes to the key from the node's
	// local routing table. The node may return fewer nodes than requested.
	GetClosestNodes(ctx context.Context, key kadt.Key, n int) ([]Node, error)

	// GetValue requests that the node return any value associated with the
	// supplied key. If the node does not have a value for the key it returns
	// ErrValueNotFound.
	GetValue(ctx context.Context, key kadt.Key) (Value, error)

	// PutValue requests that the node stores a value to be associated with the supplied key.
	// If the node cannot or chooses not to store the value for the key it returns ErrValueNotAccepted.
	PutValue(ctx context.Context, r Value, q int) error
}

var (
	ErrNodeNotFound     = errors.New("node not found")
	ErrValueNotFound    = errors.New("value not found")
	ErrValueNotAccepted = errors.New("value not accepted")
)

// QueryFunc is the type of the function called by Query to visit each node.
//
// The error result returned by the function controls how Query proceeds. If the function returns the special value
// SkipNode, Query skips fetching closer nodes from the current node. If the function returns the special value
// SkipRemaining, Query skips all visiting all remaining nodes. Otherwise, if the function returns a non-nil error,
// Query stops entirely and returns that error.
//
// The stats argument contains statistics on the progress of the query so far.
type QueryFunc func(ctx context.Context, node Node, stats QueryStats) error

type QueryStats struct {
	Start     time.Time // Start is the time the query began executing.
	End       time.Time // End is the time the query stopped executing.
	Requests  int       // Requests is a count of the number of requests made by the query.
	Success   int       // Success is a count of the number of nodes the query succesfully contacted.
	Failure   int       // Failure is a count of the number of nodes the query received an error response from.
	Exhausted bool      // Exhausted is true if the query ended after visiting every node it could.
}

var (
	// ErrSkipNode is used as a return value from a QueryFunc to indicate that the node is to be skipped.
	ErrSkipNode = errors.New("skip node")

	// ErrSkipRemaining is used as a return value a QueryFunc to indicate that all remaining nodes are to be skipped.
	ErrSkipRemaining = errors.New("skip remaining nodes")
)

// Router its a work in progress
// TODO figure out the role of protocol identifiers
type Router interface {
	// SendMessage attempts to send a request to another node. The Router will absorb the addresses in to into its
	// internal nodestore. This method blocks until a response is received or an error is encountered.
	SendMessage(ctx context.Context, to peer.ID, protoID address.ProtocolID, req *pb.Message) (*pb.Message, error)

	// GetClosestNodes attempts to send a request to another node asking it for nodes that it considers to be
	// closest to the target key.
	GetClosestNodes(ctx context.Context, to peer.ID, target kadt.Key) ([]peer.AddrInfo, error)
}
