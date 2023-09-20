package brdcst

import (
	"github.com/plprobelab/go-kademlia/kad"

	"github.com/libp2p/go-libp2p-kad-dht/v2/internal/coord/query"
)

// PoolState is the state of a bootstrap.
type PoolState interface {
	poolState()

	IsTerminal() bool
}

type StatePoolFindCloser[K kad.Key[K], N kad.NodeID[K]] struct {
	QueryID query.QueryID
	Target  K // the key that the query wants to find closer nodes for
	NodeID  N // the node to send the message to
	Stats   query.QueryStats
}

type StatePoolStoreRecord[K kad.Key[K], N kad.NodeID[K]] struct {
	QueryID query.QueryID
	NodeID  N
}

type StatePoolFinished struct {
	QueryID query.QueryID
	Stats   query.QueryStats
}

type StatePoolIdle struct{}

func (*StatePoolFindCloser[K, N]) poolState()  {}
func (*StatePoolStoreRecord[K, N]) poolState() {}
func (*StatePoolFinished) poolState()          {}
func (*StatePoolIdle) poolState()              {}

func (s *StatePoolFindCloser[K, N]) IsTerminal() bool  { return s != nil }
func (s *StatePoolStoreRecord[K, N]) IsTerminal() bool { return s != nil }
func (s *StatePoolFinished) IsTerminal() bool          { return s != nil }
func (s *StatePoolIdle) IsTerminal() bool              { return s != nil }
