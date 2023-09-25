// Package tiny implements Kademlia types suitable for tiny test networks
package tiny

import (
	"fmt"

	"github.com/plprobelab/go-kademlia/kad"
	"github.com/plprobelab/go-kademlia/key"
)

type Key = key.Key8

type Node struct {
	key Key
}

type Message struct {
	Content string
}

var _ kad.NodeID[Key] = Node{}

func NewNode(k Key) Node {
	return Node{key: k}
}

func (n Node) Key() Key {
	return n.key
}

func (n Node) Equal(other Node) bool {
	return n.key.Compare(other.key) == 0
}

func (n Node) String() string {
	return key.HexString(n.key)
}

// NodeWithCpl returns a [Node] that has a common prefix length of cpl with the supplied [Key]
func NodeWithCpl(k Key, cpl int) (Node, error) {
	if cpl > k.BitLen()-1 {
		return Node{}, fmt.Errorf("cpl too large")
	}

	// flip the bit after the cpl
	mask := Key(1 << (k.BitLen() - cpl - 1))
	return Node{key: k.Xor(mask)}, nil
}
