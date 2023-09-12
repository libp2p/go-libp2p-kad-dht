package query

import (
	"context"
	"testing"

	"github.com/plprobelab/go-kademlia/key"
	"github.com/stretchr/testify/require"

	"github.com/libp2p/go-libp2p-kad-dht/v2/coord/internal/tiny"
)

var (
	_ NodeIter[tiny.Key, tiny.Node] = (*ClosestNodesIter[tiny.Key, tiny.Node])(nil)
	_ NodeIter[tiny.Key, tiny.Node] = (*SequentialIter[tiny.Key, tiny.Node])(nil)
)

func TestClosestNodesIter(t *testing.T) {
	target := tiny.Key(0b00000001)
	a := tiny.NewNode(tiny.Key(0b00000100)) // 4
	b := tiny.NewNode(tiny.Key(0b00001000)) // 8
	c := tiny.NewNode(tiny.Key(0b00010000)) // 16
	d := tiny.NewNode(tiny.Key(0b00100000)) // 32

	// ensure the order of the known nodes
	require.True(t, target.Xor(a.Key()).Compare(target.Xor(b.Key())) == -1)
	require.True(t, target.Xor(b.Key()).Compare(target.Xor(c.Key())) == -1)
	require.True(t, target.Xor(c.Key()).Compare(target.Xor(d.Key())) == -1)

	iter := NewClosestNodesIter[tiny.Key, tiny.Node](target)

	// add nodes in "random order"

	iter.Add(&NodeStatus[tiny.Key, tiny.Node]{NodeID: b})
	iter.Add(&NodeStatus[tiny.Key, tiny.Node]{NodeID: d})
	iter.Add(&NodeStatus[tiny.Key, tiny.Node]{NodeID: a})
	iter.Add(&NodeStatus[tiny.Key, tiny.Node]{NodeID: c})

	// Each should iterate in order of distance from target

	distances := make([]tiny.Key, 0, 4)
	iter.Each(context.Background(), func(ctx context.Context, ns *NodeStatus[tiny.Key, tiny.Node]) bool {
		distances = append(distances, target.Xor(ns.NodeID.Key()))
		return false
	})

	require.True(t, key.IsSorted(distances))
}

func TestSequentialIter(t *testing.T) {
	a := tiny.NewNode(tiny.Key(0b00000100)) // 4
	b := tiny.NewNode(tiny.Key(0b00001000)) // 8
	c := tiny.NewNode(tiny.Key(0b00010000)) // 16
	d := tiny.NewNode(tiny.Key(0b00100000)) // 32

	iter := NewSequentialIter[tiny.Key, tiny.Node]()

	// add nodes in "random order"

	iter.Add(&NodeStatus[tiny.Key, tiny.Node]{NodeID: b})
	iter.Add(&NodeStatus[tiny.Key, tiny.Node]{NodeID: d})
	iter.Add(&NodeStatus[tiny.Key, tiny.Node]{NodeID: a})
	iter.Add(&NodeStatus[tiny.Key, tiny.Node]{NodeID: c})

	// Each should iterate in order the nodes were added to the iiterator

	order := make([]tiny.Key, 0, 4)
	iter.Each(context.Background(), func(ctx context.Context, ns *NodeStatus[tiny.Key, tiny.Node]) bool {
		order = append(order, ns.NodeID.Key())
		return false
	})

	require.Equal(t, 4, len(order))
	require.True(t, key.Equal(order[0], b.Key()))
	require.True(t, key.Equal(order[1], d.Key()))
	require.True(t, key.Equal(order[2], a.Key()))
	require.True(t, key.Equal(order[3], c.Key()))
}
