package triert

import (
	"fmt"
	"testing"

	"github.com/libp2p/go-libdht/kad/internal/test"
	"github.com/stretchr/testify/require"
)

func TestBucketLimit20(t *testing.T) {
	cfg := DefaultConfig[test.Key32, node[test.Key32]]()
	cfg.KeyFilter = BucketLimit20[test.Key32, node[test.Key32]]
	rt, err := New(node0, cfg)
	require.NoError(t, err)

	nodes := make([]node[test.Key32], 21)
	for i := range nodes {
		kk := test.RandomKeyWithPrefix("000100")
		nodes[i] = newNode(fmt.Sprintf("QmPeer%d", i), kk)
	}

	// Add 20 peers with cpl 3
	for i := 0; i < 20; i++ {
		success := rt.AddNode(nodes[i])
		require.True(t, success)
	}

	// cannot add 21st
	success := rt.AddNode(nodes[20])
	require.False(t, success)

	// add peer with different cpl
	kk := test.RandomKeyWithPrefix("0000100")
	node22 := newNode("QmPeer22", kk)
	success = rt.AddNode(node22)
	require.True(t, success)

	// make space for another cpl 3 key
	success = rt.RemoveKey(nodes[0].Key())
	require.True(t, success)

	// now can add cpl 3 key
	success = rt.AddNode(nodes[20])
	require.True(t, success)
}
