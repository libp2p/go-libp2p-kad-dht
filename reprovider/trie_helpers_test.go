package reprovider

import (
	"fmt"
	"testing"

	"github.com/probe-lab/go-libdht/kad/key/bitstr"
	"github.com/probe-lab/go-libdht/kad/trie"
	"github.com/stretchr/testify/require"
)

func TestNextNonEmptyLeafFullTrie(t *testing.T) {
	bitlen := 4

	tr := trie.New[bitstr.Key, any]()
	nKeys := 1 << bitlen
	binaryKeys := make([]bitstr.Key, 0, nKeys)
	for i := range nKeys {
		binary := fmt.Sprintf("%0*b", bitlen, i)
		k := bitstr.Key(binary)
		tr.Add(k, struct{}{})
		binaryKeys = append(binaryKeys, k)
	}

	order := binaryKeys[0]
	t.Run("OrderZero", func(t *testing.T) {
		for i, k := range binaryKeys {
			nextKey := nextNonEmptyLeaf(tr, k, order).Key
			require.Equal(t, binaryKeys[(i+1)%nKeys], nextKey)
		}
	})

	t.Run("Cycle", func(t *testing.T) {
		initialKey := binaryKeys[0]
		k := initialKey
		for range binaryKeys {
			k = nextNonEmptyLeaf(tr, k, order).Key
		}
		require.Equal(t, initialKey, k)
	})

	order = binaryKeys[nKeys-1]
	t.Run("CustomOrder", func(t *testing.T) {
		for i, k := range binaryKeys {
			nextKey := nextNonEmptyLeaf(tr, k, order).Key
			require.Equal(t, binaryKeys[(i-1+nKeys)%nKeys], nextKey)
		}
	})
}

func TestNextNonEmptyLeafSparseTrie(t *testing.T) {
	bitlen := 10
	sparsity := 4

	tr := trie.New[bitstr.Key, any]()
	nKeys := 1 << (bitlen - sparsity)
	binaryKeys := make([]bitstr.Key, 0, nKeys)
	suffix := (1 << sparsity) - 1
	for i := range nKeys {
		binary := fmt.Sprintf("%0*b", bitlen, i*(1<<sparsity)+suffix)
		k := bitstr.Key(binary)
		tr.Add(k, struct{}{})
		binaryKeys = append(binaryKeys, k)
	}

	order := bitstr.Key(fmt.Sprintf("%0*b", bitlen, 0))
	t.Run("OrderZero", func(t *testing.T) {
		for i, k := range binaryKeys {
			nextKey := nextNonEmptyLeaf(tr, k, order).Key
			require.Equal(t, binaryKeys[(i+1)%nKeys], nextKey)
		}
	})

	t.Run("MissingKey", func(t *testing.T) {
		for i := range 1 << bitlen {
			binary := fmt.Sprintf("%0*b", bitlen, i)
			k := bitstr.Key(binary)
			nextKey := nextNonEmptyLeaf(tr, k, order).Key
			require.Equal(t, binaryKeys[((i+1)%(1<<bitlen))/(1<<sparsity)], nextKey, k)
		}
	})

	order = binaryKeys[nKeys-1]
	t.Run("CustomOrder", func(t *testing.T) {
		for i, k := range binaryKeys {
			nextKey := nextNonEmptyLeaf(tr, k, order).Key
			require.Equal(t, binaryKeys[(i-1+nKeys)%nKeys], nextKey)
		}
	})
}
