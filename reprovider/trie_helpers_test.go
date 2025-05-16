package reprovider

import (
	"crypto/rand"
	"fmt"
	"sort"
	"testing"

	"github.com/probe-lab/go-libdht/kad/key"
	"github.com/probe-lab/go-libdht/kad/key/bit256"
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

func TestNextNonEmptyLeafRandom(t *testing.T) {
	order := bit256.ZeroKey()
	nKeys := 256
	tr := trie.New[bit256.Key, struct{}]()
	keys := make([]bit256.Key, 0, nKeys)

	var b [32]byte
	for range nKeys {
		if _, err := rand.Read(b[:]); err != nil {
			require.NoError(t, err)
		}
		k := bit256.NewKey(b[:])
		tr.Add(k, struct{}{})
		keys = append(keys, k)

		// Sort keys
		sort.Slice(keys, func(i, j int) bool {
			return keys[i].Compare(keys[j]) < 0
		})

		currentKey := bit256.ZeroKey()
		for j, k := range keys {
			currentKey = nextNonEmptyLeaf(tr, currentKey, order).Key
			require.Equal(t, k, currentKey, "failed at index %d\nExp: %s\nGot: %s", j, key.BitString(k), key.BitString(currentKey))
		}
	}
}

func TestSimpleNextNonEmptyLeaf(t *testing.T) {
	keys := []bitstr.Key{"00", "01", "10", "11"}
	tr := trie.New[bitstr.Key, any]()

	// Zero key in the trie
	for _, k0 := range keys {
		for _, k1 := range keys {
			require.Nil(t, nextNonEmptyLeaf(tr, k0, k1))
		}
	}

	// One key in the trie
	for _, k0 := range keys {
		tr.Add(k0, struct{}{})
		require.Equal(t, 1, tr.Size())
		for _, k1 := range keys {
			for _, k2 := range keys {
				require.Equal(t, k0, nextNonEmptyLeaf(tr, k1, k2).Key)
			}
		}
		tr.Remove(k0)
	}

	// Two keys in the trie
	for _, k0 := range keys {
		for _, k1 := range keys {
			if k0 == k1 {
				continue
			}
			tr.Add(k0, struct{}{})
			tr.Add(k1, struct{}{})
			require.Equal(t, 2, tr.Size())

			for i := range keys {
				var expectedKey bitstr.Key
				for j := range keys {
					if keys[(i+j+1)%len(keys)] == k0 {
						expectedKey = k0
						break
					}
					if keys[(i+j+1)%len(keys)] == k1 {
						expectedKey = k1
						break
					}
				}
				require.Equal(t, expectedKey, nextNonEmptyLeaf(tr, keys[i], keys[0]).Key, "leaf after %s should be %s", keys[i], expectedKey)
			}

			tr.Remove(k0)
			tr.Remove(k1)
		}
	}

	// Three keys in the trie
	for i := range keys {
		currentKeys := []bitstr.Key{}
		for j := range keys {
			if i == j {
				continue
			}
			tr.Add(keys[j], struct{}{})
			currentKeys = append(currentKeys, keys[j])
		}
		require.Equal(t, 3, tr.Size())
		for j := range keys {
			var expectedKey bitstr.Key
		outerLoop:
			for q := range keys {
				for _, k := range currentKeys {
					if keys[(j+q+1)%len(keys)] == k {
						expectedKey = k
						break outerLoop
					}
				}
			}
			require.Equal(t, expectedKey, nextNonEmptyLeaf(tr, keys[j], keys[0]).Key, "leaf after %s should be %s", keys[j], expectedKey)
		}

		for j := range keys {
			if i == j {
				continue
			}
			tr.Remove(keys[j])
		}
	}

	// Four keys in the trie (only order "00")
	for _, k := range keys {
		tr.Add(k, struct{}{})
	}
	require.Equal(t, 4, tr.Size())
	for i := range keys {
		require.Equal(t, keys[(i+1)%len(keys)], nextNonEmptyLeaf(tr, keys[i], keys[0]).Key)
	}
	for _, k := range keys {
		tr.Remove(k)
	}
}
