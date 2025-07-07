package reprovider

import (
	"crypto/rand"
	"fmt"
	"sort"
	"testing"

	kb "github.com/libp2p/go-libp2p-kbucket"
	"github.com/libp2p/go-libp2p/core/peer"
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

func TestExtractMinimalRegions(t *testing.T) {
	replicationFactor := 3
	selfID := [32]byte{}
	order := bit256.NewKey(selfID[:])

	genPeerWithPrefix := func(prefix bitstr.Key) peer.ID {
		k := firstFullKeyWithPrefix(prefix, order)
		bs := keyToBytes(k)
		pid, err := kb.GenRandPeerIDWithCPL(bs, uint(len(prefix)))
		require.NoError(t, err)
		return pid
	}

	prefixes := []bitstr.Key{
		"00000",
		"00001",
		"00101",
		"00110",
		"01000",
		"01001",
		"01011",
		"01100",
		"10100",
		"11000",
		"11001",
		"11010",
		"11110",
		"11111",
	}

	// Binary trie of peers
	//                                     ____________I___________
	//                                    /                        \
	//                 __________________/__                      __\_______
	//                /                     \                    /          \
	//         ______/_                     _\_                 /           _\_______
	//        /        \                   /   \               /           /         \
	//       /         _\_             ___/_    \             /       ____/_          \
	//      /         /   \           /     \    \           /       /      \          \
	//     / \       /     \        / \      \    \         /       / \      \        / \
	// 00000 00001 00101 00110  01000 01001 01011 01100  10100  11000 11001 11010 11110 11111
	//
	// Groups are expected to be:
	// * [00000, 00001, 00101, 00110]
	// * [01000, 01001, 01011, 01100]
	// * [10100, 11000, 11001, 11010, 11110, 11111]

	pids := make([]peer.ID, len(prefixes))
	peersTrie := trie.New[bit256.Key, peer.ID]()

	// Test behavior when trie is empty
	regions := extractMinimalRegions(peersTrie, bitstr.Key(""), replicationFactor, order)
	require.Nil(t, regions)
	for i := range pids {
		pid := genPeerWithPrefix(prefixes[i])
		pids[i] = pid
		peersTrie.Add(peerIDToBit256(pid), pid)
	}

	regions = extractMinimalRegions(peersTrie, bitstr.Key(""), replicationFactor, order)
	require.Len(t, regions, 3)
	require.Equal(t, bitstr.Key("00"), regions[0].prefix)
	require.Equal(t, bitstr.Key("01"), regions[1].prefix)
	require.Equal(t, bitstr.Key("1"), regions[2].prefix)
	require.Equal(t, 4, regions[0].peers.Size())
	require.Equal(t, 4, regions[1].peers.Size())
	require.Equal(t, 6, regions[2].peers.Size())
}

func TestAllocateToKClosestSingle(t *testing.T) {
	destKeys := []bitstr.Key{
		"0000",
		"0001",
		"0011",
		"0100",
		"0110",
		"1010",
		"1101",
		"1110",
	}
	dests := trie.New[bitstr.Key, bitstr.Key]()
	for _, k := range destKeys {
		dests.Add(k, k)
	}
	itemKeys := []bitstr.Key{
		"0000",
	}
	items := trie.New[bitstr.Key, bitstr.Key]()
	for _, k := range itemKeys {
		items.Add(k, k)
	}
	allocs := allocateToKClosest(items, dests, 3)

	// "0000" should be assigned to ["0000", "0001", "0011"]
	expected := map[bitstr.Key][]bitstr.Key{
		"0000": {"0000"},
		"0001": {"0000"},
		"0011": {"0000"},
	}
	require.Equal(t, expected, allocs)
}

func TestAllocateToKClosestBasic(t *testing.T) {
	destKeys := []bitstr.Key{
		"0000",
		"0001",
		"0011",
		"0100",
		"0110",
		"1010",
		"1101",
		"1110",
	}
	dests := trie.New[bitstr.Key, bitstr.Key]()
	for _, k := range destKeys {
		dests.Add(k, k)
	}
	itemKeys := []bitstr.Key{
		"0000",
		"0011",
		"0111",
		"1001",
		"1011",
		"1100",
		"1101",
	}
	items := trie.New[bitstr.Key, bitstr.Key]()
	for _, k := range itemKeys {
		items.Add(k, k)
	}
	allocs := allocateToKClosest(items, dests, 3)

	expected := map[bitstr.Key][]bitstr.Key{
		"0000": {"0000", "0011"},
		"0001": {"0000", "0011"},
		"0011": {"0000", "0011", "0111"},
		"0100": {"0111"},
		"0110": {"0111"},
		"1010": {"1001", "1011", "1100", "1101"},
		"1101": {"1001", "1011", "1100", "1101"},
		"1110": {"1001", "1011", "1100", "1101"},
	}
	require.Equal(t, expected, allocs)
}

func genRandBit256() bit256.Key {
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return bit256.NewKey(b[:])
}

func TestAllocateToKClosest(t *testing.T) {
	nItems := 2 << 12
	nDests := 2 << 10
	replication := 12

	items := trie.New[bit256.Key, bit256.Key]()
	for range nItems {
		i := genRandBit256()
		items.Add(i, i)
	}
	dests := trie.New[bit256.Key, bit256.Key]()
	for range nDests {
		d := genRandBit256()
		dests.Add(d, d)
	}

	allocs := allocateToKClosest(items, dests, replication)

	for _, itemEntry := range allEntries(items, bit256.ZeroKey()) {
		i := itemEntry.Key
		closest := trie.Closest(dests, i, replication)
		for _, closestEntry := range closest {
			d := closestEntry.Key
			require.Contains(t, allocs[d], i, "Item %s should be allocated to destination %s", key.BitString(i), key.BitString(d))
		}
	}
}
