package keyspace

import (
	"crypto/rand"
	"fmt"
	"sort"
	"testing"

	"github.com/ipfs/go-test/random"
	kb "github.com/libp2p/go-libp2p-kbucket"
	"github.com/libp2p/go-libp2p/core/peer"
	mh "github.com/multiformats/go-multihash"
	"github.com/probe-lab/go-libdht/kad/key"
	"github.com/probe-lab/go-libdht/kad/key/bit256"
	"github.com/probe-lab/go-libdht/kad/key/bitstr"
	"github.com/probe-lab/go-libdht/kad/trie"
	"github.com/stretchr/testify/require"
)

func TestAllEntries(t *testing.T) {
	tr := trie.New[bitstr.Key, string]()
	elements := []struct {
		key   bitstr.Key
		fruit string
	}{
		{
			key:   bitstr.Key("000"),
			fruit: "apple",
		},
		{
			key:   bitstr.Key("010"),
			fruit: "banana",
		},
		{
			key:   bitstr.Key("101"),
			fruit: "cherry",
		},
		{
			key:   bitstr.Key("111"),
			fruit: "durian",
		},
	}

	for _, e := range elements {
		tr.Add(e.key, e.fruit)
	}

	// Test in 0 -> 1 order
	entries := AllEntries(tr, bitstr.Key("000"))
	require.Equal(t, len(elements), len(entries))
	for i := range entries {
		require.Equal(t, entries[i].Key, elements[i].key)
		require.Equal(t, entries[i].Data, elements[i].fruit)
	}

	// Test in reverse order (1 -> 0)
	entries = AllEntries(tr, bitstr.Key("111"))
	require.Equal(t, len(elements), len(entries))
	for i := range entries {
		require.Equal(t, entries[i].Key, elements[len(elements)-1-i].key)
		require.Equal(t, entries[i].Data, elements[len(elements)-1-i].fruit)
	}
}

func TestFindPrefixOfKey(t *testing.T) {
	tr := trie.New[bitstr.Key, struct{}]()

	keys := []bitstr.Key{
		"00",
		"10",
	}
	for _, k := range keys {
		tr.Add(k, struct{}{})
	}

	match, ok := FindPrefixOfKey(tr, bitstr.Key("00"))
	require.True(t, ok)
	require.Equal(t, bitstr.Key("00"), match)

	match, ok = FindPrefixOfKey(tr, bitstr.Key("10000000"))
	require.True(t, ok)
	require.Equal(t, bitstr.Key("10"), match)

	_, ok = FindPrefixOfKey(tr, bitstr.Key("01"))
	require.False(t, ok)
	_, ok = FindPrefixOfKey(tr, bitstr.Key("11000000"))
	require.False(t, ok)
}

func TestFindSubtrie(t *testing.T) {
	keys := []bitstr.Key{
		"0000",
		"0001",
		"0010",
		"0100",
		"0111",
		"1010",
		"1011",
		"1101",
		"1110",
	}
	tr := trie.New[bitstr.Key, struct{}]()

	_, ok := FindSubtrie(tr, bitstr.Key("0000"))
	require.False(t, ok)

	for _, k := range keys {
		tr.Add(k, struct{}{})
	}

	subtrie, ok := FindSubtrie(tr, bitstr.Key(""))
	require.True(t, ok)
	require.Equal(t, tr, subtrie)
	require.Equal(t, 9, subtrie.Size())

	subtrie, ok = FindSubtrie(tr, bitstr.Key("0"))
	require.True(t, ok)
	require.Equal(t, tr.Branch(0), subtrie)
	require.Equal(t, 5, subtrie.Size())

	subtrie, ok = FindSubtrie(tr, bitstr.Key("1"))
	require.True(t, ok)
	require.Equal(t, tr.Branch(1), subtrie)
	require.Equal(t, 4, subtrie.Size())

	subtrie, ok = FindSubtrie(tr, bitstr.Key("000"))
	require.True(t, ok)
	require.Equal(t, tr.Branch(0).Branch(0).Branch(0), subtrie)
	require.Equal(t, 2, subtrie.Size())

	subtrie, ok = FindSubtrie(tr, bitstr.Key("0000"))
	require.True(t, ok)
	require.Equal(t, tr.Branch(0).Branch(0).Branch(0).Branch(0), subtrie)
	require.Equal(t, 1, subtrie.Size())
	require.True(t, subtrie.IsNonEmptyLeaf())

	subtrie, ok = FindSubtrie(tr, bitstr.Key("111"))
	require.True(t, ok)
	require.Equal(t, tr.Branch(1).Branch(1).Branch(1), subtrie)
	require.Equal(t, 1, subtrie.Size())
	require.True(t, subtrie.IsNonEmptyLeaf())

	_, ok = FindSubtrie(tr, bitstr.Key("100"))
	require.False(t, ok)
	_, ok = FindSubtrie(tr, bitstr.Key("1001"))
	require.False(t, ok)
	_, ok = FindSubtrie(tr, bitstr.Key("00000"))
	require.False(t, ok)
}

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
			nextKey := NextNonEmptyLeaf(tr, k, order).Key
			require.Equal(t, binaryKeys[(i+1)%nKeys], nextKey)
		}
	})

	t.Run("Cycle", func(t *testing.T) {
		initialKey := binaryKeys[0]
		k := initialKey
		for range binaryKeys {
			k = NextNonEmptyLeaf(tr, k, order).Key
		}
		require.Equal(t, initialKey, k)
	})

	order = binaryKeys[nKeys-1]
	t.Run("CustomOrder", func(t *testing.T) {
		for i, k := range binaryKeys {
			nextKey := NextNonEmptyLeaf(tr, k, order).Key
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
			nextKey := NextNonEmptyLeaf(tr, k, order).Key
			require.Equal(t, binaryKeys[(i+1)%nKeys], nextKey)
		}
	})

	t.Run("MissingKey", func(t *testing.T) {
		for i := range 1 << bitlen {
			binary := fmt.Sprintf("%0*b", bitlen, i)
			k := bitstr.Key(binary)
			nextKey := NextNonEmptyLeaf(tr, k, order).Key
			require.Equal(t, binaryKeys[((i+1)%(1<<bitlen))/(1<<sparsity)], nextKey, k)
		}
	})

	order = binaryKeys[nKeys-1]
	t.Run("CustomOrder", func(t *testing.T) {
		for i, k := range binaryKeys {
			nextKey := NextNonEmptyLeaf(tr, k, order).Key
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
			currentKey = NextNonEmptyLeaf(tr, currentKey, order).Key
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
			require.Nil(t, NextNonEmptyLeaf(tr, k0, k1))
		}
	}

	// One key in the trie
	for _, k0 := range keys {
		tr.Add(k0, struct{}{})
		require.Equal(t, 1, tr.Size())
		for _, k1 := range keys {
			for _, k2 := range keys {
				require.Equal(t, k0, NextNonEmptyLeaf(tr, k1, k2).Key)
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
				require.Equal(t, expectedKey, NextNonEmptyLeaf(tr, keys[i], keys[0]).Key, "leaf after %s should be %s", keys[i], expectedKey)
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
			require.Equal(t, expectedKey, NextNonEmptyLeaf(tr, keys[j], keys[0]).Key, "leaf after %s should be %s", keys[j], expectedKey)
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
		require.Equal(t, keys[(i+1)%len(keys)], NextNonEmptyLeaf(tr, keys[i], keys[0]).Key)
	}
	for _, k := range keys {
		tr.Remove(k)
	}
}

func TestPruneSubtrie(t *testing.T) {
	tr := trie.New[bitstr.Key, struct{}]()

	keys := []bitstr.Key{
		"0000",
		"0001",
		"0011",
		"0100",
		"0110",
		"1010",
		"1101",
		"1110",
	}

	resetTrie := func() {
		*tr = trie.Trie[bitstr.Key, struct{}]{}
		for _, k := range keys {
			tr.Add(k, struct{}{})
		}
		require.Equal(t, len(keys), tr.Size())
	}

	resetTrie()
	PruneSubtrie(tr, bitstr.Key(""))
	require.Equal(t, 0, tr.Size())
	require.True(t, tr.IsEmptyLeaf())
	require.Nil(t, tr.Key())
	require.Nil(t, tr.Branch(0))
	require.Nil(t, tr.Branch(1))

	resetTrie()
	PruneSubtrie(tr, bitstr.Key("0"))
	require.Equal(t, 3, tr.Size())
	require.True(t, tr.Branch(0).IsEmptyLeaf())
	require.False(t, tr.Branch(1).IsLeaf())

	resetTrie()
	PruneSubtrie(tr, bitstr.Key("00"))
	require.Equal(t, 5, tr.Size())
	require.True(t, tr.Branch(0).Branch(0).IsEmptyLeaf())

	resetTrie()
	PruneSubtrie(tr, bitstr.Key("0000"))
	require.Equal(t, 7, tr.Size())

	keys = []bitstr.Key{
		"0000",
		"0001",
		"1000",
	}

	resetTrie()
	PruneSubtrie(tr, bitstr.Key("11"))
	require.Equal(t, 3, tr.Size())

	resetTrie()
	PruneSubtrie(tr, bitstr.Key("000"))
	require.Equal(t, 1, tr.Size())
	require.True(t, tr.Branch(0).IsEmptyLeaf())
}

func TestTrieGaps(t *testing.T) {
	initTrie := func(keys []bitstr.Key) *trie.Trie[bitstr.Key, struct{}] {
		tr := trie.New[bitstr.Key, struct{}]()
		for _, k := range keys {
			tr.Add(k, struct{}{})
		}
		return tr
	}
	t.Run("Gap in empty trie", func(t *testing.T) {
		keys := []bitstr.Key{}
		tr := initTrie(keys)
		require.Equal(t, []bitstr.Key{""}, TrieGaps(tr))
	})
	t.Run("No gaps in flat trie", func(t *testing.T) {
		keys := []bitstr.Key{
			"0",
			"1",
		}
		tr := initTrie(keys)
		require.Empty(t, TrieGaps(tr))
	})
	t.Run("No gaps in unbalanced trie", func(t *testing.T) {
		keys := []bitstr.Key{
			"0",
			"10",
			"110",
			"111",
		}
		tr := initTrie(keys)
		require.Empty(t, TrieGaps(tr))
	})
	t.Run("No gaps in trie with empty key", func(t *testing.T) {
		keys := []bitstr.Key{
			"",
		}
		tr := initTrie(keys)
		require.Empty(t, TrieGaps(tr))
	})
	t.Run("Gap with single key - 0", func(t *testing.T) {
		keys := []bitstr.Key{
			"0",
		}
		tr := initTrie(keys)
		require.Equal(t, []bitstr.Key{"1"}, TrieGaps(tr))
	})
	t.Run("Gap with single key - 1", func(t *testing.T) {
		keys := []bitstr.Key{
			"1",
		}
		tr := initTrie(keys)
		require.Equal(t, []bitstr.Key{"0"}, TrieGaps(tr))
	})
	t.Run("Gap with single key - 11101101", func(t *testing.T) {
		keys := []bitstr.Key{
			"11101101",
		}
		tr := initTrie(keys)
		require.Equal(t, SiblingPrefixes(keys[0]), TrieGaps(tr))
	})
	t.Run("Gap missing single key - 0", func(t *testing.T) {
		keys := []bitstr.Key{
			"0",
			"10",
			"110",
		}
		tr := initTrie(keys)
		require.Equal(t, []bitstr.Key{"111"}, TrieGaps(tr))
	})
	t.Run("Gap missing single key - 1", func(t *testing.T) {
		keys := []bitstr.Key{
			"0",
			"10",
			"1100",
			"1101",
		}
		tr := initTrie(keys)
		require.Equal(t, []bitstr.Key{"111"}, TrieGaps(tr))
	})
	t.Run("Gap missing single key - 2", func(t *testing.T) {
		keys := []bitstr.Key{
			"000",
			"001",
			"01",
			"10",
			"1100",
			"1101",
		}
		tr := initTrie(keys)
		require.Equal(t, []bitstr.Key{"111"}, TrieGaps(tr))
	})
	t.Run("Gap missing multiple keys - 0", func(t *testing.T) {
		keys := []bitstr.Key{
			"000",
			"01",
			"10",
			"1100",
			"1101",
		}
		tr := initTrie(keys)
		require.Equal(t, []bitstr.Key{"001", "111"}, TrieGaps(tr))
	})
	t.Run("Gap missing multiple keys - 1", func(t *testing.T) {
		keys := []bitstr.Key{
			"000",
			"1101",
		}
		tr := initTrie(keys)
		require.Equal(t, []bitstr.Key{"01", "001", "10", "111", "1100"}, TrieGaps(tr))
	})
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
	allocs := AllocateToKClosest(items, dests, 3)

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
	allocs := AllocateToKClosest(items, dests, 3)

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

func TestAllocateToKClosestSingleDest(t *testing.T) {
	destKeys := []bitstr.Key{
		"0000",
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
	allocs := AllocateToKClosest(items, dests, 3)

	require.Len(t, allocs, 1)
	require.ElementsMatch(t, allocs[destKeys[0]], itemKeys)
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

	allocs := AllocateToKClosest(items, dests, replication)

	for _, itemEntry := range AllEntries(items, bit256.ZeroKey()) {
		i := itemEntry.Key
		closest := trie.Closest(dests, i, replication)
		for _, closestEntry := range closest {
			d := closestEntry.Key
			require.Contains(t, allocs[d], i, "Item %s should be allocated to destination %s", key.BitString(i), key.BitString(d))
		}
	}
}

func TestRegionsFromPeers(t *testing.T) {
	// No peers
	regions, commonPrefix := RegionsFromPeers(nil, 1, bit256.ZeroKey())
	require.Empty(t, regions)
	require.Equal(t, bitstr.Key(""), commonPrefix)

	// Single peer
	p0 := random.Peers(1)[0]
	regions, commonPrefix = RegionsFromPeers([]peer.ID{p0}, 1, bit256.ZeroKey())
	require.Len(t, regions, 1)
	bstrPid0 := bitstr.Key(key.BitString(PeerIDToBit256(p0)))
	require.Equal(t, bstrPid0, commonPrefix)

	// Two peers
	p1 := random.Peers(1)[0]
	regions, commonPrefix = RegionsFromPeers([]peer.ID{p0, p1}, 2, bit256.ZeroKey())
	require.Len(t, regions, 1)
	cpl := key.CommonPrefixLength(bstrPid0, PeerIDToBit256(p1))
	common := bstrPid0[:cpl]
	require.Equal(t, common, commonPrefix)

	// Three peers
	p2 := random.Peers(1)[0]
	regions, commonPrefix = RegionsFromPeers([]peer.ID{p0, p1, p2}, 2, bit256.ZeroKey())
	require.Len(t, regions, 1)
	cpl = key.CommonPrefixLength(common, PeerIDToBit256(p2))
	common = common[:cpl]
	require.Equal(t, common, commonPrefix)

	// From 4 peers onwards, there is a probability of having more than 1
	// regions. Refer to TestExtractMinimalRegions.
}

func TestExtractMinimalRegions(t *testing.T) {
	replicationFactor := 3
	selfID := [32]byte{}
	order := bit256.NewKey(selfID[:])

	genPeerWithPrefix := func(prefix bitstr.Key) peer.ID {
		k := FirstFullKeyWithPrefix(prefix, order)
		bs := KeyToBytes(k)
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
		peersTrie.Add(PeerIDToBit256(pid), pid)
	}

	regions = extractMinimalRegions(peersTrie, bitstr.Key(""), replicationFactor, order)
	require.Len(t, regions, 3)
	require.Equal(t, bitstr.Key("00"), regions[0].Prefix)
	require.Equal(t, bitstr.Key("01"), regions[1].Prefix)
	require.Equal(t, bitstr.Key("1"), regions[2].Prefix)
	require.Equal(t, 4, regions[0].Peers.Size())
	require.Equal(t, 4, regions[1].Peers.Size())
	require.Equal(t, 6, regions[2].Peers.Size())
}

func TestAssignKeysToRegions(t *testing.T) {
	regions := []Region{
		{Prefix: "0", Peers: nil, Keys: nil},
		{Prefix: "1", Peers: nil, Keys: nil},
	}

	nKeys := 1 << 8
	mhs := make([]mh.Multihash, nKeys)
	for i, h := range genMultihashes(nKeys) {
		mhs[i] = h
	}

	regions = AssignKeysToRegions(regions, mhs)
	for _, r := range regions {
		for _, h := range AllValues(r.Keys, bit256.ZeroKey()) {
			k := MhToBit256(h)
			require.True(t, IsPrefix(r.Prefix, k))
		}
	}
}
