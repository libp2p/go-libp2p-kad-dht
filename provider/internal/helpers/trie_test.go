package helpers

import (
	"testing"

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
