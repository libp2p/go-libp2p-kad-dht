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
