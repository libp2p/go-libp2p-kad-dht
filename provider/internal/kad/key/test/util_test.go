package test

import (
	"strings"
	"testing"

	"github.com/probe-lab/go-libdht/kad/key"
	"github.com/probe-lab/go-libdht/kad/key/bit256"
	"github.com/probe-lab/go-libdht/kad/key/bitstr"
	"github.com/stretchr/testify/require"
)

func TestGenericCommonPrefixLength(t *testing.T) {
	k0 := bit256.NewKey(make([]byte, 32))             // 0000...0000
	k1 := bitstr.Key(strings.Repeat("0", 256))        // 0000...0000
	k2 := bitstr.Key("01" + strings.Repeat("0", 254)) // 0100...0000
	k3 := bitstr.Key("000")                           // 0001

	require.Equal(t, 256, key.CommonPrefixLength(k0, k1)) // keys are identical
	require.Equal(t, 1, key.CommonPrefixLength(k0, k2))   // only first bit is common
	require.Equal(t, 3, key.CommonPrefixLength(k0, k3))   // no common bits
}
