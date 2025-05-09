package reprovider

import (
	"bytes"
	"testing"
	"time"

	"github.com/probe-lab/go-libdht/kad/key/bit256"
	"github.com/probe-lab/go-libdht/kad/key/bitstr"
	"github.com/stretchr/testify/require"
)

func TestReprovideTimeForPrefixWithOrderZero(t *testing.T) {
	s := reprovideSweeper{
		reprovideInterval: 16 * time.Second,
		order:             bit256.ZeroKey(),
	}

	require.Equal(t, 0*time.Second, s.reprovideTimeForPrefix("0"))
	require.Equal(t, 8*time.Second, s.reprovideTimeForPrefix("1"))
	require.Equal(t, 0*time.Second, s.reprovideTimeForPrefix("000"))
	require.Equal(t, 8*time.Second, s.reprovideTimeForPrefix("1000"))
	require.Equal(t, 10*time.Second, s.reprovideTimeForPrefix("1010"))
	require.Equal(t, 15*time.Second, s.reprovideTimeForPrefix("1111"))
}

func TestReprovideTimeForPrefixWithCustomOrder(t *testing.T) {
	s := reprovideSweeper{
		reprovideInterval: 16 * time.Second,
		order:             bit256.NewKey(bytes.Repeat([]byte{0xFF}, 32)), // 111...1
	}

	require.Equal(t, 0*time.Second, s.reprovideTimeForPrefix("1"))
	require.Equal(t, 8*time.Second, s.reprovideTimeForPrefix("0"))
	require.Equal(t, 0*time.Second, s.reprovideTimeForPrefix("111"))
	require.Equal(t, 8*time.Second, s.reprovideTimeForPrefix("0111"))
	require.Equal(t, 10*time.Second, s.reprovideTimeForPrefix("0101"))
	require.Equal(t, 15*time.Second, s.reprovideTimeForPrefix("0000"))
}

func TestKeyToBytes(t *testing.T) {
	require.Equal(t, []byte{0b00000000}, keyToBytes(bitstr.Key("0")))
	require.Equal(t, []byte{0b00000000}, keyToBytes(bitstr.Key("00000000")))
	require.Equal(t, []byte{0b00000000, 0b00000000}, keyToBytes(bitstr.Key("000000000")))
	require.Equal(t, []byte{0b00110000}, keyToBytes(bitstr.Key("0011")))
	require.Equal(t, []byte{0b11111110}, keyToBytes(bitstr.Key("1111111")))
}

// TODO: Test that the peers returned by GetClosestPeers are sorted by distance to the requested key
