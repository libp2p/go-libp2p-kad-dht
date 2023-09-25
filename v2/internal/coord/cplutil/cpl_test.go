package cplutil

import (
	"crypto/rand"
	"encoding/binary"
	"testing"

	"github.com/libp2p/go-libp2p-kad-dht/v2/kadt"
	"github.com/plprobelab/go-kademlia/key"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPrefix(t *testing.T) {
	testCases := []uint16{
		0b1111111111111111,
		0b1111111111111101,
		0b1011111111111101,
		0b0000000000000000,
		0b0000000000000010,
	}

	makeKeyWithPrefix := func(v uint16) key.Key256 {
		data := [32]byte{}
		binary.BigEndian.PutUint16(data[0:2], v)
		return key.NewKey256(data[:])
	}

	for _, tc := range testCases {
		k := makeKeyWithPrefix(tc)

		for cpl := 0; cpl < 15; cpl++ {
			p := prefix(k, cpl)
			k2 := makeKeyWithPrefix(p)
			assert.Equal(t, cpl, k.CommonPrefixLength(k2), "cpl %d: generated prefix %016b for key starting %016b", cpl, p, tc)
		}
	}
}

func TestGenRandPeerID(t *testing.T) {
	randomKey := func() kadt.Key {
		var buf [32]byte
		_, _ = rand.Read(buf[:])
		return kadt.NewKey(buf[:])
	}

	keys := make([]kadt.Key, 20)
	for i := range keys {
		keys[i] = randomKey()
	}

	for _, k := range keys {
		for cpl := 0; cpl < 15; cpl++ {
			id, err := GenRandPeerID(k, cpl)
			require.NoError(t, err)

			assert.Equal(t, cpl, k.CommonPrefixLength(id.Key()))
		}
	}
}
