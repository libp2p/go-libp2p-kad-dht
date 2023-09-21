package cplutil

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"

	mh "github.com/multiformats/go-multihash"

	"github.com/libp2p/go-libp2p-kad-dht/v2/kadt"
)

//go:generate go run ./gen.go

// GenRandPeerID generates a random peerID for a given cpl
// Ported from go-libp2p-kbucket
func GenRandPeerID(k kadt.Key, cpl int) (kadt.PeerID, error) {
	if cpl > 15 {
		return "", fmt.Errorf("cannot generate peer ID for Cpl greater than 15")
	}

	targetPrefix := prefix(k, cpl)

	// Convert to a known peer ID.
	key := keyPrefixMap[targetPrefix]
	id := [32 + 2]byte{mh.SHA2_256, 32}
	binary.BigEndian.PutUint32(id[2:], key)
	return kadt.PeerID(string(id[:])), nil
}

// prefix generates random bits that have a common prefix length of exactly cpl with the supplied key.
func prefix(k kadt.Key, cpl int) uint16 {
	var p uint16
	// copy the first cpl+1 bits so we can flip the last one
	for i := 0; i < cpl+1; i++ {
		bit := uint16(k.Bit(i)) << (15 - i)
		p |= bit
	}

	// flip the bit at cpl (cpl 5 means bits 0-4 must be the same)
	mask := uint16(1) << (15 - cpl)
	p ^= mask

	if cpl < 15 {
		// pad with random data
		var buf [2]byte
		_, _ = rand.Read(buf[:])
		r := binary.BigEndian.Uint16(buf[:])

		mask = (^uint16(0)) << (15 - cpl)
		p = (p & mask) | (r & ^mask)
	}
	return p
}
