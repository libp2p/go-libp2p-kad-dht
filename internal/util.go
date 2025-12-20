package internal

import (
	"slices"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	mh "github.com/multiformats/go-multihash"
)

// CloneAddrInfo returns a copy of the AddrInfo with a cloned Addrs slice.
// This prevents data races when the original Addrs backing array is shared.
// See: https://github.com/ipfs/kubo/issues/11116
func CloneAddrInfo(ai peer.AddrInfo) peer.AddrInfo {
	return peer.AddrInfo{
		ID:    ai.ID,
		Addrs: slices.Clone(ai.Addrs),
	}
}

// Hash is the global IPFS hash function. uses multihash SHA2_256, 256 bits
func Hash(data []byte) mh.Multihash {
	h, err := mh.Sum(data, mh.SHA2_256, -1)
	if err != nil {
		// this error can be safely ignored (panic) because multihash only fails
		// from the selection of hash function. If the fn + length are valid, it
		// won't error.
		panic("multihash failed to hash using SHA2_256.")
	}
	return h
}

// ParseRFC3339 parses an RFC3339Nano-formatted time stamp and
// returns the UTC time.
func ParseRFC3339(s string) (time.Time, error) {
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Time{}, err
	}
	return t.UTC(), nil
}

// FormatRFC3339 returns the string representation of the
// UTC value of the given time in RFC3339Nano format.
func FormatRFC3339(t time.Time) string {
	return t.UTC().Format(time.RFC3339Nano)
}
