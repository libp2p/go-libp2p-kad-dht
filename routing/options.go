package routing

import (
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/routing"
)

type quorumOptionKey struct{}

const defaultQuorum = 0

// Quorum is a DHT option that tells the DHT how many peers it needs to get
// values from before returning the best one. Zero means the DHT query
// should complete instead of returning early.
//
// Default: 0
func Quorum(n int) routing.Option {
	return func(opts *routing.Options) error {
		if opts.Other == nil {
			opts.Other = make(map[interface{}]interface{}, 1)
		}
		opts.Other[quorumOptionKey{}] = n
		return nil
	}
}

func GetQuorum(opts *routing.Options) int {
	responsesNeeded, ok := opts.Other[quorumOptionKey{}].(int)
	if !ok {
		responsesNeeded = defaultQuorum
	}
	return responsesNeeded
}

type seedPeersOptionKey struct{}
type SeedPeersOptions struct {
	SeedPeers  []peer.ID
	UseRTPeers bool
}

// SeedPeers is a DHT option that tells the DHT which peers it should use to seed a DHT query.
//
// Default: Use BucketSize closest peers to the target that are in the routing table
func SeedPeers(seedPeers []peer.ID, useRoutingTablePeers bool) routing.Option {
	return func(opts *routing.Options) error {
		if opts.Other == nil {
			opts.Other = make(map[interface{}]interface{}, 1)
		}
		opts.Other[seedPeersOptionKey{}] = SeedPeersOptions{SeedPeers: seedPeers, UseRTPeers: useRoutingTablePeers}
		return nil
	}
}

func GetSeedPeers(opts *routing.Options) SeedPeersOptions {
	seedPeersOpts, ok := opts.Other[seedPeersOptionKey{}].(SeedPeersOptions)
	if !ok {
		seedPeersOpts = SeedPeersOptions{UseRTPeers: true}
	}
	return seedPeersOpts
}
