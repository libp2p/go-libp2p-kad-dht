package fullrt

import (
	"fmt"
	"github.com/libp2p/go-libp2p-core/peer"
	kaddht "github.com/libp2p/go-libp2p-kad-dht"
)

type config struct {
	dhtOpts                   []kaddht.Option
	initialRTPeers            []peer.AddrInfo
	skipInitialRTVerification bool
}

func (cfg *config) apply(opts ...Option) error {
	for i, o := range opts {
		if err := o(cfg); err != nil {
			return fmt.Errorf("fullrt dht option %d failed: %w", i, err)
		}
	}
	return nil
}

type Option func(opt *config) error

func DHTOption(opts ...kaddht.Option) Option {
	return func(c *config) error {
		c.dhtOpts = append(c.dhtOpts, opts...)
		return nil
	}
}

// LoadRoutingTable loads the given set of peers into the routing table.
// If skipInitialVerification is true then the DHT client will be immediately usable,
// instead of using the peers as seeds for the initial routing table scan.
//
// Note: While skipping the initial verification will result in the DHT client being
// usable more quickly on startup, if the persisted routing table is out of date or
// corrupted then any initial operations might not give accurate results (e.g. not
// finding data present in the network or putting data to fewer nodes or nodes
// that are no longer closest to the keys)
func LoadRoutingTable(peers []peer.AddrInfo, skipInitialVerification bool) Option {
	return func(c *config) error {
		c.initialRTPeers = peers
		c.skipInitialRTVerification = skipInitialVerification
		return nil
	}
}
