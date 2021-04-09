package fullrt

import (
	ds "github.com/ipfs/go-datastore"
	"github.com/libp2p/go-libp2p-core/peer"
	record "github.com/libp2p/go-libp2p-record"
)

type config struct {
	validator      record.Validator
	datastore      ds.Batching
	bootstrapPeers []peer.AddrInfo
}

// Option DHT option type.
type Option func(*config) error

func Validator(validator record.Validator) Option {
	return func(c *config) error {
		c.validator = validator
		return nil
	}
}

// BootstrapPeers configures the bootstrapping nodes that we will connect to to seed
// and refresh our Routing Table if it becomes empty.
func BootstrapPeers(bootstrappers ...peer.AddrInfo) Option {
	return func(c *config) error {
		c.bootstrapPeers = bootstrappers
		return nil
	}
}

// Datastore configures the DHT to use the specified datastore.
//
// Defaults to an in-memory (temporary) map.
func Datastore(ds ds.Batching) Option {
	return func(c *config) error {
		c.datastore = ds
		return nil
	}
}
