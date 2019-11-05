package dhtopts

import (
	"fmt"
	"time"

	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/libp2p/go-libp2p-core/protocol"
	"github.com/libp2p/go-libp2p-record"
)

// Deprecated: The old format did not support more than one message per stream, and is not supported
// or relevant with stream pooling. ProtocolDHT should be used instead.
const ProtocolDHTOld protocol.ID = "/ipfs/dht"

var (
	ProtocolDHT      protocol.ID = "/ipfs/kad/1.0.0"
	DefaultProtocols             = []protocol.ID{ProtocolDHT}
)

// BootstrapConfig specifies parameters used for bootstrapping the DHT.
type BootstrapConfig struct {
	BucketPeriod      time.Duration // how long to wait for a k-bucket to be queried before doing a random walk on it
	Timeout           time.Duration // how long to wait for a bootstrap query to run
	SelfQueryInterval time.Duration // how often to query for self
}

// Options is a structure containing all the options that can be used when constructing a DHT.
type Options struct {
	Datastore            ds.Batching
	Validator            record.Validator
	Client               bool
	Protocols            []protocol.ID
	BucketSize           int
	BootstrapConfig      BootstrapConfig
	TriggerAutoBootstrap bool
}

// Apply applies the given options to this Option
func (o *Options) Apply(opts ...Option) error {
	for i, opt := range opts {
		if err := opt(o); err != nil {
			return fmt.Errorf("dht option %d failed: %s", i, err)
		}
	}
	return nil
}

// Option DHT option type.
type Option func(*Options) error

// Defaults are the default DHT options. This option will be automatically
// prepended to any options you pass to the DHT constructor.
var Defaults = func(o *Options) error {
	o.Validator = record.NamespacedValidator{
		"pk": record.PublicKeyValidator{},
	}
	o.Datastore = dssync.MutexWrap(ds.NewMapDatastore())
	o.Protocols = DefaultProtocols

	o.BootstrapConfig = BootstrapConfig{
		// same as that mentioned in the kad dht paper
		BucketPeriod: 1 * time.Hour,

		Timeout: 10 * time.Second,

		SelfQueryInterval: 1 * time.Hour,
	}

	o.TriggerAutoBootstrap = true

	return nil
}

// Bootstrap configures the dht bootstrapping process
func Bootstrap(b BootstrapConfig) Option {
	return func(o *Options) error {
		o.BootstrapConfig = b
		return nil
	}
}

// Datastore configures the DHT to use the specified datastore.
//
// Defaults to an in-memory (temporary) map.
func Datastore(ds ds.Batching) Option {
	return func(o *Options) error {
		o.Datastore = ds
		return nil
	}
}

// Client configures whether or not the DHT operates in client-only mode.
//
// Defaults to false.
func Client(only bool) Option {
	return func(o *Options) error {
		o.Client = only
		return nil
	}
}

// Validator configures the DHT to use the specified validator.
//
// Defaults to a namespaced validator that can only validate public keys.
func Validator(v record.Validator) Option {
	return func(o *Options) error {
		o.Validator = v
		return nil
	}
}

// NamespacedValidator adds a validator namespaced under `ns`. This option fails
// if the DHT is not using a `record.NamespacedValidator` as it's validator (it
// uses one by default but this can be overridden with the `Validator` option).
//
// Example: Given a validator registered as `NamespacedValidator("ipns",
// myValidator)`, all records with keys starting with `/ipns/` will be validated
// with `myValidator`.
func NamespacedValidator(ns string, v record.Validator) Option {
	return func(o *Options) error {
		nsval, ok := o.Validator.(record.NamespacedValidator)
		if !ok {
			return fmt.Errorf("can only add namespaced validators to a NamespacedValidator")
		}
		nsval[ns] = v
		return nil
	}
}

// Protocols sets the protocols for the DHT
//
// Defaults to dht.DefaultProtocols
func Protocols(protocols ...protocol.ID) Option {
	return func(o *Options) error {
		o.Protocols = protocols
		return nil
	}
}

// BucketSize configures the bucket size of the routing table.
//
// The default value is 20.
func BucketSize(bucketSize int) Option {
	return func(o *Options) error {
		o.BucketSize = bucketSize
		return nil
	}
}

// DisableAutoBootstrap completely disables 'auto-bootstrap' on the Dht
// This means that neither will we do periodic bootstrap nor will we
// bootstrap the Dht even if the Routing Table size goes below the minimum threshold
func DisableAutoBootstrap() Option {
	return func(o *Options) error {
		o.TriggerAutoBootstrap = false
		return nil
	}
}
