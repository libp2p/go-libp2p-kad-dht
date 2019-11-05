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

// Options is a structure containing all the options that can be used when constructing a DHT.
type Options struct {
	Datastore  ds.Batching
	Validator  record.Validator
	Client     bool
	Protocols  []protocol.ID
	BucketSize int

	BootstrapTimeout time.Duration
	BootstrapPeriod  time.Duration
	AutoBootstrap    bool
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

	o.BootstrapTimeout = 10 * time.Second
	o.BootstrapPeriod = 1 * time.Hour
	o.AutoBootstrap = true

	return nil
}

// BootstrapTimeout sets the timeout for bootstrap queries.
func BootstrapTimeout(timeout time.Duration) Option {
	return func(o *Options) error {
		o.BootstrapTimeout = timeout
		return nil
	}
}

// BootstrapPeriod sets the period for bootstrapping. The DHT will bootstrap
// every bootstrap period by:
//
// 1. First searching for nearby peers to figure out how many buckets we should try to fill.
// 1. Then searching for a random key in each bucket that hasn't been queried in
//    the last bootstrap period.
func BootstrapPeriod(period time.Duration) Option {
	return func(o *Options) error {
		o.BootstrapPeriod = period
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
		o.AutoBootstrap = false
		return nil
	}
}
