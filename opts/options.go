package dhtopts

import (
	"fmt"
	"time"

	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"
	"github.com/libp2p/go-libp2p-kad-dht/persist"
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
	BucketPeriod             time.Duration // how long to wait for a k-bucket to be queried before doing a random walk on it
	Timeout                  time.Duration // how long to wait for a bootstrap query to run
	RoutingTableScanInterval time.Duration // how often to scan the RT for k-buckets that haven't been queried since the given period
	SelfQueryInterval        time.Duration // how often to query for self
}

type PersistConfig struct {
	Snapshotter      persist.Snapshotter
	SeedsProposer    persist.SeedsProposer
	SnapshotInterval time.Duration
	FallbackPeers    []peer.ID
}

var DefaultSnapshotInterval = 5 * time.Minute

// Options is a structure containing all the options that can be used when constructing a DHT.
type Options struct {
	Datastore       ds.Batching
	Validator       record.Validator
	Client          bool
	Protocols       []protocol.ID
	Persistence     *PersistConfig
	BucketSize      int
	Datastore       ds.Batching
	Validator       record.Validator
	Client          bool
	Protocols       []protocol.ID
	BucketSize      int
	MaxRecordAge    time.Duration
	EnableProviders bool
	EnableValues    bool

	RoutingTable struct {
		RefreshQueryTimeout time.Duration
		RefreshPeriod       time.Duration
		AutoRefresh         bool
	}
	Persistence     *PersistConfig
	BootstrapConfig BootstrapConfig
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
	o.EnableProviders = true
	o.EnableValues = true

	o.RoutingTable.RefreshQueryTimeout = 10 * time.Second
	o.RoutingTable.RefreshPeriod = 1 * time.Hour
	o.RoutingTable.AutoRefresh = true
	o.MaxRecordAge = time.Hour * 36

	return nil
}

// RoutingTableRefreshQueryTimeout sets the timeout for routing table refresh
// queries.
func RoutingTableRefreshQueryTimeout(timeout time.Duration) Option {
	return func(o *Options) error {
		o.RoutingTable.RefreshQueryTimeout = timeout
		return nil
	}
	o.Persistence = new(PersistConfig)
	o.Persistence.SnapshotInterval = DefaultSnapshotInterval
	return nil
}

// RoutingTableRefreshPeriod sets the period for refreshing buckets in the
// routing table. The DHT will refresh buckets every period by:
//
// 1. First searching for nearby peers to figure out how many buckets we should try to fill.
// 1. Then searching for a random key in each bucket that hasn't been queried in
//    the last refresh period.
func RoutingTableRefreshPeriod(period time.Duration) Option {
	return func(o *Options) error {
		o.RoutingTable.RefreshPeriod = period
		return nil
	}
}

func SeedsProposer(sp persist.SeedsProposer) Option {
	return func(o *Options) error {
		o.Persistence.SeedsProposer = sp
		return nil
	}
}

func Snapshotter(snpshttr persist.Snapshotter, interval time.Duration) Option {
	return func(o *Options) error {
		o.Persistence.Snapshotter = snpshttr
		o.Persistence.SnapshotInterval = interval
		return nil
	}
}

func FallbackPeers(fallback []peer.ID) Option {
	return func(o *Options) error {
		o.Persistence.FallbackPeers = fallback
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

// MaxRecordAge specifies the maximum time that any node will hold onto a record ("PutValue record")
// from the time its received. This does not apply to any other forms of validity that
// the record may contain.
// For example, a record may contain an ipns entry with an EOL saying its valid
// until the year 2020 (a great time in the future). For that record to stick around
// it must be rebroadcasted more frequently than once every 'MaxRecordAge'
func MaxRecordAge(maxAge time.Duration) Option {
	return func(o *Options) error {
		o.MaxRecordAge = maxAge
		return nil
	}
}

// DisableAutoRefresh completely disables 'auto-refresh' on the DHT routing
// table. This means that we will neither refresh the routing table periodically
// nor when the routing table size goes below the minimum threshold.
func DisableAutoRefresh() Option {
	return func(o *Options) error {
		o.RoutingTable.AutoRefresh = false
		return nil
	}
}

// DisableProviders disables storing and retrieving provider records.
//
// Defaults to enabled.
//
// WARNING: do not change this unless you're using a forked DHT (i.e., a private
// network and/or distinct DHT protocols with the `Protocols` option).
func DisableProviders() Option {
	return func(o *Options) error {
		o.EnableProviders = false
		return nil
	}
}

// DisableProviders disables storing and retrieving value records (including
// public keys).
//
// Defaults to enabled.
//
// WARNING: do not change this unless you're using a forked DHT (i.e., a private
// network and/or distinct DHT protocols with the `Protocols` option).
func DisableValues() Option {
	return func(o *Options) error {
		o.EnableValues = false
		return nil
	}
}
