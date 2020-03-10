package dht

import (
	"fmt"
	"time"

	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"
	host "github.com/libp2p/go-libp2p-host"
	record "github.com/libp2p/go-libp2p-record"
)

// ModeOpt describes what mode the dht should operate in
type ModeOpt int

const (
	// ModeAuto utilizes EvtLocalReachabilityChanged events sent over the event bus to dynamically switch the DHT
	// between Client and Server modes based on network conditions
	ModeAuto ModeOpt = iota
	// ModeClient operates the DHT as a client only, it cannot respond to incoming queries
	ModeClient
	// ModeServer operates the DHT as a server, it can both send and respond to queries
	ModeServer
)

const DefaultPrefix protocol.ID = "/ipfs"

// Options is a structure containing all the options that can be used when constructing a DHT.
type config struct {
	datastore       ds.Batching
	validator       record.Validator
	mode            ModeOpt
	protocolPrefix  protocol.ID
	bucketSize      int
	disjointPaths   int
	concurrency     int
	maxRecordAge    time.Duration
	enableProviders bool
	enableValues    bool
	queryPeerFilter QueryFilterFunc

	routingTable struct {
		refreshQueryTimeout time.Duration
		refreshPeriod       time.Duration
		autoRefresh         bool
		latencyTolerance    time.Duration
		checkInterval       time.Duration
		peerFilter          RouteTableFilterFunc
	}

	// internal parameters, not publicly exposed
	protocols, serverProtocols []protocol.ID

	// test parameters
	testProtocols []protocol.ID
}

func emptyQueryFilter(h host.Host, ai peer.AddrInfo) bool  { return true }
func emptyRTFilter(h host.Host, conns []network.Conn) bool { return true }

// apply applies the given options to this Option
func (c *config) apply(opts ...Option) error {
	for i, opt := range opts {
		if err := opt(c); err != nil {
			return fmt.Errorf("dht option %d failed: %s", i, err)
		}
	}
	return nil
}

// Option DHT option type.
type Option func(*config) error

const defaultBucketSize = 20

// defaults are the default DHT options. This option will be automatically
// prepended to any options you pass to the DHT constructor.
var defaults = func(o *config) error {
	o.validator = record.NamespacedValidator{
		"pk": record.PublicKeyValidator{},
	}
	o.datastore = dssync.MutexWrap(ds.NewMapDatastore())
	o.protocolPrefix = DefaultPrefix
	o.enableProviders = true
	o.enableValues = true
	o.queryPeerFilter = emptyQueryFilter

	o.routingTable.latencyTolerance = time.Minute
	o.routingTable.refreshQueryTimeout = 10 * time.Second
	o.routingTable.refreshPeriod = 10 * time.Minute
	o.routingTable.autoRefresh = true
	o.routingTable.peerFilter = emptyRTFilter
	o.maxRecordAge = time.Hour * 36

	o.bucketSize = defaultBucketSize
	o.concurrency = 3

	return nil
}

// applyFallbacks sets default DHT options. It is applied after Defaults and any options passed to the constructor in
// order to allow for defaults that are based on other set options.
func (c *config) applyFallbacks() error {
	if c.disjointPaths == 0 {
		c.disjointPaths = c.bucketSize / 2
	}
	return nil
}

func (c *config) validate() error {
	if c.protocolPrefix == DefaultPrefix {
		if c.bucketSize != defaultBucketSize {
			return fmt.Errorf("protocol prefix %s must use bucket size %d", DefaultPrefix, defaultBucketSize)
		}
		if !c.enableProviders {
			return fmt.Errorf("protocol prefix %s must have providers enabled", DefaultPrefix)
		}
		if !c.enableValues {
			return fmt.Errorf("protocol prefix %s must have values enabled", DefaultPrefix)
		}
		if nsval, ok := c.validator.(record.NamespacedValidator); !ok {
			return fmt.Errorf("protocol prefix %s must use a namespaced validator", DefaultPrefix)
		} else if len(nsval) > 2 || nsval["pk"] == nil || nsval["ipns"] == nil {
			return fmt.Errorf("protocol prefix %s must support only the /pk and /ipns namespaces", DefaultPrefix)
		}
		return nil
	}
	return nil
}

// RoutingTableCheckInterval is the interval between two runs of the RT cleanup routine.
func RoutingTableCheckInterval(i time.Duration) Option {
	return func(c *config) error {
		c.routingTable.checkInterval = i
		return nil
	}
}

// RoutingTableLatencyTolerance sets the maximum acceptable latency for peers
// in the routing table's cluster.
func RoutingTableLatencyTolerance(latency time.Duration) Option {
	return func(c *config) error {
		c.routingTable.latencyTolerance = latency
		return nil
	}
}

// RoutingTableRefreshQueryTimeout sets the timeout for routing table refresh
// queries.
func RoutingTableRefreshQueryTimeout(timeout time.Duration) Option {
	return func(c *config) error {
		c.routingTable.refreshQueryTimeout = timeout
		return nil
	}
}

// RoutingTableRefreshPeriod sets the period for refreshing buckets in the
// routing table. The DHT will refresh buckets every period by:
//
// 1. First searching for nearby peers to figure out how many buckets we should try to fill.
// 1. Then searching for a random key in each bucket that hasn't been queried in
//    the last refresh period.
func RoutingTableRefreshPeriod(period time.Duration) Option {
	return func(c *config) error {
		c.routingTable.refreshPeriod = period
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

// Mode configures which mode the DHT operates in (Client, Server, Auto).
//
// Defaults to ModeAuto.
func Mode(m ModeOpt) Option {
	return func(c *config) error {
		c.mode = m
		return nil
	}
}

// Validator configures the DHT to use the specified validator.
//
// Defaults to a namespaced validator that can only validate public keys.
func Validator(v record.Validator) Option {
	return func(c *config) error {
		c.validator = v
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
	return func(c *config) error {
		nsval, ok := c.validator.(record.NamespacedValidator)
		if !ok {
			return fmt.Errorf("can only add namespaced validators to a NamespacedValidator")
		}
		nsval[ns] = v
		return nil
	}
}

// ProtocolPrefix sets an application specific prefix to be attached to all DHT protocols. For example,
// /myapp/kad/1.0.0 instead of /ipfs/kad/1.0.0. Prefix should be of the form /myapp.
//
// Defaults to dht.DefaultPrefix
func ProtocolPrefix(prefix protocol.ID) Option {
	return func(c *config) error {
		c.protocolPrefix = prefix
		return nil
	}
}

// BucketSize configures the bucket size (k in the Kademlia paper) of the routing table.
//
// The default value is 20.
func BucketSize(bucketSize int) Option {
	return func(c *config) error {
		c.bucketSize = bucketSize
		return nil
	}
}

// Concurrency configures the number of concurrent requests (alpha in the Kademlia paper) for a given query path.
//
// The default value is 3.
func Concurrency(alpha int) Option {
	return func(c *config) error {
		c.concurrency = alpha
		return nil
	}
}

// DisjointPaths configures the number of disjoint paths (d in the S/Kademlia paper) taken per query.
//
// The default value is BucketSize/2.
func DisjointPaths(d int) Option {
	return func(c *config) error {
		c.disjointPaths = d
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
	return func(c *config) error {
		c.maxRecordAge = maxAge
		return nil
	}
}

// DisableAutoRefresh completely disables 'auto-refresh' on the DHT routing
// table. This means that we will neither refresh the routing table periodically
// nor when the routing table size goes below the minimum threshold.
func DisableAutoRefresh() Option {
	return func(c *config) error {
		c.routingTable.autoRefresh = false
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
	return func(c *config) error {
		c.enableProviders = false
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
	return func(c *config) error {
		c.enableValues = false
		return nil
	}
}

// QueryFilter sets a function that approves which peers may be dialed in a query
func QueryFilter(filter QueryFilterFunc) Option {
	return func(c *config) error {
		c.queryPeerFilter = filter
		return nil
	}
}

// RoutingTableFilter sets a function that approves which peers may be added to the routing table. The host should
// already have at least one connection to the peer under consideration.
func RoutingTableFilter(filter RouteTableFilterFunc) Option {
	return func(c *config) error {
		c.routingTable.peerFilter = filter
		return nil
	}
}

// customProtocols is only to be used for testing. It sets the protocols that the DHT listens on and queries with to be
// the ones passed in. The custom protocols are still augmented by the Prefix.
func customProtocols(protos ...protocol.ID) Option {
	return func(c *config) error {
		c.testProtocols = protos
		return nil
	}
}
