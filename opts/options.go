package dhtopts

import (
	"fmt"
	"time"

	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"

	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	record "github.com/libp2p/go-libp2p-record"
)

// Deprecated: The old format did not support more than one message per stream, and is not supported
// or relevant with stream pooling. ProtocolDHT should be used instead.
const ProtocolDHTOld protocol.ID = "/ipfs/dht"

var (
	ProtocolDHT      protocol.ID = "/ipfs/kad/1.0.0"
	DefaultProtocols             = []protocol.ID{ProtocolDHT}
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

// Options is a structure containing all the options that can be used when constructing a DHT.
type Options struct {
	Datastore       ds.Batching
	Validator       record.Validator
	Mode            ModeOpt
	Protocols       []protocol.ID
	BucketSize      int
	DisjointPaths   int
	Concurrency     int
	MaxRecordAge    time.Duration
	EnableProviders bool
	EnableValues    bool
	QueryPeerFilter QueryFilterFunc

	RoutingTable struct {
		RefreshQueryTimeout time.Duration
		RefreshPeriod       time.Duration
		AutoRefresh         bool
		LatencyTolerance    time.Duration
		PeerFilter          RouteTableFilterFunc
	}
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
	o.QueryPeerFilter = emptyQueryFilter

	o.RoutingTable.LatencyTolerance = time.Minute
	o.RoutingTable.RefreshQueryTimeout = 10 * time.Second
	o.RoutingTable.RefreshPeriod = 1 * time.Hour
	o.RoutingTable.AutoRefresh = true
	o.RoutingTable.PeerFilter = emptyRTFilter
	o.MaxRecordAge = time.Hour * 36

	o.BucketSize = 20
	o.Concurrency = 3

	return nil
}

func emptyQueryFilter(h host.Host, ai peer.AddrInfo) bool  { return true }
func emptyRTFilter(h host.Host, conns []network.Conn) bool { return true }

// RoutingTableLatencyTolerance sets the maximum acceptable latency for peers
// in the routing table's cluster.
func RoutingTableLatencyTolerance(latency time.Duration) Option {
	return func(o *Options) error {
		o.RoutingTable.LatencyTolerance = latency
		return nil
	}
}

// RoutingTableRefreshQueryTimeout sets the timeout for routing table refresh
// queries.
func RoutingTableRefreshQueryTimeout(timeout time.Duration) Option {
	return func(o *Options) error {
		o.RoutingTable.RefreshQueryTimeout = timeout
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
	return func(o *Options) error {
		o.RoutingTable.RefreshPeriod = period
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
		if only {
			o.Mode = ModeClient
		}
		return nil
	}
}

// Mode configures which mode the DHT operates in (Client, Server, Auto).
//
// Defaults to ModeAuto.
func Mode(m ModeOpt) Option {
	return func(o *Options) error {
		o.Mode = m
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

// BucketSize configures the bucket size (k in the Kademlia paper) of the routing table.
//
// The default value is 20.
func BucketSize(bucketSize int) Option {
	return func(o *Options) error {
		o.BucketSize = bucketSize
		return nil
	}
}

// Concurrency configures the number of concurrent requests (alpha in the Kademlia paper) for a given query path.
//
// The default value is 3.
func Concurrency(alpha int) Option {
	return func(o *Options) error {
		o.Concurrency = alpha
		return nil
	}
}

// DisjointPaths configures the number of disjoint paths (d in the S/Kademlia paper) taken per query.
//
// The default value is BucketSize/2.
func DisjointPaths(d int) Option {
	return func(o *Options) error {
		o.DisjointPaths = d
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

// DisableValues disables storing and retrieving value records (including
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

// QueryFilter sets a function that approves which peers may be dialed in a query
func QueryFilter(filter QueryFilterFunc) Option {
	return func(o *Options) error {
		o.QueryPeerFilter = filter
		return nil
	}
}

// RoutingTableFilter sets a function that approves which peers may be added to the routing table. The host should
// already have at least one connection to the peer under consideration.
func RoutingTableFilter(filter RouteTableFilterFunc) Option {
	return func(o *Options) error {
		o.RoutingTable.PeerFilter = filter
		return nil
	}
}
