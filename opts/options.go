// Deprecated: Options are now defined in the root package.

package dhtopts

import (
	"time"

	ds "github.com/ipfs/go-datastore"
	"github.com/libp2p/go-libp2p-core/protocol"
	"github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p-record"
)

// Deprecated: The old format did not support more than one message per stream, and is not supported
// or relevant with stream pooling. ProtocolDHT should be used instead.
const ProtocolDHTOld = "/ipfs/dht"

var (
	ProtocolDHT      = dht.ProtocolDHT
	DefaultProtocols = dht.DefaultProtocols
)

// Deprecated: use dht.RoutingTableLatencyTolerance
func RoutingTableLatencyTolerance(latency time.Duration) dht.Option {
	return dht.RoutingTableLatencyTolerance(latency)
}

// Deprecated: use dht.RoutingTableRefreshQueryTimeout
func RoutingTableRefreshQueryTimeout(timeout time.Duration) dht.Option {
	return dht.RoutingTableRefreshQueryTimeout(timeout)
}

// Deprecated: use dht.RoutingTableRefreshPeriod
func RoutingTableRefreshPeriod(period time.Duration) dht.Option {
	return dht.RoutingTableRefreshPeriod(period)
}

// Deprecated: use dht.Datastore
func Datastore(ds ds.Batching) dht.Option { return dht.Datastore(ds) }

// Deprecated: use dht.Client
func Client(only bool) dht.Option { return dht.Client(only) }

// Deprecated: use dht.Mode
func Mode(m dht.ModeOpt) dht.Option { return dht.Mode(m) }

// Deprecated: use dht.Validator
func Validator(v record.Validator) dht.Option { return dht.Validator(v) }

// Deprecated: use dht.NamespacedValidator
func NamespacedValidator(ns string, v record.Validator) dht.Option {
	return dht.NamespacedValidator(ns, v)
}

// Deprecated: use dht.Protocols
func Protocols(protocols ...protocol.ID) dht.Option { return dht.Protocols(protocols...) }

// Deprecated: use dht.BucketSize
func BucketSize(bucketSize int) dht.Option { return dht.BucketSize(bucketSize) }

// Deprecated: use dht.MaxRecordAge
func MaxRecordAge(maxAge time.Duration) dht.Option { return dht.MaxRecordAge(maxAge) }

// Deprecated: use dht.DisableAutoRefresh
func DisableAutoRefresh() dht.Option { return dht.DisableAutoRefresh() }

// Deprecated: use dht.DisableProviders
func DisableProviders() dht.Option { return dht.DisableProviders() }

// Deprecated: use dht.DisableValues
func DisableValues() dht.Option { return dht.DisableValues() }
