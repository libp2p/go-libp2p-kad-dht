// Deprecated: Options are now defined in the root package.

package dhtopts

import (
	"time"

	ds "github.com/ipfs/go-datastore"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	record "github.com/libp2p/go-libp2p-record"
)

type Option = dht.Option

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

// Client configures whether or not the DHT operates in client-only mode.
//
// Defaults to false (which is ModeAuto).
// Deprecated: use dht.Mode(ModeClient)
func Client(only bool) dht.Option {
	if only {
		return dht.Mode(dht.ModeClient)
	}
	return dht.Mode(dht.ModeAuto)
}

// Deprecated: use dht.Mode
func Mode(m dht.ModeOpt) dht.Option { return dht.Mode(m) }

// Deprecated: use dht.Validator
func Validator(v record.Validator) dht.Option { return dht.Validator(v) }

// Deprecated: use dht.NamespacedValidator
func NamespacedValidator(ns string, v record.Validator) dht.Option {
	return dht.NamespacedValidator(ns, v)
}

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
