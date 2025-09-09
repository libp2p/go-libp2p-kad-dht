package stats

import (
	"time"

	"github.com/probe-lab/go-libdht/kad/key/bitstr"
)

type Stats struct {
	Closed       bool         `json:"closed"`
	Connectivity Connectivity `json:"connectivity"`
	Queues       Queues       `json:"queues"`
	Schedule     Schedule     `json:"schedule"`
	Workers      Workers      `json:"workers"`
	Timing       Timing       `json:"timing"`
	Operations   Operations   `json:"operations"`
	Network      Network      `json:"network"`
}

type Queues struct {
	PendingKeyProvides      int `json:"pending_key_provides"`
	PendingRegionProvides   int `json:"pending_region_provides"`
	PendingRegionReprovides int `json:"pending_region_reprovides"`
}

type Connectivity struct {
	Status string    `json:"status"`
	Since  time.Time `json:"since"`
}

type Schedule struct {
	Keys                int        `json:"keys"`
	Regions             int        `json:"regions"`
	AvgPrefixLength     int        `json:"avg_prefix_length"`
	NextReprovideAt     time.Time  `json:"next_reprovide_at"`
	NextReprovidePrefix bitstr.Key `json:"next_reprovide_prefix"`
}

type Workers struct {
	Max                      int `json:"max"`
	Active                   int `json:"active"`
	ActivePeriodic           int `json:"active_periodic"`
	ActiveBurst              int `json:"active_burst"`
	DedicatedPeriodic        int `json:"dedicated_periodic"`
	DedicatedBurst           int `json:"dedicated_burst"`
	QueuedPeriodic           int `json:"queued_periodic"`
	QueuedBurst              int `json:"queued_burst"`
	MaxProvideConnsPerWorker int `json:"max_provide_conns_per_worker"`
}

type Timing struct {
	Uptime             time.Duration `json:"uptime"`
	ReprovidesInterval time.Duration `json:"reprovides_interval"`
	CycleStart         time.Time     `json:"cycle_start"`
	CurrentTimeOffset  time.Duration `json:"current_time_offset"`
	MaxReprovideDelay  time.Duration `json:"max_reprovide_delay"`
}

type Operations struct {
	Ongoing OngoingOperations `json:"ongoing"`
	Past    PastOperations    `json:"past"`
}

type OngoingOperations struct {
	RegionProvides   int `json:"region_provides"`
	KeyProvides      int `json:"key_provides"`
	RegionReprovides int `json:"region_reprovides"`
	KeyReprovides    int `json:"key_reprovides"`
}

type PastOperations struct {
	RecordsProvided int `json:"records_provided"` // since provided is running
	KeysProvided    int `json:"key_provided"`     // since provided is running
	KeysFailed      int `json:"keys_failed"`      // since provided is running

	KeysProvidedPerMinute   float64       `json:"keys_provided_per_minute"`   // last cycle
	KeysReprovidedPerMinute float64       `json:"keys_reprovided_per_minute"` // last cycle
	RegionReprovideDuration time.Duration `json:"reprovide_duration"`         // last cycle
	AvgKeysPerReprovide     float64       `json:"avg_keys_per_reprovide"`     // last cycle
}

type Network struct { // TODO: more fields?
	Peers                    int     `json:"peers"` // reprovide only
	CompleteKeyspaceCoverage bool    `json:"complete_keyspace_coverage"`
	Reachable                int     `json:"reachable"` // reprovide only
	AvgHolders               float64 `json:"avg_holders"`
	ReplicationFactor        int     `json:"replication_factor"`
}
