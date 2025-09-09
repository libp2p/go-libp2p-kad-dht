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
}

type PastOperations struct {
	RecordsProvided int `json:"records_provided"`
	KeysProvided    int `json:"key_provided"`
	KeysFailed      int `json:"keys_failed"`

	KeyProvidesPerMinute    float32       `json:"key_provides_per_minute"`
	RegionReprovideDuration time.Duration `json:"reprovide_duration"`
	AvgKeysPerReprovide     float32       `json:"avg_keys_per_reprovide"`
}

type Network struct { // TODO: more fields?
	Peers             int `json:"peers"`
	Reachable         int `json:"reachable"`
	AvgHolders        int `json:"avg_holders"`
	ReplicationFactor int `json:"replication_factor"`
}
