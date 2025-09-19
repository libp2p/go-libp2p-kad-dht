package provider

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/libp2p/go-libp2p-kad-dht/provider/internal/timeseries"
	"github.com/libp2p/go-libp2p-kad-dht/provider/stats"
	"github.com/probe-lab/go-libdht/kad/trie"
)

func (s *SweepingProvider) Stats() stats.Stats {
	now := time.Now()
	snapshot := stats.Stats{
		Closed: s.closed(),
	}

	if snapshot.Closed {
		return snapshot
	}

	// Queue metrics
	snapshot.Queues = stats.Queues{
		PendingKeyProvides:      s.provideQueue.Size(),
		PendingRegionProvides:   s.provideQueue.NumRegions(),
		PendingRegionReprovides: s.reprovideQueue.Size(),
	}

	s.avgPrefixLenLk.Lock()
	avgPrefixLen := s.cachedAvgPrefixLen
	s.avgPrefixLenLk.Unlock()

	// Connectivity status
	var status string
	if s.connectivity.IsOnline() {
		status = "online"
	} else {
		if avgPrefixLen >= 0 {
			status = "disconnected"
		} else {
			status = "offline"
		}
	}
	snapshot.Connectivity = stats.Connectivity{
		Status: status,
		Since:  s.connectivity.LastStateChange(),
	}

	// Schedule information
	s.scheduleLk.Lock()
	scheduleSize := s.schedule.Size()
	nextPrefix := s.scheduleCursor
	ok, nextReprovideOffset := trie.Find(s.schedule, nextPrefix)
	s.scheduleLk.Unlock()

	currentOffset := s.currentTimeOffset()
	nextReprovideAt := time.Time{}
	if ok {
		nextReprovideAt = now.Add(s.timeUntil(nextReprovideOffset))
	}

	keys := -1 // Default value if keyStore.Size() fails
	if keyCount, err := s.keystore.Size(context.Background()); err == nil {
		keys = keyCount
	}
	snapshot.Schedule = stats.Schedule{
		Keys:                keys,
		Regions:             scheduleSize,
		AvgPrefixLength:     avgPrefixLen,
		NextReprovideAt:     nextReprovideAt,
		NextReprovidePrefix: nextPrefix,
	}

	// Worker pool status
	workerStats := s.workerPool.Stats()
	active := 0
	for _, v := range workerStats.Used {
		active += v
	}
	snapshot.Workers = stats.Workers{
		Max:                      workerStats.Max,
		Active:                   active,
		ActivePeriodic:           workerStats.Used[periodicWorker],
		ActiveBurst:              workerStats.Used[burstWorker],
		DedicatedPeriodic:        workerStats.Reserves[periodicWorker],
		DedicatedBurst:           workerStats.Reserves[burstWorker],
		QueuedPeriodic:           workerStats.Queued[periodicWorker],
		QueuedBurst:              workerStats.Queued[burstWorker],
		MaxProvideConnsPerWorker: s.maxProvideConnsPerWorker,
	}

	// Timing information
	snapshot.Timing = stats.Timing{
		Uptime:             time.Since(s.cycleStart),
		ReprovidesInterval: s.reprovideInterval,
		CycleStart:         now.Add(-currentOffset),
		CurrentTimeOffset:  currentOffset,
		MaxReprovideDelay:  s.maxReprovideDelay,
	}

	ongoingOps := stats.OngoingOperations{
		RegionProvides:   int(s.stats.ongoingProvides.opCount.Load()),
		KeyProvides:      int(s.stats.ongoingProvides.keyCount.Load()),
		RegionReprovides: int(s.stats.ongoingReprovides.opCount.Load()),
		KeyReprovides:    int(s.stats.ongoingReprovides.keyCount.Load()),
	}

	// Take snapshots of cycle stats data while holding the lock
	s.stats.cycleStatsLk.Lock()
	s.stats.keysPerReprovide.Cleanup()
	s.stats.reprovideDuration.Cleanup()
	s.stats.peers.Cleanup()
	s.stats.reachable.Cleanup()

	// Capture data for calculations outside the lock
	keysPerProvideSum := s.stats.keysPerProvide.Sum()
	provideDurationSum := s.stats.provideDuration.Sum()
	keysPerReprovideSum := s.stats.keysPerReprovide.Sum()
	reprovideDurationSum := s.stats.reprovideDuration.Sum()
	reprovideDurationAvg := s.stats.reprovideDuration.Avg()
	keysPerReprovideAvg := s.stats.keysPerReprovide.Avg()
	reprovideDurationCount := s.stats.reprovideDuration.Count()
	peersSum := s.stats.peers.Sum()
	peersFullyCovered := s.stats.peers.FullyCovered()
	reachableSum := s.stats.reachable.Sum()
	avgHoldersAvg := s.stats.avgHolders.Avg()

	keysProvidedPerMinute := 0.
	if time.Duration(provideDurationSum) > 0 {
		keysProvidedPerMinute = float64(keysPerProvideSum) / time.Duration(provideDurationSum).Minutes()
	}
	keysReprovidedPerMinute := 0.
	if time.Duration(reprovideDurationSum) > 0 {
		keysReprovidedPerMinute = float64(keysPerReprovideSum) / time.Duration(reprovideDurationSum).Minutes()
	}
	s.stats.cycleStatsLk.Unlock()

	pastOps := stats.PastOperations{
		RecordsProvided: int(s.stats.recordsProvided.Load()),
		KeysProvided:    int(s.stats.keysProvided.Load()),
		KeysFailed:      int(s.stats.keysFailed.Load()),

		KeysProvidedPerMinute:     keysProvidedPerMinute,
		KeysReprovidedPerMinute:   keysReprovidedPerMinute,
		RegionReprovideDuration:   time.Duration(reprovideDurationAvg),
		AvgKeysPerReprovide:       keysPerReprovideAvg,
		RegionReprovidedLastCycle: reprovideDurationCount,
	}

	snapshot.Operations = stats.Operations{
		Ongoing: ongoingOps,
		Past:    pastOps,
	}

	snapshot.Network = stats.Network{ // in the last reprovide cycle
		Peers:                    int(peersSum),
		CompleteKeyspaceCoverage: peersFullyCovered,
		Reachable:                int(reachableSum),
		AvgHolders:               avgHoldersAvg,
		ReplicationFactor:        s.replicationFactor,
	}

	return snapshot
}

// operationStats tracks provider operation metrics over time windows.
type operationStats struct {
	// Cumulative counters since provider started
	recordsProvided atomic.Int32 // total provider records sent
	keysProvided    atomic.Int32 // total keys successfully provided
	keysFailed      atomic.Int32 // total keys that failed to provide

	// Current ongoing operations
	ongoingProvides   ongoingOpStats // active provide operations
	ongoingReprovides ongoingOpStats // active reprovide operations

	// Time-windowed metrics for provide operations
	keysPerProvide  timeseries.IntTimeSeries // keys provided per operation
	provideDuration timeseries.IntTimeSeries // duration of provide operations

	// Time-windowed metrics for reprovide operations (by keyspace region)
	keysPerReprovide  timeseries.CycleStats // keys reprovided per region
	reprovideDuration timeseries.CycleStats // duration of reprovide operations per region

	// Network topology metrics (by keyspace region)
	peers      timeseries.CycleStats      // number of peers per region
	reachable  timeseries.CycleStats      // number of reachable peers per region
	avgHolders timeseries.FloatTimeSeries // average holders per key (weighted)

	cycleStatsLk sync.Mutex // protects cycle-based statistics
}

func newOperationStats(reprovideInterval, maxDelay time.Duration) operationStats {
	return operationStats{
		keysPerProvide:  timeseries.NewIntTimeSeries(reprovideInterval),
		provideDuration: timeseries.NewIntTimeSeries(reprovideInterval),
		avgHolders:      timeseries.NewFloatTimeSeries(reprovideInterval),

		keysPerReprovide:  timeseries.NewCycleStats(reprovideInterval, maxDelay),
		reprovideDuration: timeseries.NewCycleStats(reprovideInterval, maxDelay),
		peers:             timeseries.NewCycleStats(reprovideInterval, maxDelay),
		reachable:         timeseries.NewCycleStats(reprovideInterval, maxDelay),
	}
}

// addProvidedRecords increments the total count of provider records sent.
func (s *operationStats) addProvidedRecords(count int) {
	s.recordsProvided.Add(int32(count))
}

// addCompletedKeys updates the counts of successfully provided and failed keys.
func (s *operationStats) addCompletedKeys(successes, failures int) {
	s.keysProvided.Add(int32(successes))
	s.keysFailed.Add(int32(failures))
}

// ongoingOpStats tracks currently active operations.
type ongoingOpStats struct {
	opCount  atomic.Int32 // number of active operations
	keyCount atomic.Int32 // total keys being processed in active operations
}

// start records the beginning of a new operation with the given number of keys.
func (s *ongoingOpStats) start(keyCount int) {
	s.opCount.Add(1)
	s.keyCount.Add(int32(keyCount))
}

// addKeys adds more keys to the current active operations.
func (s *ongoingOpStats) addKeys(keyCount int) {
	s.keyCount.Add(int32(keyCount))
}

// finish records the completion of an operation and removes its keys from the active count.
func (s *ongoingOpStats) finish(keyCount int) {
	s.opCount.Add(-1)
	s.keyCount.Add(-int32(keyCount))
}
