package provider

import (
	"context"
	"time"

	"github.com/libp2p/go-libp2p-kad-dht/provider/stats"
	"github.com/probe-lab/go-libdht/kad/trie"
)

func (s *SweepingProvider) Stats() stats.Stats {
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
	_, nextReprovideAt := trie.Find(s.schedule, nextPrefix)
	s.scheduleLk.Unlock()

	keys, _ := s.keyStore.Size(context.Background())
	currentOffset := s.currentTimeOffset()
	snapshot.Schedule = stats.Schedule{
		Keys:                keys,
		Regions:             scheduleSize,
		AvgPrefixLength:     avgPrefixLen,
		NextReprovideAt:     time.Now().Add(nextReprovideAt - currentOffset),
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
		DedicatedPeriodic:        workerStats.Used[periodicWorker],
		DedicatedBurst:           workerStats.Used[burstWorker],
		QueuedPeriodic:           workerStats.Queued[periodicWorker],
		QueuedBurst:              workerStats.Queued[burstWorker],
		MaxProvideConnsPerWorker: s.maxProvideConnsPerWorker,
	}

	// Timing information
	snapshot.Timing = stats.Timing{
		Uptime:             time.Since(s.cycleStart),
		ReprovidesInterval: s.reprovideInterval,
		CycleStart:         time.Now().Add(-currentOffset),
		CurrentTimeOffset:  currentOffset,
		MaxReprovideDelay:  s.maxReprovideDelay,
	}

	ongoingOps := stats.OngoingOperations{
		RegionProvides:   0, // TODO:
		KeyProvides:      0, // TODO:
		RegionReprovides: 0, // TODO:
	}

	pastOps := stats.PastOperations{
		RecordsProvided: 0, // TODO:
		KeysProvided:    0, // TODO:
		KeysFailed:      0, // TODO:

		KeyProvidesPerMinute:    0, // TODO:
		RegionReprovideDuration: 0, // TODO:
		AvgKeysPerReprovide:     0, // TODO:
	}

	snapshot.Operations = stats.Operations{
		Ongoing: ongoingOps,
		Past:    pastOps,
	}

	snapshot.Network = stats.Network{
		Peers:             0, // TODO: in the last reprovide cycle
		Reachable:         0, // TODO: in the last reprovide cycle
		AvgHolders:        0, // TODO: in the last reprovide cycle
		ReplicationFactor: s.replicationFactor,
	}

	return snapshot
}
