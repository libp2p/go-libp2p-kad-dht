package provider

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gammazero/deque"
	"github.com/libp2p/go-libp2p-kad-dht/provider/internal/keyspace"
	"github.com/libp2p/go-libp2p-kad-dht/provider/stats"
	"github.com/probe-lab/go-libdht/kad/key/bit256"
	"github.com/probe-lab/go-libdht/kad/key/bitstr"
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
		RegionProvides:   int(s.opStats.ongoingProvides.opCount.Load()),
		KeyProvides:      int(s.opStats.ongoingProvides.keyCount.Load()),
		RegionReprovides: int(s.opStats.ongoingReprovides.opCount.Load()),
		KeyReprovides:    int(s.opStats.ongoingReprovides.keyCount.Load()),
	}

	s.opStats.cycleStatsLk.Lock()
	s.opStats.keysPerReprovide.cleanup()
	s.opStats.reprovideDuration.cleanup()
	s.opStats.peers.cleanup()
	s.opStats.reachable.cleanup()

	pastOps := stats.PastOperations{
		RecordsProvided: int(s.opStats.recordsProvided.Load()),
		KeysProvided:    int(s.opStats.keysProvided.Load()),
		KeysFailed:      int(s.opStats.keysFailed.Load()),

		KeysProvidedPerMinute:     float64(s.opStats.keysPerProvide.sum()) / time.Duration(s.opStats.provideDuration.sum()).Minutes(),
		KeysReprovidedPerMinute:   float64(s.opStats.keysPerReprovide.sum()) / time.Duration(s.opStats.reprovideDuration.sum()).Minutes(),
		RegionReprovideDuration:   time.Duration(s.opStats.reprovideDuration.avg()),
		AvgKeysPerReprovide:       s.opStats.keysPerReprovide.avg(),
		RegionReprovidedLastCycle: int(s.opStats.reprovideDuration.len()),
	}

	snapshot.Operations = stats.Operations{
		Ongoing: ongoingOps,
		Past:    pastOps,
	}

	snapshot.Network = stats.Network{ // in the last reprovide cycle
		Peers:                    int(s.opStats.peers.sum()),
		CompleteKeyspaceCoverage: s.opStats.peers.fullyCovered(),
		Reachable:                int(s.opStats.reachable.sum()),
		AvgHolders:               s.opStats.avgHolders.avg(),
		ReplicationFactor:        s.replicationFactor,
	}
	s.opStats.cycleStatsLk.Unlock()

	return snapshot
}

type operationStats struct {
	recordsProvided atomic.Int32
	keysProvided    atomic.Int32
	keysFailed      atomic.Int32

	ongoingProvides   ongoingOpStats
	ongoingReprovides ongoingOpStats

	keysPerProvide    intTimeSeries
	provideDuration   intTimeSeries
	keysPerReprovide  cycleStats
	reprovideDuration cycleStats

	peers      cycleStats
	reachable  cycleStats
	avgHolders floatTimeSeries

	cycleStatsLk sync.Mutex
}

func newOperationStats(reprovideInterval, maxDelay time.Duration) operationStats {
	return operationStats{
		keysPerProvide:  newIntTimeSeries(reprovideInterval),
		provideDuration: newIntTimeSeries(reprovideInterval),
		avgHolders:      newFloatTimeSeries(reprovideInterval),

		keysPerReprovide:  newCycleStats(reprovideInterval, maxDelay),
		reprovideDuration: newCycleStats(reprovideInterval, maxDelay),
		peers:             newCycleStats(reprovideInterval, maxDelay),
		reachable:         newCycleStats(reprovideInterval, maxDelay),
	}
}

func (s *operationStats) providedRecords(count int) {
	s.recordsProvided.Add(int32(count))
}

func (s *operationStats) providedKeys(successes, failures int) {
	s.keysProvided.Add(int32(successes))
	s.keysFailed.Add(int32(failures))
}

type ongoingOpStats struct {
	opCount  atomic.Int32
	keyCount atomic.Int32
}

func (s *ongoingOpStats) start(keyCount int) {
	s.opCount.Add(1)
	s.keyCount.Add(int32(keyCount))
}

func (s *ongoingOpStats) addKeys(keyCount int) {
	s.keyCount.Add(int32(keyCount))
}

func (s *ongoingOpStats) finish(keyCount int) {
	s.opCount.Add(-1)
	s.keyCount.Add(-int32(keyCount))
}

type intTimeEntry struct {
	timestamp time.Time
	value     int64
}

type intTimeSeries struct {
	mutex     sync.Mutex
	data      deque.Deque[intTimeEntry]
	retention time.Duration
}

func newIntTimeSeries(retention time.Duration) intTimeSeries {
	return intTimeSeries{
		data:      deque.Deque[intTimeEntry]{},
		retention: retention,
	}
}

func (t *intTimeSeries) add(value int64) {
	now := time.Now()
	t.mutex.Lock()
	defer t.mutex.Unlock()
	t.data.PushBack(intTimeEntry{timestamp: now, value: value})
	t.gc(now)
}

func (t *intTimeSeries) sum() (sum int64) {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	t.gc(time.Now())

	for i := 0; i < t.data.Len(); i++ {
		sum += t.data.At(i).value
	}
	return sum
}

func (t *intTimeSeries) avg() float64 {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	t.gc(time.Now())

	l := t.data.Len()
	if l == 0 {
		return 0
	}
	return float64(t.sum()) / float64(l)
}

// mutex should be held
func (t *intTimeSeries) gc(now time.Time) {
	cutoff := now.Add(-t.retention)
	for t.data.Len() > 0 {
		if t.data.Front().timestamp.Before(cutoff) {
			t.data.PopFront()
		} else {
			break
		}
	}
}

type floatTimeEntry struct {
	timestamp time.Time
	value     float64
	weight    int
}

type floatTimeSeries struct {
	mutex     sync.Mutex
	data      deque.Deque[floatTimeEntry]
	retention time.Duration
}

func newFloatTimeSeries(retention time.Duration) floatTimeSeries {
	return floatTimeSeries{
		data:      deque.Deque[floatTimeEntry]{},
		retention: retention,
	}
}

func (t *floatTimeSeries) add(value float64, weight int) {
	now := time.Now()
	t.mutex.Lock()
	defer t.mutex.Unlock()
	t.data.PushBack(floatTimeEntry{timestamp: now, value: value, weight: weight})
	t.gc(now)
}

func (t *floatTimeSeries) sum() float64 {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	t.gc(time.Now())

	var sum float64
	for i := 0; i < t.data.Len(); i++ {
		e := t.data.At(i)
		sum += e.value * float64(e.weight)
	}
	return sum
}

func (t *floatTimeSeries) avg() float64 {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	t.gc(time.Now())

	l := t.data.Len()
	if l == 0 {
		return 0
	}
	return t.sum() / float64(l)
}

// mutex should be held
func (t *floatTimeSeries) gc(now time.Time) {
	cutoff := now.Add(-t.retention)
	for t.data.Len() > 0 {
		if t.data.Front().timestamp.Before(cutoff) {
			t.data.PopFront()
		} else {
			break
		}
	}
}

type entry struct {
	time time.Time
	val  int64
}

type cycleStats struct {
	tr *trie.Trie[bitstr.Key, entry]

	queue *trie.Trie[bitstr.Key, entry]

	ttl, maxDelay time.Duration
}

func newCycleStats(ttl, maxDelay time.Duration) cycleStats {
	return cycleStats{
		tr:       trie.New[bitstr.Key, entry](),
		queue:    trie.New[bitstr.Key, entry](),
		ttl:      ttl,
		maxDelay: maxDelay,
	}
}

func (s *cycleStats) cleanup() {
	allEntries := keyspace.AllEntries(s.tr, bit256.ZeroKey())
	for _, e := range allEntries {
		if e.Data.time.Add(s.ttl + s.maxDelay).Before(time.Now()) {
			s.tr.Remove(e.Key)
			if subtrie, ok := keyspace.FindSubtrie(s.queue, e.Key); ok {
				for _, qe := range keyspace.AllEntries(subtrie, bit256.ZeroKey()) {
					s.tr.Add(qe.Key, qe.Data)
				}
			}
		}
	}
}

func (s *cycleStats) add(prefix bitstr.Key, val int64) {
	e := entry{time: time.Now(), val: val}
	if _, ok := keyspace.FindSubtrie(s.tr, prefix); ok {
		// shorter prefix
		keyspace.PruneSubtrie(s.tr, prefix)
		s.tr.Add(prefix, e)
		return
	}
	// longer prefix, group with complements before replacing
	target, ok := keyspace.FindPrefixOfKey(s.tr, prefix)
	if !ok {
		// No keys in s.tr is a prefix of `prefix`
		s.tr.Add(prefix, e)
		return
	}

	if queuePrefix, ok := keyspace.FindPrefixOfKey(s.queue, prefix); ok {
		_, entry := trie.Find(s.queue, queuePrefix)
		if time.Since(entry.time) < s.maxDelay {
			// A recent entry is a superset of the current one, skip.
			return
		}
		// Remove old entry
		keyspace.PruneSubtrie(s.queue, queuePrefix)
	} else {
		// Remove (older) superstrings from queue
		keyspace.PruneSubtrie(s.queue, prefix)
	}
	// Add prefix to queue
	s.queue.Add(prefix, e)

	subtrie, ok := keyspace.FindSubtrie(s.queue, target)
	if !ok || !keyspace.KeyspaceCovered(subtrie) {
		// Subtrie not complete
		return
	}
	// Target keyspace is fully covered by queue entries. Replace target with
	// queue entries.
	keyspace.PruneSubtrie(s.tr, target)
	for _, e := range keyspace.AllEntries(subtrie, bit256.ZeroKey()) {
		s.tr.Add(e.Key, e.Data)
	}
}

func (s *cycleStats) sum() (sum int64) {
	for _, v := range keyspace.AllValues(s.tr, bit256.ZeroKey()) {
		sum += v.val
	}
	return sum
}

func (s *cycleStats) avg() float64 {
	allValues := keyspace.AllValues(s.tr, bit256.ZeroKey())
	var sum int64
	for _, v := range allValues {
		sum += v.val
	}
	return float64(sum) / float64(len(allValues))
}

func (s *cycleStats) len() int {
	return s.tr.Size()
}

func (s *cycleStats) fullyCovered() bool {
	return keyspace.KeyspaceCovered(s.tr)
}
