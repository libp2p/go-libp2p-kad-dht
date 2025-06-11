package reprovider

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/filecoin-project/go-clock"
	pool "github.com/guillaumemichel/reservedpool"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"

	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p-kad-dht/internal/net"
	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
	kb "github.com/libp2p/go-libp2p-kbucket"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	mh "github.com/multiformats/go-multihash"

	"github.com/probe-lab/go-libdht/kad/key"
	"github.com/probe-lab/go-libdht/kad/key/bit256"
	"github.com/probe-lab/go-libdht/kad/key/bitstr"
	"github.com/probe-lab/go-libdht/kad/trie"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

// Provider announces blocks to the network
type Provider interface {
	// Provide takes a cid and makes an attempt to announce it to the network
	Provide(context.Context, cid.Cid, bool) error
}

type ProvideMany interface {
	ProvideMany(ctx context.Context, keys []mh.Multihash) error
}

var (
	_ Provider    = &SweepingReprovider{}
	_ ProvideMany = &SweepingReprovider{}
)

type KadClosestPeersRouter interface {
	GetClosestPeers(context.Context, string) ([]peer.ID, error)
}

var logger = logging.Logger("dht/SweepingReprovider")

var (
	ErrNodeOffline                        = errors.New("reprovider: node is offline")
	ErrNoKeyProvided                      = errors.New("reprovider: failed to provide any key")
	errTooManyIterationsDuringExploration = errors.New("closestPeersToPrefix needed more than maxPrefixSearches iterations")
)

const keyLen = bit256.KeyLen * 8 // 256

// TODO: support resuming reprovide service after a restart
// persist cids to disk
// persist prefixes waiting to be reprovided and last reprovided prefix (with time) on disk

type workerType uint8

const (
	periodicWorker workerType = iota
	burstWorker
)

type provideReq struct {
	ctx  context.Context
	cids []mh.Multihash
	done chan<- error
}

type resetCidsReq struct {
	ctx  context.Context
	cids <-chan mh.Multihash
	done chan<- error
}

type SweepingReprovider struct {
	ctx    context.Context
	peerid peer.ID
	router KadClosestPeersRouter
	order  bit256.Key

	connectivity connectivityChecker

	workerPool               *pool.Pool[workerType]
	maxProvideConnsPerWorker int

	replicationFactor int
	clock             clock.Clock
	cycleStart        time.Time
	reprovideInterval time.Duration
	maxReprovideDelay time.Duration

	avgPrefixLenLk       sync.Mutex
	cachedAvgPrefixLen   int
	lastAvgPrefixLen     time.Time
	avgPrefixLenValidity time.Duration

	cids          *trie.Trie[bit256.Key, mh.Multihash]
	resetCidsChan chan resetCidsReq

	provideChan            chan provideReq
	schedule               *trie.Trie[bitstr.Key, time.Duration]
	scheduleLk             sync.Mutex
	scheduleTimer          *clock.Timer
	scheduleTimerStartedAt time.Time

	failedRegionsChan  chan bitstr.Key
	lateRegionsQueue   []bitstr.Key
	pendingCids        []mh.Multihash
	pendingCidsChan    chan []mh.Multihash
	catchupInProgress  atomic.Bool
	catchupPendingChan chan struct{}

	prefixCursor        bitstr.Key
	ongoingReprovides   *trie.Trie[bitstr.Key, struct{}]
	ongoingReprovidesLk sync.Mutex

	msgSender      pb.MessageSender
	getSelfAddrs   func() []ma.Multiaddr
	addLocalRecord func(mh.Multihash) error

	provideCounter metric.Int64Counter
}

func NewReprovider(ctx context.Context, opts ...Option) (*SweepingReprovider, error) {
	cfg := DefaultConfig
	err := cfg.apply(opts...)
	if err != nil {
		return nil, err
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	meter := otel.Meter("github.com/libp2p/go-libp2p-kad-dht/reprovider")
	providerCounter, err := meter.Int64Counter(
		"total_provide_count",
		metric.WithDescription("Number of successful provides since node is running"),
	)
	if err != nil {
		return nil, err
	}
	reprovider := &SweepingReprovider{
		ctx:    ctx,
		router: cfg.router,
		peerid: cfg.peerid,
		order:  peerIDToBit256(cfg.peerid),

		replicationFactor: cfg.replicationFactor,
		reprovideInterval: cfg.reprovideInterval,
		maxReprovideDelay: cfg.maxReprovideDelay,

		connectivity: connectivityChecker{
			ctx:                  ctx,
			clock:                cfg.clock,
			onlineCheckInterval:  cfg.connectivityCheckOnlineInterval,
			offlineCheckInterval: cfg.connectivityCheckOfflineInterval,
			checkFunc: func() bool {
				peers, err := cfg.router.GetClosestPeers(ctx, string(cfg.peerid))
				return err == nil && len(peers) > 0
			},
		},
		workerPool: pool.New(cfg.maxWorkers, map[workerType]int{
			periodicWorker: cfg.dedicatedPeriodicWorkers,
			burstWorker:    cfg.dedicatedBurstWorkers,
		}),
		maxProvideConnsPerWorker: cfg.maxProvideConnsPerWorker,

		avgPrefixLenValidity: 5 * time.Minute,

		clock:      cfg.clock,
		cycleStart: cfg.clock.Now(),

		msgSender:      cfg.msgSender,
		getSelfAddrs:   cfg.selfAddrs,
		addLocalRecord: cfg.addLocalRecord,

		cids:          trie.New[bit256.Key, mh.Multihash](),
		resetCidsChan: make(chan resetCidsReq, 1),

		provideChan:   make(chan provideReq),
		schedule:      trie.New[bitstr.Key, time.Duration](),
		scheduleTimer: cfg.clock.Timer(time.Hour),

		failedRegionsChan:  make(chan bitstr.Key, 1),
		pendingCidsChan:    make(chan []mh.Multihash),
		catchupPendingChan: make(chan struct{}, 1),

		ongoingReprovides: trie.New[bitstr.Key, struct{}](),

		provideCounter: providerCounter,
	}
	// Don't need to start schedule timer yet
	reprovider.scheduleTimer.Stop()

	// Connectivity checker starts in online state
	reprovider.connectivity.online.Store(true)
	reprovider.connectivity.backOnlineNotify = reprovider.catchupPendingNotify

	go reprovider.run()

	return reprovider, nil
}

func NewDHTReprovider(dht *dht.IpfsDHT, opts ...Option) (*SweepingReprovider, error) {
	opts = append([]Option{
		WithPeerID(dht.Host().ID()),
		WithRouter(dht),
		WithSelfAddrs(dht.FilteredAddrs),
		WithMessageSender(net.NewMessageSenderImpl(dht.Host(), dht.Protocols())),
		WithAddLocalRecord(func(h mh.Multihash) error {
			return dht.Provide(dht.Context(), cid.NewCidV0(h), false)
		}),
	}, opts...,
	)
	return NewReprovider(dht.Context(), opts...)
}

func (s *SweepingReprovider) run() {
	logger.Debug("Starting SweepingReprovider")
	s.measureInitialPrefixLen()
	defer s.scheduleTimer.Stop()
	defer s.workerPool.Close()

	retryTicker := time.NewTicker(time.Minute)
	defer retryTicker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			// TODO: gracefully shutdown
			return
		case provideRequest := <-s.provideChan:
			s.handleProvide(provideRequest)
		case <-s.scheduleTimer.C:
			s.handleReprovide()
		case prefix := <-s.failedRegionsChan:
			if !slices.Contains(s.lateRegionsQueue, prefix) {
				s.lateRegionsQueue = append(s.lateRegionsQueue, prefix)
			}
			s.connectivity.triggerCheck()
		case cids := <-s.pendingCidsChan:
			s.pendingCids = append(s.pendingCids, cids...)
			s.connectivity.triggerCheck()
		case <-retryTicker.C:
			s.catchupPendingNotify()
		case <-s.catchupPendingChan:
			s.catchupPendingWork()
		case req := <-s.resetCidsChan:
			s.resetCids(req)
		}
	}
}

const initialGetClosestPeers = 4

// measureInitialPrefixLen makes a few GetClosestPeers calls to get an estimate
// of the prefix length to be used in the network.
//
// This function blocks until GetClosestPeers succeeds.
func (s *SweepingReprovider) measureInitialPrefixLen() {
	cplSum := atomic.Int32{}
	cplSamples := atomic.Int32{}
	wg := sync.WaitGroup{}
	wg.Add(initialGetClosestPeers)
	for range initialGetClosestPeers {
		go func() {
			defer wg.Done()
			bytes := [32]byte{}
			rand.Read(bytes[:])
			for {
				peers, err := s.router.GetClosestPeers(s.ctx, string(bytes[:]))
				if err == nil && len(peers) > 0 {
					if len(peers) <= 2 {
						return // Ignore result if only 2 other peers in DHT.
					}
					cpl := keyLen
					firstPeerKey := peerIDToBit256(peers[0])
					for _, p := range peers[1:] {
						cpl = min(cpl, key.CommonPrefixLength(firstPeerKey, peerIDToBit256(p)))
					}
					cplSum.Add(int32(cpl))
					cplSamples.Add(1)
					return
				}
				s.clock.Sleep(500 * time.Millisecond)
			}
		}()
	}
	wg.Wait()

	nSamples := cplSamples.Load()
	if nSamples == 0 {
		s.cachedAvgPrefixLen = 0
	} else {
		s.cachedAvgPrefixLen = int(cplSum.Load() / nSamples)
	}
	logger.Debugf("initial avgPrefixLen is %d", s.cachedAvgPrefixLen)
	s.lastAvgPrefixLen = s.clock.Now()
}

func (s *SweepingReprovider) addPrefixToScheduleNoLock(prefix bitstr.Key) {
	reprovideTime := s.reprovideTimeForPrefix(prefix)
	s.schedule.Add(prefix, reprovideTime)

	currentTimeOffset := s.currentTimeOffset()
	timeUntilReprovide := s.timeBetween(currentTimeOffset, reprovideTime)
	if s.prefixCursor == "" {
		s.scheduleNextReprovideNoLock(prefix, timeUntilReprovide)
	} else {
		followingKey := nextNonEmptyLeaf(s.schedule, prefix, s.order).Key
		if s.prefixCursor == followingKey {
			found, scheduledAlarm := trie.Find(s.schedule, s.prefixCursor)
			if !found {
				s.scheduleNextReprovideNoLock(prefix, timeUntilReprovide)
			} else {
				timeUntilScheduledAlarm := s.timeBetween(currentTimeOffset, scheduledAlarm)
				if timeUntilReprovide < timeUntilScheduledAlarm {
					s.scheduleNextReprovideNoLock(prefix, timeUntilReprovide)
				}
			}
		}
	}
}

func (s *SweepingReprovider) scheduleNextReprovideNoLock(prefix bitstr.Key, timeUntilReprovide time.Duration) {
	s.prefixCursor = prefix
	s.scheduleTimer.Reset(timeUntilReprovide)
	s.scheduleTimerStartedAt = s.clock.Now()
}

func (s *SweepingReprovider) getPrefixesForCids(cids []mh.Multihash) map[bitstr.Key]*trie.Trie[bit256.Key, mh.Multihash] {
	prefixes := make(map[bitstr.Key]*trie.Trie[bit256.Key, mh.Multihash])
	s.scheduleLk.Lock()
	defer s.scheduleLk.Unlock()
	for _, c := range cids {
		k := mhToBit256(c)
		scheduled, prefix := trieHasPrefixOfKey(s.schedule, k)
		if scheduled {
			if _, ok := prefixes[prefix]; !ok {
				prefixes[prefix] = trie.New[bit256.Key, mh.Multihash]()
			}
			prefixes[prefix].Add(k, c)
		}
	}
	return prefixes
}

func (s *SweepingReprovider) handleProvide(provideRequest provideReq) {
	if len(provideRequest.cids) == 0 {
		provideRequest.done <- nil
		return
	}
	prefixes, newCidsCount := s.addCids(provideRequest.cids)

	if len(prefixes) == 0 {
		// All cids are already tracked and have been provided.
		provideRequest.done <- nil
		return
	}

	if !s.connectivity.isOnline() {
		logger.Warn("Node is offline, cannot provide requested cids.")
		cids := make([]mh.Multihash, len(provideRequest.cids))
		for _, t := range prefixes {
			cids = append(cids, allValues(t, s.order)...)
		}
		s.pendingCids = append(s.pendingCids, cids...)
		provideRequest.done <- ErrNodeOffline
		return
	}

	logger.Debugf("Providing %d new cids with %d different prefixes", newCidsCount, len(prefixes))
	go func() {
		err := s.networkProvide(prefixes)
		provideRequest.done <- err
	}()
}

// addCids adds the provided cids to the reprovider tracked cids, and add the
// appropriate prefix to the schedule if cid isn't covered by a prefix already
// in the schedule.
//
// It returns a map of prefixes to fresh cids (that weren't tracked so far).
func (s *SweepingReprovider) addCids(cids []mh.Multihash) (map[bitstr.Key]*trie.Trie[bit256.Key, mh.Multihash], int) {
	prefixes := make(map[bitstr.Key]*trie.Trie[bit256.Key, mh.Multihash])
	count := 0
	var avgPrefixLen int
	s.scheduleLk.Lock()
	defer s.scheduleLk.Unlock()
	for _, c := range cids {
		prefixConsolidation := false
		k := mhToBit256(c)
		// Add cid to s.cids if not there already, and add fresh cids to newCids.
		// Deduplication is handled here.
		if added := s.cids.Add(k, c); added {
			count++
			// If prefix of c not scheduled yet, add it to the schedule.
			scheduled, prefix := trieHasPrefixOfKey(s.schedule, k)
			if !scheduled {
				if avgPrefixLen == 0 {
					avgPrefixLen = s.getAvgPrefixLenNoLock()
				}
				prefix = bitstr.Key(key.BitString(k)[:avgPrefixLen])
				if subtrie, ok := subtrieMatchingPrefix(s.schedule, prefix); ok {
					// If generated prefix is a prefix of existing scheduled keyspace
					// zones, consolidate these zones around the shorter prefix.
					for _, entry := range allEntries(subtrie, s.order) {
						s.schedule.Remove(entry.Key)
					}
					prefixConsolidation = true
				}
				s.addPrefixToScheduleNoLock(prefix)
			}

			if _, ok := prefixes[prefix]; !ok {
				prefixes[prefix] = trie.New[bit256.Key, mh.Multihash]()
			}
			prefixes[prefix].Add(k, c)
			if prefixConsolidation {
				// prefix is a shorted prefix of a prefix already in the schedule.
				// Consolidate everything into prefix.
				for p, t := range prefixes {
					if isBitstrPrefix(p, prefix) {
						for _, entry := range allEntries(t, s.order) {
							prefixes[prefix].Add(entry.Key, entry.Data)
						}
						delete(prefixes, p)
					}
				}
			}
		}
	}
	return prefixes, count
}

func (s *SweepingReprovider) networkProvide(prefixes map[bitstr.Key]*trie.Trie[bit256.Key, mh.Multihash]) error {
	if len(prefixes) == 1 {
		for prefix, cid := range prefixes {
			// Provide a single cid (NOT ProvideMany)
			if cid.Size() == 1 {
				return s.provideForPrefix(prefix, cid, initialProvide)
			}
		}
	}

	sortedPrefixes := sortPrefixesBySize(prefixes)

	// Atomic flag: 0 = no cids successfully provided yet, 1 = at least one
	// success
	var anySuccess uint32
	wg := sync.WaitGroup{}

	onlineTicker := s.clock.Ticker(5 * time.Second)
	defer onlineTicker.Stop()

	cancelWorkers := atomic.Bool{}
	workerAcquiredChan := make(chan struct{}, 1)
	acquireWorkerAsync := func() {
		if err := s.workerPool.Acquire(burstWorker); err == nil {
			if cancelWorkers.Load() {
				s.workerPool.Release(burstWorker)
			} else {
				workerAcquiredChan <- struct{}{}
			}
		}
	}

	i := 0
	go acquireWorkerAsync()
workerLoop:
	for {
		select {
		case <-workerAcquiredChan:
			wg.Add(1)
			go func(p prefixAndCids, i int) {
				defer wg.Done()
				defer s.workerPool.Release(burstWorker)

				if err := s.provideForPrefix(p.prefix, p.cids, initialProvide); err != nil {
					logger.Warn(err)
				} else {
					// Track if we were able to provide at least one cid.
					atomic.StoreUint32(&anySuccess, 1)
				}
			}(sortedPrefixes[i], i)
			i++
			if i == len(sortedPrefixes) {
				break workerLoop
			}
			// Try to acquire a worker for the next prefix.
			go acquireWorkerAsync()
		case <-onlineTicker.C:
			if !s.connectivity.isOnline() {
				cancelWorkers.Store(true)
				break workerLoop
			}
		case <-s.ctx.Done():
			// TODO: save pending cids to disk to exit gracefully
			cancelWorkers.Store(true)
			break workerLoop
		}
	}

	if i != len(sortedPrefixes) {
		// Add cids that haven't been provided yet to the pending queue.
		cids := make([]mh.Multihash, 0)
		for _, prefix := range sortedPrefixes[i:] {
			cids = append(cids, allValues(prefix.cids, s.order)...)
		}
		s.pendingCidsChan <- cids
	} else {
		// Wait for workers to finish before reporting completion.
		wg.Wait()
	}

	if atomic.LoadUint32(&anySuccess) == 0 {
		return ErrNoKeyProvided
	}
	return nil
}

func (s *SweepingReprovider) handleReprovide() {
	online := s.connectivity.isOnline()
	s.scheduleLk.Lock()
	currentPrefix := s.prefixCursor
	// Get next prefix to reprovide, and set timer for it.
	next := nextNonEmptyLeaf(s.schedule, currentPrefix, s.order)

	if next == nil {
		// Schedule is empty, don't reprovide anything.
		s.scheduleLk.Unlock()
		return
	}

	currentTimeOffset := s.currentTimeOffset()
	var nextPrefix bitstr.Key
	var timeUntilNextReprovide time.Duration
	if next.Key == currentPrefix {
		// There is a single prefix in the schedule.
		nextPrefix = currentPrefix
		timeUntilNextReprovide = s.timeBetween(currentTimeOffset, s.reprovideTimeForPrefix(currentPrefix))
	} else {
		timeSinceTimerRunning := s.timeBetween(s.timeOffset(s.scheduleTimerStartedAt), currentTimeOffset)
		timeSinceTimerUntilNext := s.timeBetween(s.timeOffset(s.scheduleTimerStartedAt), next.Data)

		if s.scheduleTimerStartedAt.Add(s.reprovideInterval).Before(s.clock.Now()) {
			// Alarm was programmed more than reprovideInterval ago, which means that
			// no regions has been reprovided since. Add all regions to
			// failedRegionsChan. This only happens if the main thread gets blocked
			// for more than reprovideInterval.
			nextKeyFound := false
			scheduleEntries := allEntries(s.schedule, s.order)
			for _, entry := range scheduleEntries {
				if !nextKeyFound && entry.Data > currentTimeOffset {
					next = entry
					nextKeyFound = true
				}
				prefix := entry.Key
				if !slices.Contains(s.lateRegionsQueue, prefix) {
					s.lateRegionsQueue = append(s.lateRegionsQueue, prefix)
				}
			}
			if !nextKeyFound {
				next = scheduleEntries[0]
			}
			timeUntilNextReprovide = s.timeBetween(currentTimeOffset, next.Data)
			// Don't reprovide any region now, but schedule the next one. All regions
			// are expected to be reprovided when the provider is catching up with
			// failed regions.
			s.scheduleNextReprovideNoLock(next.Key, timeUntilNextReprovide)
			s.scheduleLk.Unlock()
			s.connectivity.triggerCheck()
			return
		} else if timeSinceTimerUntilNext < timeSinceTimerRunning {
			// next is scheduled in the past. While next is in the past, add next to
			// failedRegions and take nextLeaf as next.

			count := 0
			scheduleSize := s.schedule.Size()
			for timeSinceTimerUntilNext < timeSinceTimerRunning && count < scheduleSize {
				prefix := next.Key
				if !slices.Contains(s.lateRegionsQueue, prefix) {
					s.lateRegionsQueue = append(s.lateRegionsQueue, prefix)
				}
				next = nextNonEmptyLeaf(s.schedule, next.Key, s.order)
				timeSinceTimerUntilNext = s.timeBetween(s.timeOffset(s.scheduleTimerStartedAt), next.Data)
				count++
			}
			s.connectivity.triggerCheck()
			if count == scheduleSize {
				// No reprovides are scheduled in the future, return here.
				s.scheduleLk.Unlock()
				return
			}
		}
		// next is in the future
		nextPrefix = next.Key
		timeUntilNextReprovide = s.timeBetween(currentTimeOffset, next.Data)
	}

	s.scheduleNextReprovideNoLock(nextPrefix, timeUntilNextReprovide)
	s.scheduleLk.Unlock()

	// If we are offline, don't even try to reprovide region.
	if !online {
		s.failedRegionsChan <- currentPrefix
		return
	}

	// Remove prefix that is about to be reprovided from the late regions queue
	// if present.
	for i, r := range s.lateRegionsQueue {
		if r == currentPrefix {
			s.lateRegionsQueue = slices.Delete(s.lateRegionsQueue, i, i+1)
			break
		}
	}

	cids := s.cids.Copy() // NOTE: if many cids, this may have a large memory footprint
	go func() {
		s.workerPool.Acquire(periodicWorker)
		defer s.workerPool.Release(periodicWorker)
		s.provideForPrefix(currentPrefix, cids, periodicReprovide)
	}()
}

type provideType uint8

const (
	periodicReprovide provideType = iota
	lateRegionReprovide
	initialProvide
)

func (s *SweepingReprovider) handleProvideError(prefix bitstr.Key, cids *trie.Trie[bit256.Key, mh.Multihash], op provideType) {
	switch op {
	case initialProvide:
		s.pendingCidsChan <- allValues(cids, s.order)
	case periodicReprovide, lateRegionReprovide:
		s.failedRegionsChan <- prefix
	}
	// We should schedule the next reprovide even if there was an error during
	// the reprovide. Otherwise the schedule for this region will deviate.
	s.scheduleNextReprovide(prefix, s.currentTimeOffset(), op)
}

func (s *SweepingReprovider) provideForPrefix(prefix bitstr.Key, cids *trie.Trie[bit256.Key, mh.Multihash], op provideType) error {
	subtrie, ok := subtrieMatchingPrefix(cids, prefix)
	if !ok {
		// There was no cid matching prefix to provide
		return nil
	}
	selfAddrs := s.getSelfAddrs()
	if len(selfAddrs) == 0 {
		return errors.New("no self addresses available for providing cids")
	}
	if subtrie.Size() <= 2 {
		// Region has 1 or 2 cids, it is more optimized to provide them naively.
		cidsSlice := allValues(subtrie, s.order)
		return s.individualProvideForPrefix(s.ctx, prefix, cidsSlice, op)
	}
	logger.Debugf("Starting to explore prefix %s for %d matching cids", prefix, subtrie.Size())
	peers, explorationErr := s.closestPeersToPrefix(prefix)
	if explorationErr != nil {
		if explorationErr != errTooManyIterationsDuringExploration {
			s.handleProvideError(prefix, subtrie, op)
			return explorationErr
		}
		logger.Errorf("prefix key exploration not complete: %s", prefix)
	}
	if len(peers) == 0 {
		err := errors.New("no peers found when exploring prefix " + string(prefix))
		s.handleProvideError(prefix, subtrie, op)
		return err
	}

	regions, coveredPrefix := s.regionsFromPeers(peers, cids)
	logger.Debugf("requested prefix: %s (len %d), prefix covered: %s (len %d)", prefix, len(prefix), coveredPrefix, len(coveredPrefix))

	claimRegions := (op == periodicReprovide || op == lateRegionReprovide) && explorationErr != errTooManyIterationsDuringExploration
	if claimRegions {
		// When reproviding a region, remove all scheduled regions starting with
		// the currently covered prefix.
		s.unscheduleSubsumedPrefixes(coveredPrefix)

		// NOTE: same can be done for provideMany, this would remove incentive to find initial prefix len.
		// region reprovide should unschedule provieMany, but not the opposite

		// Claim the regions to be reprovided, so that no other thread will try to
		// reprovdie them concurrently.
		regions = s.claimRegionReprovide(regions)
	}

	errCount := 0
	addrInfo := peer.AddrInfo{ID: s.peerid, Addrs: selfAddrs}
	for _, r := range regions {
		if r.cids.Size() == 0 {
			if claimRegions {
				s.releaseRegionReprovide(r.prefix)
			}
			continue
		}
		// Add keys to local provider store
		for _, entry := range allEntries(r.cids, s.order) {
			s.addLocalRecord(entry.Data)
		}
		cidsAllocations := s.cidsAllocationsToPeers(r)
		err := s.sendProviderRecords(cidsAllocations, addrInfo)
		if claimRegions {
			s.releaseRegionReprovide(r.prefix)
		}
		if err != nil {
			logger.Warn(err, ": region ", r.prefix)
			errCount++
			s.handleProvideError(r.prefix, r.cids, op)
			continue
		}
		s.provideCounter.Add(s.ctx, int64(r.cids.Size()))

		s.scheduleNextReprovide(r.prefix, s.currentTimeOffset(), op)
		if op == periodicReprovide || op == lateRegionReprovide {
			// TODO: persist to datastore that region identified by prefix was reprovided `now`, only during periodic reprovides?
		}
	}
	// If at least 1 regions was provided, we don't consider it a failure.
	if errCount == len(regions) {
		return errors.New("failed to provide any region")
	}
	return nil
}

func (s *SweepingReprovider) individualProvideForPrefix(ctx context.Context, prefix bitstr.Key, cids []mh.Multihash, op provideType) error {
	if len(cids) == 0 {
		return nil
	}

	fullRegionReprovide := op == periodicReprovide || op == lateRegionReprovide
	var provideErr error

	if len(cids) == 1 {
		coveredPrefix, err := s.vanillaProvide(ctx, cids[0])
		if err != nil && !fullRegionReprovide {
			s.pendingCidsChan <- cids
		}
		provideErr = err
		s.scheduleNextReprovide(coveredPrefix, s.currentTimeOffset(), op)
	} else {
		wg := sync.WaitGroup{}
		success := atomic.Bool{}
		for _, cid := range cids {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, err := s.vanillaProvide(ctx, cid)
				if err == nil {
					success.Store(true)
				} else if !fullRegionReprovide {
					// Individual provide failed
					s.pendingCidsChan <- []mh.Multihash{cid}
				}
			}()
		}
		wg.Wait()

		if !success.Load() {
			provideErr = errors.New("all individual provides failed for prefix " + string(prefix))
		}
		s.scheduleNextReprovide(prefix, s.currentTimeOffset(), op)
	}
	if fullRegionReprovide {
		// TODO: persist to datastore that region identified by prefix was reprovided `now`
		if provideErr != nil {
			s.failedRegionsChan <- prefix
		}
	}
	return provideErr
}

// vanillaProvide provides a single cid to the network without any
// optimization. It should be used for providing a small number of cids
// (typically 1 or 2), because exploring the keyspace would add too much
// overhead for a small number of cids.
func (s *SweepingReprovider) vanillaProvide(ctx context.Context, k mh.Multihash) (bitstr.Key, error) {
	// Add provider record to local provider store.
	s.addLocalRecord(k)
	// Get peers to which the record will be allocated.
	peers, err := s.router.GetClosestPeers(ctx, string(k))
	if err != nil {
		return "", err
	}
	coveredPrefix, _ := shortestCoveredPrefix(bitstr.Key(key.BitString(mhToBit256(k))), peers)
	addrInfo := peer.AddrInfo{ID: s.peerid, Addrs: s.getSelfAddrs()}
	cidsAllocations := make(map[peer.ID][]mh.Multihash)
	for _, p := range peers {
		cidsAllocations[p] = []mh.Multihash{k}
	}
	return coveredPrefix, s.sendProviderRecords(cidsAllocations, addrInfo)
}

// closestPeersToPrefix returns more than s.replicationFactor peers
// corresponding to the branch of the network peers trie matching the provided
// prefix. In the case there aren't enough peers matching the provided prefix,
// it will find and return the closest peers to the prefix, even if they don't
// exactly match it.
func (s *SweepingReprovider) closestPeersToPrefix(prefix bitstr.Key) ([]peer.ID, error) {
	allClosestPeers := make(map[peer.ID]struct{}, 2*s.replicationFactor)

	maxPrefixSearches := 64
	nextPrefix := prefix
	startTime := time.Now()
	coveredPrefixesStack := []bitstr.Key{}
	var closestPeers []peer.ID
	var err error

	earlyExit := false
	i := 0
	// Go down the trie to fully cover prefix.
explorationLoop:
	for i < maxPrefixSearches {
		if !s.connectivity.isOnline() {
			err = ErrNodeOffline
			earlyExit = true
			break explorationLoop
		}
		i++
		fullKey := firstFullKeyWithPrefix(nextPrefix, s.order)
		closestPeers, err = s.closestPeersToKey(fullKey)
		peerStrs := ""
		for _, p := range closestPeers {
			peerStrs += key.BitString(peerIDToBit256(p))[:8] + ", "
		}
		if err != nil {
			// We only get an err if something really bad happened, e.g no peers in
			// routing table, invalid key, etc.
			earlyExit = true
			break explorationLoop
		}
		if len(closestPeers) == 0 {
			err = errors.New("dht lookup didn't return any peers")
			earlyExit = true
			break explorationLoop
		}
		coveredPrefix, coveredPeers := shortestCoveredPrefix(fullKey, closestPeers)
		for _, p := range coveredPeers {
			allClosestPeers[p] = struct{}{}
		}

		coveredPrefixLen := len(coveredPrefix)
		if i == 1 {
			if coveredPrefixLen <= len(prefix) && coveredPrefix == prefix[:coveredPrefixLen] && len(allClosestPeers) > s.replicationFactor {
				// Exit early if the prefix is fully covered at the first request and
				// we have enough peers.
				earlyExit = true
				break explorationLoop
			}
		} else {
			latestPrefix := coveredPrefixesStack[len(coveredPrefixesStack)-1]
			for coveredPrefixLen <= len(latestPrefix) && coveredPrefix[:coveredPrefixLen-1] == latestPrefix[:coveredPrefixLen-1] {
				// Pop latest prefix from stack, because current prefix is
				// complementary.
				// e.g latestPrefix=0010, currentPrefix=0011. latestPrefix is
				// replaced by 001, unless 000 was also in the stack, etc.
				coveredPrefixesStack = coveredPrefixesStack[:len(coveredPrefixesStack)-1]
				coveredPrefix = coveredPrefix[:len(coveredPrefix)-1]
				coveredPrefixLen = len(coveredPrefix)

				if len(coveredPrefixesStack) == 0 {
					if coveredPrefixLen <= len(prefix) && len(allClosestPeers) > s.replicationFactor {
						earlyExit = true
						break explorationLoop
					}
					// Not enough peers -> add coveredPrefix to stack and continue.
					break
				}
				if coveredPrefixLen == 0 {
					logger.Error("coveredPrefixLen==0, coveredPrefixStack ", coveredPrefixesStack)
					earlyExit = true
					break explorationLoop
				}
				latestPrefix = coveredPrefixesStack[len(coveredPrefixesStack)-1]
			}
		}
		// Push coveredPrefix to stack
		coveredPrefixesStack = append(coveredPrefixesStack, coveredPrefix)
		// Flip last bit of last covered prefix
		nextPrefix = flipLastBit(coveredPrefixesStack[len(coveredPrefixesStack)-1])
	}

	peers := make([]peer.ID, 0, len(allClosestPeers))
	for p := range allClosestPeers {
		peers = append(peers, p)
	}
	if !earlyExit {
		return peers, errTooManyIterationsDuringExploration
	}
	logger.Debugf("Region %s exploration required %d requests to discover %d peers in %s", prefix, i, len(allClosestPeers), time.Since(startTime))
	return peers, err
}

// closestPeersToKey returns a valid peer ID sharing a long common prefix with
// the provided key. Note that the returned peer IDs aren't random, they are
// taken from a static list of preimages.
func (s *SweepingReprovider) closestPeersToKey(k bitstr.Key) ([]peer.ID, error) {
	p, _ := kb.GenRandPeerIDWithCPL(keyToBytes(k), kb.PeerIDPreimageMaxCpl)
	return s.router.GetClosestPeers(s.ctx, string(p))
}

// regionsFromPeers build the keyspace regions from the given peers, and
// assigns cids to the appropriate region. It returns the created regions
// ordered according to s.order and the Common Prefix shared by all peers.
func (s *SweepingReprovider) regionsFromPeers(peers []peer.ID, cids *trie.Trie[bit256.Key, mh.Multihash]) ([]region, bitstr.Key) {
	if len(peers) == 0 {
		return []region{}, ""
	}
	peersTrie := trie.New[bit256.Key, peer.ID]()
	minCpl := keyLen
	firstPeerKey := peerIDToBit256(peers[0])
	for _, p := range peers {
		k := peerIDToBit256(p)
		peersTrie.Add(k, p)
		minCpl = min(minCpl, firstPeerKey.CommonPrefixLength(k))
	}
	commonPrefix := bitstr.Key(key.BitString(firstPeerKey)[:minCpl])
	regions := extractMinimalRegions(peersTrie, commonPrefix, s.replicationFactor, s.order)
	for i, r := range regions {
		regions[i].cids, _ = subtrieMatchingPrefix(cids, r.prefix)
	}
	return regions, commonPrefix
}

func (s *SweepingReprovider) unscheduleSubsumedPrefixes(prefix bitstr.Key) {
	s.scheduleLk.Lock()
	// Pop prefixes scheduled in the future being covered by the explored peers.
	if subtrie, ok := subtrieMatchingPrefix(s.schedule, prefix); ok {
		logger.Warnf("previous next scheduled prefix is %s", s.prefixCursor)
		for _, entry := range allEntries(subtrie, s.order) {
			if s.schedule.Remove(entry.Key) {
				logger.Warnf("removed %s from schedule because of %s", entry.Key, prefix)
				if s.prefixCursor == entry.Key {
					next := nextNonEmptyLeaf(s.schedule, s.prefixCursor, s.order)
					if next == nil {
						s.scheduleNextReprovideNoLock(prefix, s.reprovideInterval)
					} else {
						timeUntilReprovide := s.timeUntil(next.Data)
						s.scheduleNextReprovideNoLock(next.Key, timeUntilReprovide)
					}
				}
			}
		}
		logger.Warnf("next scheduled prefix now is %s", s.prefixCursor)
	}
	s.scheduleLk.Unlock()
}

// claimRegionReprovide checks if the region is already being reprovided by
// another thread. If not it marks the region as being currently reprovided.
func (s *SweepingReprovider) claimRegionReprovide(regions []region) []region {
	out := regions[:0]
	s.ongoingReprovidesLk.Lock()
	defer s.ongoingReprovidesLk.Unlock()
	for _, r := range regions {
		if r.cids.Size() == 0 {
			continue
		}
		if ok, _ := trieHasPrefixOfKey(s.ongoingReprovides, r.prefix); !ok {
			out = append(out, r)
			s.ongoingReprovides.Add(r.prefix, struct{}{})
		}
	}
	return out
}

// releaseRegionReprovide marks the region as no longer being reprovided.
func (s *SweepingReprovider) releaseRegionReprovide(prefix bitstr.Key) {
	s.ongoingReprovidesLk.Lock()
	defer s.ongoingReprovidesLk.Unlock()
	s.ongoingReprovides.Remove(prefix)
}

const minimalRegionReachablePeersRatio float32 = 0.2

type provideJob struct {
	pid  peer.ID
	cids []mh.Multihash
}

// sendProviderRecords manages reprovides for all given peer ids and allocated
// cids. Upon failure to reprovide a CID, or to connect to a peer, it will NOT
// retry.
//
// Returns an error if we were unable to reprovide cids to a given threshold of
// peers. In this case, the region reprovide is considered failed and the
// caller is responsible for trying again. This allows detecting if we are
// offline.
func (s *SweepingReprovider) sendProviderRecords(cidsAllocations map[peer.ID][]mh.Multihash, addrInfo peer.AddrInfo) error {
	if len(cidsAllocations) == 0 {
		return nil
	}
	startTime := s.clock.Now()
	errCount := atomic.Uint32{}
	nWorkers := s.maxProvideConnsPerWorker
	jobChan := make(chan provideJob, nWorkers)

	wg := sync.WaitGroup{}
	wg.Add(nWorkers)
	for range nWorkers {
		go func() {
			pmes := genProvideMessage(addrInfo)
			defer wg.Done()
			for job := range jobChan {
				err := s.provideCidsToPeer(job.pid, job.cids, pmes)
				if err != nil {
					// logger.Debug(err)
					errCount.Add(1)
				}
			}
		}()
	}

	for p, cids := range cidsAllocations {
		jobChan <- provideJob{p, cids}
	}
	close(jobChan)
	wg.Wait()

	errCountLoaded := int(errCount.Load())
	logger.Infof("sent provider records to peers in %s, errors %d/%d", s.clock.Since(startTime), errCountLoaded, len(cidsAllocations))

	if errCountLoaded == len(cidsAllocations) || errCountLoaded > int(float32(len(cidsAllocations))*(1-minimalRegionReachablePeersRatio)) {
		return fmt.Errorf("unable to provide to enough peers (%d/%d)", len(cidsAllocations)-errCountLoaded, len(cidsAllocations))
	}
	return nil
}

// cidsAllocationsToPeers assigns each cid of the provided region to the
// replicationFactor closest peers from the region (XOR distance).
func (s *SweepingReprovider) cidsAllocationsToPeers(r region) map[peer.ID][]mh.Multihash {
	// TODO: this is a very greedy approach, can be greatly optimized
	keysPerPeer := make(map[peer.ID][]mh.Multihash)
	for _, cidEntry := range allEntries(r.cids, s.order) {
		for _, peerEntry := range trie.Closest(r.peers, cidEntry.Key, s.replicationFactor) {
			pid := peerEntry.Data
			if _, ok := keysPerPeer[pid]; !ok {
				keysPerPeer[pid] = []mh.Multihash{cidEntry.Data}
			} else {
				keysPerPeer[pid] = append(keysPerPeer[pid], cidEntry.Data)
			}
		}
	}
	return keysPerPeer
}

func genProvideMessage(addrInfo peer.AddrInfo) *pb.Message {
	pmes := pb.NewMessage(pb.Message_ADD_PROVIDER, []byte{}, 0)
	pmes.ProviderPeers = pb.RawPeerInfosToPBPeers([]peer.AddrInfo{addrInfo})
	return pmes
}

// maxConsecutiveProvideFailuresAllowed is the maximum number of consecutive
// provides that are allowed to fail to the same remote peer before cancelling
// all pending requests to this peer.
const maxConsecutiveProvideFailuresAllowed = 2

// provideCidsToPeer performs the network operation to advertise to the given
// DHT server (p) that we serve all the given cids.
func (s *SweepingReprovider) provideCidsToPeer(p peer.ID, cids []mh.Multihash, pmes *pb.Message) error {
	errCount := 0
	for _, mh := range cids {
		pmes.Key = mh
		err := s.msgSender.SendMessage(s.ctx, p, pmes)
		if err != nil {
			// logger.Debug("providing cid: ", mh, ", to: ", p, ", error: ", err)
			errCount++

			if errCount == len(cids) || errCount > maxConsecutiveProvideFailuresAllowed {
				return fmt.Errorf("failed to provide to %s: %s", p, err.Error())
			}
		} else if errCount > 0 {
			// Reset error count
			errCount = 0
		}
	}
	return nil
}

// scheduleNextReprovide schedules the next periodical reprovide for the given
// prefix, if the current operation is a periodic reprovide (not if initial
// provide or late reprovide).
//
// If the given prefix is already on the schedule, this function is a no op.
func (s *SweepingReprovider) scheduleNextReprovide(prefix bitstr.Key, lastReprovide time.Duration, op provideType) {
	if op == periodicReprovide {
		// Schedule next reprovide given that the prefix was just reprovided on
		// schedule. In the case the next reprovide time should be delayed due to a
		// growth in the number of network peers matching the prefix, don't delay
		// more than s.maxReprovideDelay.
		nextReprovideTime := min(s.reprovideTimeForPrefix(prefix), lastReprovide+s.reprovideInterval+s.maxReprovideDelay)
		s.scheduleLk.Lock()
		// If schedule contains keys starting with prefix, remove them to avoid
		// overlap.
		if subtrie, ok := subtrieMatchingPrefix(s.schedule, prefix); ok {
			for _, entry := range allEntries(subtrie, s.order) {
				s.schedule.Remove(entry.Key)
			}
		} else if ok, _ := trieHasPrefixOfKey(s.schedule, prefix); ok {
			return
		}
		s.schedule.Add(prefix, nextReprovideTime)
		if s.schedule.Size() == 1 {
			timeUntilReprovide := s.timeUntil(nextReprovideTime)
			s.scheduleNextReprovideNoLock(prefix, timeUntilReprovide)
		}
		s.scheduleLk.Unlock()
	}
}

// currentTimeOffset returns the current time offset in the reprovide cycle.
func (s *SweepingReprovider) currentTimeOffset() time.Duration {
	return s.timeOffset(s.clock.Now())
}

// timeOffset returns the time offset in the reprovide cycle for the given
// time.
func (s *SweepingReprovider) timeOffset(t time.Time) time.Duration {
	return t.Sub(s.cycleStart) % s.reprovideInterval
}

// timeUntil returns the time left (duration) until the given time offset.
func (s *SweepingReprovider) timeUntil(d time.Duration) time.Duration {
	return s.timeBetween(s.currentTimeOffset(), d)
}

// timeBetween returns the duration between the two provided offsets, assuming
// it is no more than s.reprovideInterval.
func (s *SweepingReprovider) timeBetween(from, to time.Duration) time.Duration {
	return (to-from+s.reprovideInterval-1)%s.reprovideInterval + 1
}

const maxPrefixSize = 24

// reprovideTimeForPrefix calculates the scheduled time offset for reproviding
// keys associated with a given prefix based on its bitstring prefix. The
// function maps the given binary prefix to a fraction of the overall reprovide
// interval (s.reprovideInterval), such that keys with prefixes closer to a
// configured target s.order (in XOR distance) are scheduled earlier and those
// further away later in the cycle.
//
// For any prefix of bit length n, the function generates 2^n distinct
// reprovide times that evenly partition the entire reprovide interval. The
// process first truncates s.order to n bits and then XORs it with the provided
// prefix. The resulting binary string is converted to an integer,
// corresponding to the index of the 2^n possible reprovide times to use for
// the prefix.
//
// This method ensures a deterministic and evenly distributed reprovide
// schedule, where the temporal position within the cycle is based on the
// binary representation of the key's prefix.
func (s *SweepingReprovider) reprovideTimeForPrefix(prefix bitstr.Key) time.Duration {
	if len(prefix) == 0 {
		// Empty prefix: all reprovides occur at the beginning of the cycle.
		return 0
	}
	if len(prefix) > maxPrefixSize {
		// Truncate the prefix to the maximum allowed size to avoid overly fine
		// slicing of time.
		prefix = prefix[:maxPrefixSize]
	}
	// Number of possible bitstrings of the same length as prefix.
	maxInt := int64(1 << len(prefix))
	// XOR the prefix with the order key to reorder the schedule: keys "close" to
	// s.order are scheduled first in the cycle, and those "far" from it are
	// scheduled later.
	order := bitstr.Key(key.BitString(s.order)[:len(prefix)])
	k := prefix.Xor(order)
	val, _ := strconv.ParseInt(string(k), 2, 64)
	// Calculate the time offset as a fraction of the overall reprovide interval.
	return time.Duration(int64(s.reprovideInterval) * val / maxInt)
}

// nextPrefixToReprovide returns the next prefix to reprovide, based on the
// current time offset.
func (s *SweepingReprovider) nextPrefixToReprovideNoLock() bitstr.Key {
	now := s.currentTimeOffset()
	maxVal := time.Duration(1 << maxPrefixSize)

	intPrefix := now * maxVal / s.reprovideInterval
	prefix := bitstr.Key(fmt.Sprintf("%0*b", maxPrefixSize, intPrefix))
	return nextNonEmptyLeaf(s.schedule, prefix, s.order).Key
}

func (s *SweepingReprovider) catchupPendingNotify() {
	select {
	case s.catchupPendingChan <- struct{}{}:
	default:
	}
}

func (s *SweepingReprovider) catchupPendingWork() {
	if s.catchupInProgress.Load() {
		// Another catchup is already in process, skip and we'll try again later.
		return
	}
	pendingCids := s.pendingCids
	lateRegions := s.lateRegionsQueue
	if len(pendingCids) == 0 && len(lateRegions) == 0 {
		// No pending work :)
		return
	}
	// Empty pending lists
	s.pendingCids = s.pendingCids[:0]
	s.lateRegionsQueue = s.lateRegionsQueue[:0]

	// 1. Iterate on cids, and remove those matching any prefix in the
	//    lateRegions (they will be provided as part of the region)
	lateRegionsTrie := trie.New[bitstr.Key, mh.Multihash]()
	for _, r := range lateRegions {
		lateRegionsTrie.Add(r, nil)
	}
	cidsInNoRegions := []mh.Multihash{}
	for _, c := range pendingCids {
		if ok, _ := trieHasPrefixOfKey(lateRegionsTrie, mhToBit256(c)); !ok {
			cidsInNoRegions = append(cidsInNoRegions, c)
		}
	}

	allCids := s.cids.Copy() // NOTE: memory usage may be large
	go func() {
		// 2. Iterate on lateRegions, and reprovide them
		for _, r := range lateRegions {
			s.workerPool.Acquire(burstWorker)
			go func() {
				defer s.workerPool.Release(burstWorker)
				s.provideForPrefix(r, allCids, lateRegionReprovide)
			}()
		}

		// 3. Provide the remaining cids that haven't been provided yet as part of
		//    a late region.
		s.networkProvide(s.getPrefixesForCids(cidsInNoRegions))
	}()
}

func (s *SweepingReprovider) clearPendingWork() {
	s.lateRegionsQueue = s.lateRegionsQueue[:0]
	s.pendingCids = s.pendingCids[:0]
	// Emtpy chans with pending work
	for {
		select {
		case <-s.failedRegionsChan:
		case <-s.pendingCidsChan:
		case <-s.catchupPendingChan:
		default:
			return
		}
	}
}

func (s *SweepingReprovider) resetCids(req resetCidsReq) {
	var err error
	s.cids = trie.New[bit256.Key, mh.Multihash]()

	s.scheduleLk.Lock()
	prefixLen := s.getAvgPrefixLenNoLock()
	newSchedule := trie.New[bitstr.Key, time.Duration]()

loop:
	for {
		select {
		case <-req.ctx.Done():
			err = req.ctx.Err()
			break loop
		case h, ok := <-req.cids:
			if !ok {
				break loop // all keys processed
			}
			k := mhToBit256(h)
			// Add new key to cids trie
			s.cids.Add(k, h)

			// Add to local provider store
			s.addLocalRecord(h)

			// Add according prefix to schedule if needed
			if ok, _ := trieHasPrefixOfKey(newSchedule, k); !ok {
				var prefix bitstr.Key
				if ok, p := trieHasPrefixOfKey(s.schedule, k); ok {
					prefix = p
				} else {
					prefix = bitstr.Key(key.BitString(k)[:prefixLen])
				}
				newSchedule.Add(prefix, s.reprovideTimeForPrefix(prefix))
			}
		}
	}
	nextPrefix := s.nextPrefixToReprovideNoLock()
	timeUntilReprovide := s.timeUntil(s.reprovideTimeForPrefix(nextPrefix))
	s.scheduleNextReprovideNoLock(nextPrefix, timeUntilReprovide)
	s.scheduleLk.Unlock()

	s.clearPendingWork()

	req.done <- err
}

// getAvgPrefixLenNoLock returns the average prefix length of all scheduled
// prefixes.
func (s *SweepingReprovider) getAvgPrefixLenNoLock() int {
	s.avgPrefixLenLk.Lock()
	defer s.avgPrefixLenLk.Unlock()
	if s.lastAvgPrefixLen.Add(s.avgPrefixLenValidity).After(s.clock.Now()) {
		// Return cached value if it is still valid.
		return s.cachedAvgPrefixLen
	}
	prefixLenSum := 0
	scheduleSize := s.schedule.Size()
	if scheduleSize > 0 {
		// Take average prefix length of all scheduled prefixes.
		for _, entry := range allEntries(s.schedule, s.order) {
			prefixLenSum += len(entry.Key)
		}
		s.cachedAvgPrefixLen = prefixLenSum / scheduleSize
		s.lastAvgPrefixLen = s.clock.Now()
	}
	return s.cachedAvgPrefixLen
}

// Provide returns an error if the cid failed to be provided to the network.
// However, it will keep reproviding the cid regardless of whether the first
// provide succeeded.
func (s *SweepingReprovider) Provide(ctx context.Context, c cid.Cid, broadcast bool) error {
	if !broadcast {
		s.addLocalRecord(c.Hash())
	}
	doneChan := make(chan error, 1)
	req := provideReq{
		ctx:  ctx,
		cids: []mh.Multihash{c.Hash()},
		done: doneChan,
	}
	s.provideChan <- req
	// Wait for initial provide to complete before returning.
	return <-doneChan
}

// ProvideMany provides multiple cids to the network. It will return an error
// if none of the cids could be provided, however it will keep reproviding
// these cids regardless of the initial provide success.
func (s *SweepingReprovider) ProvideMany(ctx context.Context, keys []mh.Multihash) error {
	doneChan := make(chan error, 1)
	req := provideReq{
		ctx:  ctx,
		cids: keys,
		done: doneChan,
	}
	s.provideChan <- req
	// Wait for all cids to be provided before returning.
	return <-doneChan
}

func (s *SweepingReprovider) ResetReprovideSet(ctx context.Context, keyChan <-chan mh.Multihash) error {
	doneChan := make(chan error, 1)
	req := resetCidsReq{
		ctx:  ctx,
		cids: keyChan,
		done: doneChan,
	}
	s.resetCidsChan <- req
	// Wait for the reset to complete before returning.
	return <-doneChan
}
