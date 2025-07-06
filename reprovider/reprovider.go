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
	ds "github.com/ipfs/go-datastore"
	logging "github.com/ipfs/go-log/v2"

	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
	"github.com/libp2p/go-libp2p-kad-dht/reprovider/datastore"
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

type Reprovider interface {
	StartProviding(...mh.Multihash)
	StopProviding(...mh.Multihash)
	InstantProvide(context.Context, ...mh.Multihash) error
	ForceProvide(context.Context, ...mh.Multihash) error
}

var (
	_ Provider    = &SweepingReprovider{}
	_ ProvideMany = &SweepingReprovider{}
	_ Reprovider  = &SweepingReprovider{}
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
// persist prefixes waiting to be reprovided and last reprovided prefix (with time) on disk

type workerType uint8

const (
	periodicWorker workerType = iota
	burstWorker
)

type provideReq struct {
	ctx           context.Context
	cids          []mh.Multihash
	done          chan<- error
	reprovideKeys bool
	forceProvide  bool
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
	avgPrefixLenReady    chan struct{}
	cachedAvgPrefixLen   int
	lastAvgPrefixLen     time.Time
	avgPrefixLenValidity time.Duration

	mhStore *datastore.MHStore

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
	var cfg config
	err := cfg.apply(append([]Option{DefaultConfig}, opts...)...)
	if err != nil {
		return nil, err
	}
	if cfg.mhStore == nil {
		// Setup MHStore if missing
		mhStore, err := datastore.NewMHStore(ctx, ds.NewMapDatastore())
		if err != nil {
			return nil, err
		}
		cfg.mhStore = mhStore
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
		cachedAvgPrefixLen:   -1,
		avgPrefixLenReady:    make(chan struct{}, 1),

		clock:      cfg.clock,
		cycleStart: cfg.clock.Now(),

		msgSender:      cfg.msgSender,
		getSelfAddrs:   cfg.selfAddrs,
		addLocalRecord: cfg.addLocalRecord,

		mhStore: cfg.mhStore,

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

func (s *SweepingReprovider) run() {
	logger.Debug("Starting SweepingReprovider")
	go s.measureInitialPrefixLen()
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
	s.avgPrefixLenLk.Lock()
	defer s.avgPrefixLenLk.Unlock()
	if nSamples == 0 {
		s.cachedAvgPrefixLen = 0
	} else {
		s.cachedAvgPrefixLen = int(cplSum.Load() / nSamples)
	}
	logger.Debugf("initial avgPrefixLen is %d", s.cachedAvgPrefixLen)
	s.lastAvgPrefixLen = s.clock.Now()
	s.avgPrefixLenReady <- struct{}{}
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

func (s *SweepingReprovider) handleProvide(provideRequest provideReq) {
	if len(provideRequest.cids) == 0 {
		if provideRequest.done != nil {
			provideRequest.done <- nil
		}
		return
	}

	var err error
	var cids []mh.Multihash
	var prefixes map[bitstr.Key][]mh.Multihash
	if provideRequest.reprovideKeys {
		// Add cids to list of cids to be reprovided. Returned cids are deduplicated
		// newly added cids.
		cids, err = s.mhStore.Put(s.ctx, provideRequest.cids...)
		if err != nil {
			if provideRequest.done != nil {
				provideRequest.done <- err
			}
			return
		} else if len(cids) == 0 {
			// All cids are already tracked and have been provided.
			if provideRequest.done != nil {
				provideRequest.done <- nil
			}
			return
		}
		prefixes = s.groupCidsAndAddToSchedule(cids)
	}

	if !provideRequest.reprovideKeys || provideRequest.forceProvide {
		// Don't filter out cids that are already being reprovided.
		cids = make([]mh.Multihash, 0, len(provideRequest.cids))
		seen := make(map[string]struct{})
		for _, cid := range provideRequest.cids {
			k := string(cid)
			if _, ok := seen[k]; !ok {
				seen[k] = struct{}{}
				cids = append(cids, cid)
			}
		}
		prefixes = s.groupCidsByPrefix(cids)
	}

	if !s.connectivity.isOnline() {
		logger.Warn("Node is offline, cannot provide requested cids.")
		if provideRequest.reprovideKeys {
			s.pendingCids = append(s.pendingCids, cids...)
		}
		if provideRequest.done != nil {
			provideRequest.done <- ErrNodeOffline
		}
		return
	}

	logger.Debugf("Providing %d new cids with %d different prefixes", len(cids), len(prefixes))
	go func() {
		err := s.networkProvide(prefixes, provideRequest.reprovideKeys)
		if provideRequest.done != nil {
			provideRequest.done <- err
		}
	}()
}

// prefixForKeyNoLock returns the prefix corresponding to the region in which
// the given key belongs. The prefix corresponding to the region depends on the
// peers in the DHT swarms.
func (s *SweepingReprovider) prefixForKeyNoLock(k bit256.Key) bitstr.Key {
	if scheduled, prefix := trieHasPrefixOfKey(s.schedule, k); scheduled {
		return prefix
	}
	avgPrefixLen := s.getAvgPrefixLenNoLock()
	return bitstr.Key(key.BitString(k)[:avgPrefixLen])
}

// groupCidsAndAddToSchedule groups cids by common prefix used in the schedule.
// If the common prefix isn't included in the schedule, add it.
func (s *SweepingReprovider) groupCidsAndAddToSchedule(cids []mh.Multihash) map[bitstr.Key][]mh.Multihash {
	prefixes := make(map[bitstr.Key][]mh.Multihash)
	avgPrefixLen := 0

	s.scheduleLk.Lock()
	defer s.scheduleLk.Unlock()
	for _, c := range cids {
		prefixConsolidation := false
		k := mhToBit256(c)
		// Add cid to s.cids if not there already, and add fresh cids to newCids.
		// Deduplication is handled here.
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
			prefixes[prefix] = []mh.Multihash{c}
		} else {
			prefixes[prefix] = append(prefixes[prefix], c)
		}
		if prefixConsolidation {
			// prefix is a shorted prefix of a prefix already in the schedule.
			// Consolidate everything into prefix.
			for p, cids := range prefixes {
				if isBitstrPrefix(p, prefix) {
					seen := make(map[string]struct{})
					for _, k := range prefixes[prefix] {
						seen[string(k)] = struct{}{}
					}
					for _, c := range cids {
						strKey := string(c)
						if _, ok := seen[strKey]; ok {
							continue
						}
						seen[strKey] = struct{}{}
						prefixes[prefix] = append(prefixes[prefix], c)
					}
					delete(prefixes, p)
				}
			}
		}
	}
	return prefixes
}

// groupCidsBySchedulePrefix groups cids by their common prefix that is
// currently used in the schedule.
func (s *SweepingReprovider) groupCidsByPrefix(cids []mh.Multihash) map[bitstr.Key][]mh.Multihash {
	prefixes := make(map[bitstr.Key][]mh.Multihash)
	seen := make(map[bit256.Key]struct{})
	s.scheduleLk.Lock()
	defer s.scheduleLk.Unlock()
	for _, c := range cids {
		k := mhToBit256(c)
		// Don't add duplicates
		if _, ok := seen[k]; ok {
			continue
		}
		seen[k] = struct{}{}
		prefix := s.prefixForKeyNoLock(k)
		if _, ok := prefixes[prefix]; !ok {
			prefixes[prefix] = []mh.Multihash{c}
		} else {
			prefixes[prefix] = append(prefixes[prefix], c)
		}
	}
	return prefixes
}

func (s *SweepingReprovider) networkProvide(prefixes map[bitstr.Key][]mh.Multihash, retryOnFailure bool) error {
	if len(prefixes) == 1 {
		for prefix, cids := range prefixes {
			// Provide a single cid (NOT ProvideMany)
			if len(cids) == 1 {
				return s.provideForPrefix(prefix, cids, retryOnFailure)
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

				if err := s.provideForPrefix(p.prefix, p.cids, retryOnFailure); err != nil {
					logger.Error(err)
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
			cids = append(cids, prefix.cids...)
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

	go func() {
		s.workerPool.Acquire(periodicWorker)
		defer s.workerPool.Release(periodicWorker)
		err := s.reprovideForPrefix(currentPrefix, true)
		if err != nil {
			logger.Error(err)
		}
	}()
}

func (s *SweepingReprovider) reprovideForPrefix(prefix bitstr.Key, periodicReprovide bool) error {
	selfAddrs := s.getSelfAddrs()
	if len(selfAddrs) == 0 {
		// NOTE: kubo doesn't like no being able to provide eventough sometimes no
		// valid addresses are provided. This makes the kubo tests fail.
		return errors.New("no self addresses available for providing cids")
	}
	cids, err := s.mhStore.Get(s.ctx, prefix)
	if err != nil {
		s.failedRegionsChan <- prefix
		if periodicReprovide {
			s.scheduleNextReprovide(prefix, s.currentTimeOffset())
		}
		return fmt.Errorf("couldn't reprovide, error when loading cids: %s", err)
	} else if len(cids) == 0 {
		logger.Errorf("No cids to reprovide for prefix %s", prefix)
		return nil
	} else if len(cids) <= 2 {
		return s.individualProvideForPrefix(s.ctx, prefix, cids, true, periodicReprovide)
	}

	logger.Debugf("Starting to explore prefix %s for reproviding %d matching cids", prefix, len(cids))
	peers, err := s.closestPeersToPrefix(prefix)
	if err != nil {
		if err == errTooManyIterationsDuringExploration {
			logger.Errorf("prefix key exploration not complete: %s", prefix)
		}
		s.failedRegionsChan <- prefix
		if periodicReprovide {
			s.scheduleNextReprovide(prefix, s.currentTimeOffset())
		}
		return err
	}
	if len(peers) == 0 {
		s.failedRegionsChan <- prefix
		if periodicReprovide {
			s.scheduleNextReprovide(prefix, s.currentTimeOffset())
		}
		return errors.New("no peers found when exploring prefix " + string(prefix))
	}
	regions, coveredPrefix := s.regionsFromPeers(peers)
	if len(coveredPrefix) < len(prefix) {
		// We need to load more cids
		cids, err = s.mhStore.Get(s.ctx, coveredPrefix)
		if err != nil {
			s.failedRegionsChan <- prefix
			if periodicReprovide {
				s.scheduleNextReprovide(prefix, s.currentTimeOffset())
			}
			return fmt.Errorf("couldn't reprovide, error when loading cids: %s", err)
		}
	}
	regions = assignCidsToRegions(regions, cids)
	logger.Debugf("requested prefix: %s (len %d), prefix covered: %s (len %d)", prefix, len(prefix), coveredPrefix, len(coveredPrefix))

	// When reproviding a region, remove all scheduled regions starting with
	// the currently covered prefix.
	s.unscheduleSubsumedPrefixes(coveredPrefix)

	// Claim the regions to be reprovided, so that no other thread will try to
	// reprovdie them concurrently.
	regions = s.claimRegionReprovide(regions)

	errCount := 0
	addrInfo := peer.AddrInfo{ID: s.peerid, Addrs: selfAddrs}
	for _, r := range regions {
		if r.cids.Size() == 0 {
			s.releaseRegionReprovide(r.prefix)
			continue
		}
		// Add keys to local provider store
		for _, entry := range allEntries(r.cids, s.order) {
			s.addLocalRecord(entry.Data)
		}
		cidsAllocations := allocateToKClosest(r.cids, r.peers, s.replicationFactor)
		err := s.sendProviderRecords(cidsAllocations, addrInfo)
		s.releaseRegionReprovide(r.prefix)
		if periodicReprovide {
			s.scheduleNextReprovide(r.prefix, s.currentTimeOffset())
		}
		if err != nil {
			logger.Warn(err, ": region ", r.prefix)
			errCount++
			s.failedRegionsChan <- r.prefix
			continue
		}
		s.provideCounter.Add(s.ctx, int64(r.cids.Size()))

		// TODO: persist to datastore that region identified by prefix was reprovided `now`, only during periodic reprovides?
	}
	// If at least 1 regions was provided, we don't consider it a failure.
	if errCount == len(regions) {
		return errors.New("failed to reprovide any region")
	}
	return nil
}

func (s *SweepingReprovider) provideForPrefix(prefix bitstr.Key, cids []mh.Multihash, retryOnFailure bool) error {
	if len(cids) == 0 {
		return nil
	}
	selfAddrs := s.getSelfAddrs()
	if len(selfAddrs) == 0 {
		// NOTE: kubo doesn't like no being able to provide eventough sometimes no
		// valid addresses are provided. This makes the kubo tests fail.
		return errors.New("no self addresses available for providing cids")
	}
	if len(cids) <= 2 {
		// Region has 1 or 2 cids, it is more optimized to provide them naively.
		return s.individualProvideForPrefix(s.ctx, prefix, cids, false, false)
	}
	logger.Debugf("Starting to explore prefix %s for %d matching cids", prefix, len(cids))
	peers, err := s.closestPeersToPrefix(prefix)
	if err != nil {
		if retryOnFailure {
			s.pendingCidsChan <- cids
		}
		if err == errTooManyIterationsDuringExploration {
			logger.Errorf("prefix key exploration not complete: %s", prefix)
		}
		return err
	}
	if len(peers) == 0 {
		if retryOnFailure {
			s.pendingCidsChan <- cids
		}
		return errors.New("no peers found when exploring prefix " + string(prefix))
	}

	regions, _ := s.regionsFromPeers(peers)
	// TODO: it would be great to take all cids matching coveredPrefix from
	// pendingCids pool.
	regions = assignCidsToRegions(regions, cids)

	errCount := 0
	addrInfo := peer.AddrInfo{ID: s.peerid, Addrs: selfAddrs}
	for _, r := range regions {
		if r.cids.Size() == 0 {
			continue
		}
		// Add keys to local provider store
		for _, entry := range allEntries(r.cids, s.order) {
			s.addLocalRecord(entry.Data)
		}
		cidsAllocations := allocateToKClosest(r.cids, r.peers, s.replicationFactor)
		err := s.sendProviderRecords(cidsAllocations, addrInfo)
		if err != nil {
			logger.Warn(err, ": region ", r.prefix)
			errCount++
			if retryOnFailure {
				s.pendingCidsChan <- allValues(r.cids, s.order)
			}
			continue
		}
		s.provideCounter.Add(s.ctx, int64(r.cids.Size()))
	}
	// If at least 1 regions was provided, we don't consider it a failure.
	if errCount == len(regions) {
		return errors.New("failed to provide to any region")
	}
	return nil
}

func (s *SweepingReprovider) individualProvideForPrefix(ctx context.Context, prefix bitstr.Key, cids []mh.Multihash, reprovide bool, periodicReprovide bool) error {
	if len(cids) == 0 {
		return nil
	}

	var provideErr error

	if len(cids) == 1 {
		coveredPrefix, err := s.vanillaProvide(ctx, cids[0])
		if err != nil && !reprovide {
			s.pendingCidsChan <- cids
		}
		provideErr = err
		if periodicReprovide {
			s.scheduleNextReprovide(coveredPrefix, s.currentTimeOffset())
		}
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
				} else if !reprovide {
					// Individual provide failed
					s.pendingCidsChan <- []mh.Multihash{cid}
				}
			}()
		}
		wg.Wait()

		if !success.Load() {
			provideErr = errors.New("all individual provides failed for prefix " + string(prefix))
		}
		if periodicReprovide {
			s.scheduleNextReprovide(prefix, s.currentTimeOffset())
		}
	}
	if reprovide {
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

// regionsFromPeers returns the keyspace regions from given peers ordered
// according to s.order and the Common Prefix shared by all peers.
func (s *SweepingReprovider) regionsFromPeers(peers []peer.ID) ([]region, bitstr.Key) {
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
// prefix, should be used only if the current operation is a periodic reprovide
// (not if initial provide or late reprovide).
//
// If the given prefix is already on the schedule, this function is a no op.
func (s *SweepingReprovider) scheduleNextReprovide(prefix bitstr.Key, lastReprovide time.Duration) {
	// Schedule next reprovide given that the prefix was just reprovided on
	// schedule. In the case the next reprovide time should be delayed due to a
	// growth in the number of network peers matching the prefix, don't delay
	// more than s.maxReprovideDelay.
	nextReprovideTime := min(s.reprovideTimeForPrefix(prefix), lastReprovide+s.reprovideInterval+s.maxReprovideDelay)
	s.scheduleLk.Lock()
	defer s.scheduleLk.Unlock()
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

// TODO: remove if not needed
//
// nextPrefixToReprovide returns the next prefix to reprovide, based on the
// current time offset.
// func (s *SweepingReprovider) nextPrefixToReprovideNoLock() bitstr.Key {
// 	now := s.currentTimeOffset()
// 	maxVal := time.Duration(1 << maxPrefixSize)
//
// 	intPrefix := now * maxVal / s.reprovideInterval
// 	prefix := bitstr.Key(fmt.Sprintf("%0*b", maxPrefixSize, intPrefix))
// 	return nextNonEmptyLeaf(s.schedule, prefix, s.order).Key
// }

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

	go func() {
		// 2. Iterate on lateRegions, and reprovide them
		for _, r := range lateRegions {
			s.workerPool.Acquire(burstWorker)
			go func() {
				defer s.workerPool.Release(burstWorker)
				err := s.reprovideForPrefix(r, false)
				if err != nil {
					logger.Error(err)
				}
			}()
		}

		// 3. Provide the remaining cids that haven't been provided yet as part of
		//    a late region.
		s.networkProvide(s.groupCidsByPrefix(cidsInNoRegions), true)
	}()
}

// getAvgPrefixLenNoLock returns the average prefix length of all scheduled
// prefixes.
func (s *SweepingReprovider) getAvgPrefixLenNoLock() int {
	s.avgPrefixLenLk.Lock()
	defer s.avgPrefixLenLk.Unlock()

	if s.cachedAvgPrefixLen == -1 {
		// Wait for initial measurement to complete. Requires the node to come
		// online.
		s.avgPrefixLenLk.Unlock()
		<-s.avgPrefixLenReady
		s.avgPrefixLenLk.Lock()
		return s.cachedAvgPrefixLen
	}

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
		ctx:           ctx,
		cids:          []mh.Multihash{c.Hash()},
		done:          doneChan,
		reprovideKeys: true,
		forceProvide:  false,
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
		ctx:           ctx,
		cids:          keys,
		done:          doneChan,
		reprovideKeys: true,
		forceProvide:  false,
	}
	s.provideChan <- req
	// Wait for all cids to be provided before returning.
	return <-doneChan
}

// StartProviding provides the given keys to the DHT swarm unless they were
// already provided in the past. The keys will be periodically reprovided until
// StopProviding is called for the same keys or user defined garbage collection
// deletes the keys.
func (s *SweepingReprovider) StartProviding(keys ...mh.Multihash) {
	s.provideChan <- provideReq{
		ctx:           context.Background(),
		cids:          keys,
		done:          nil,
		reprovideKeys: true,
		forceProvide:  false,
	}
}

// StopProviding stops reproviding the given keys to the DHT swarm. The node
// stops being referred as a provider when the provider records in the DHT
// swarm expire.
func (s *SweepingReprovider) StopProviding(keys ...mh.Multihash) {
	err := s.mhStore.Delete(s.ctx, keys...)
	if err != nil {
		logger.Errorf("failed to stop providing keys: %s", err)
	}
}

// InstantProvide only sends provider records for the given keys out to the DHT
// swarm. It does NOT take the responsibility to reprovide these keys.
func (s *SweepingReprovider) InstantProvide(ctx context.Context, keys ...mh.Multihash) error {
	doneChan := make(chan error, 1)
	req := provideReq{
		ctx:           ctx,
		cids:          keys,
		done:          doneChan,
		reprovideKeys: false,
		forceProvide:  true,
	}
	s.provideChan <- req
	// Wait for provide to complete before returning potential error.
	return <-doneChan
}

// ForceProvide is similar to StartProviding, but it sends provider records out
// to the DHT even if the keys were already provided in the past.
func (s *SweepingReprovider) ForceProvide(ctx context.Context, keys ...mh.Multihash) error {
	doneChan := make(chan error, 1)
	req := provideReq{
		ctx:           ctx,
		cids:          keys,
		done:          doneChan,
		reprovideKeys: true,
		forceProvide:  true,
	}
	s.provideChan <- req
	// Wait for initial provide to complete before returning.
	return <-doneChan
}
