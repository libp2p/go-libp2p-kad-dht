package provider

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/filecoin-project/go-clock"
	pool "github.com/guillaumemichel/reservedpool"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipfs/go-test/random"
	kb "github.com/libp2p/go-libp2p-kbucket"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	mh "github.com/multiformats/go-multihash"
	"go.opentelemetry.io/otel/metric"

	"github.com/probe-lab/go-libdht/kad/key"
	"github.com/probe-lab/go-libdht/kad/key/bit256"
	"github.com/probe-lab/go-libdht/kad/key/bitstr"
	"github.com/probe-lab/go-libdht/kad/trie"

	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
	"github.com/libp2p/go-libp2p-kad-dht/provider/datastore"
	"github.com/libp2p/go-libp2p-kad-dht/provider/internal/connectivity"
	"github.com/libp2p/go-libp2p-kad-dht/provider/internal/keyspace"
	"github.com/libp2p/go-libp2p-kad-dht/provider/internal/queue"
)

// DHTProvider is an interface for providing keys to a DHT swarm. It holds a
// state of keys to be advertised, and is responsible for periodically
// publishing provider records for these keys to the DHT swarm before the
// records expire.
type DHTProvider interface {
	// StartProviding ensures keys are periodically advertised to the DHT swarm.
	//
	// If the `keys` aren't currently being reprovided, they are added to the
	// queue to be provided to the DHT swarm as soon as possible, and scheduled
	// to be reprovided periodically. If `force` is set to true, all keys are
	// provided to the DHT swarm, regardless of whether they were already being
	// reprovided in the past. `keys` keep being reprovided until `StopProviding`
	// is called.
	//
	// This operation is asynchronous, it returns as soon as the `keys` are added
	// to the provide queue, and provides happens asynchronously.
	StartProviding(force bool, keys ...mh.Multihash)

	// StopProviding stops reproviding the given keys to the DHT swarm. The node
	// stops being referred as a provider when the provider records in the DHT
	// swarm expire.
	//
	// Remove the `keys` from the schedule and return immediately. Valid records
	// can remain in the DHT swarm up to the provider record TTL after calling
	// `StopProviding`.
	StopProviding(keys ...mh.Multihash)

	// ProvideOnce sends provider records for the specified keys to the DHT swarm
	// only once. It does not automatically reprovide those keys afterward.
	//
	// Add the supplied multihashes to the provide queue, and return immediately.
	// The provide operation happens asynchronously.
	ProvideOnce(keys ...mh.Multihash)
}

var _ DHTProvider = &SweepingProvider{}

const (
	// maxPrefixSize is the maximum size of a prefix used to define a keyspace
	// region.
	maxPrefixSize = 24
	// individualProvideThreshold is the threshold for the number of keys to
	// trigger a region exploration. If the number of keys to provide for a
	// region is less or equal to the threshold, the keys will be individually
	// provided.
	individualProvideThreshold = 2
)

const loggerName = "dht/SweepingProvider"

var logger = logging.Logger(loggerName)

type KadClosestPeersRouter interface {
	GetClosestPeers(context.Context, string) ([]peer.ID, error)
}

type workerType uint8

const (
	periodicWorker workerType = iota
	burstWorker
)

type SweepingProvider struct {
	// TODO: complete me
	done      chan struct{}
	closeOnce sync.Once

	peerid peer.ID
	order  bit256.Key
	router KadClosestPeersRouter

	connectivity *connectivity.ConnectivityChecker

	keyStore *datastore.KeyStore

	replicationFactor int

	provideQueue   *queue.ProvideQueue
	provideRunning sync.Mutex
	reprovideQueue *queue.ReprovideQueue

	workerPool               *pool.Pool[workerType]
	maxProvideConnsPerWorker int

	clock             clock.Clock
	cycleStart        time.Time
	reprovideInterval time.Duration
	maxReprovideDelay time.Duration

	schedule               *trie.Trie[bitstr.Key, time.Duration]
	scheduleLk             sync.Mutex
	scheduleCursor         bitstr.Key
	scheduleTimer          *clock.Timer
	scheduleTimerStartedAt time.Time

	ongoingReprovides   *trie.Trie[bitstr.Key, struct{}]
	ongoingReprovidesLk sync.Mutex

	avgPrefixLenLk       sync.Mutex
	avgPrefixLenReady    chan struct{}
	cachedAvgPrefixLen   int
	lastAvgPrefixLen     time.Time
	avgPrefixLenValidity time.Duration

	msgSender      pb.MessageSender
	getSelfAddrs   func() []ma.Multiaddr
	addLocalRecord func(mh.Multihash) error

	provideCounter metric.Int64Counter
}

// FIXME: remove me
func (s *SweepingProvider) SatisfyLinter() {
	s.measureInitialPrefixLen()
	s.batchReprovide("", true)
}

// Close stops the provider and releases all resources.
func (s *SweepingProvider) Close() {
	s.closeOnce.Do(func() { close(s.done) })
}

func (s *SweepingProvider) closed() bool {
	select {
	case <-s.done:
		return true
	default:
		return false
	}
}

// scheduleNextReprovideNoLock makes sure the scheduler wakes up in
// `timeUntilReprovide` to reprovide the region identified by `prefix`.
func (s *SweepingProvider) scheduleNextReprovideNoLock(prefix bitstr.Key, timeUntilReprovide time.Duration) {
	s.scheduleCursor = prefix
	s.scheduleTimer.Reset(timeUntilReprovide)
	s.scheduleTimerStartedAt = s.clock.Now()
}

func (s *SweepingProvider) reschedulePrefix(prefix bitstr.Key) {
	s.scheduleLk.Lock()
	s.schedulePrefixNoLock(prefix, true)
	s.scheduleLk.Unlock()
}

// schedulePrefixNoLock adds the supplied prefix to the schedule, unless
// already present.
//
// If `justReprovided` is true, it will schedule the next reprovide at most
// s.reprovideInterval+s.maxReprovideDelay in the future, allowing the
// reprovide to be delayed of at most maxReprovideDelay.
//
// If the supplied prefix is the next prefix to be reprovided, update the
// schedule cursor and timer.
func (s *SweepingProvider) schedulePrefixNoLock(prefix bitstr.Key, justReprovided bool) {
	nextReprovideTime := s.reprovideTimeForPrefix(prefix)
	if justReprovided {
		// Schedule next reprovide given that the prefix was just reprovided on
		// schedule. In the case the next reprovide time should be delayed due to a
		// growth in the number of network peers matching the prefix, don't delay
		// more than s.maxReprovideDelay.
		nextReprovideTime = min(nextReprovideTime, s.currentTimeOffset()+s.reprovideInterval+s.maxReprovideDelay)
	}
	// If schedule contains keys starting with prefix, remove them to avoid
	// overlap.
	if _, ok := keyspace.FindPrefixOfKey(s.schedule, prefix); ok {
		// Already scheduled.
		return
	}
	// Unschedule superstrings in schedule if any.
	s.unscheduleSubsumedPrefixesNoLock(prefix)

	s.schedule.Add(prefix, nextReprovideTime)

	// Check if the prefix that was just added is the next one to be reprovided.
	if s.schedule.IsNonEmptyLeaf() {
		// The prefix we insterted is the only element in the schedule.
		timeUntilPrefixReprovide := s.timeUntil(nextReprovideTime)
		s.scheduleNextReprovideNoLock(prefix, timeUntilPrefixReprovide)
		return
	}
	followingKey := keyspace.NextNonEmptyLeaf(s.schedule, prefix, s.order).Key
	if followingKey == s.scheduleCursor {
		// The key following prefix is the schedule cursor.
		timeUntilPrefixReprovide := s.timeUntil(nextReprovideTime)
		_, scheduledAlarm := trie.Find(s.schedule, s.scheduleCursor)
		if timeUntilPrefixReprovide < s.timeUntil(scheduledAlarm) {
			s.scheduleNextReprovideNoLock(prefix, timeUntilPrefixReprovide)
		}
	}
}

// unscheduleSubsumedPrefixes removes all superstrings of `prefix` that are
// scheduled in the future. Assumes that the schedule lock is held.
func (s *SweepingProvider) unscheduleSubsumedPrefixesNoLock(prefix bitstr.Key) {
	// Pop prefixes scheduled in the future being covered by the explored peers.
	keyspace.PruneSubtrie(s.schedule, prefix)

	// If we removed s.scheduleCursor from schedule, select the next one
	if keyspace.IsBitstrPrefix(prefix, s.scheduleCursor) {
		next := keyspace.NextNonEmptyLeaf(s.schedule, s.scheduleCursor, s.order)
		if next == nil {
			s.scheduleNextReprovideNoLock(prefix, s.reprovideInterval)
		} else {
			timeUntilReprovide := s.timeUntil(next.Data)
			s.scheduleNextReprovideNoLock(next.Key, timeUntilReprovide)
			logger.Warnf("next scheduled prefix now is %s", s.scheduleCursor)
		}
	}
}

// currentTimeOffset returns the current time offset in the reprovide cycle.
func (s *SweepingProvider) currentTimeOffset() time.Duration {
	return s.timeOffset(s.clock.Now())
}

// timeOffset returns the time offset in the reprovide cycle for the given
// time.
func (s *SweepingProvider) timeOffset(t time.Time) time.Duration {
	return t.Sub(s.cycleStart) % s.reprovideInterval
}

// timeUntil returns the time left (duration) until the given time offset.
func (s *SweepingProvider) timeUntil(d time.Duration) time.Duration {
	return s.timeBetween(s.currentTimeOffset(), d)
}

// timeBetween returns the duration between the two provided offsets, assuming
// it is no more than s.reprovideInterval.
func (s *SweepingProvider) timeBetween(from, to time.Duration) time.Duration {
	return (to-from+s.reprovideInterval-1)%s.reprovideInterval + 1
}

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
func (s *SweepingProvider) reprovideTimeForPrefix(prefix bitstr.Key) time.Duration {
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

const initialGetClosestPeers = 4

// measureInitialPrefixLen makes a few GetClosestPeers calls to get an estimate
// of the prefix length to be used in the network.
//
// This function blocks until GetClosestPeers succeeds or the provider is
// closed. No provide operation can happen until this function returns.
func (s *SweepingProvider) measureInitialPrefixLen() {
	cplSum := atomic.Int32{}
	cplSamples := atomic.Int32{}
	wg := sync.WaitGroup{}
	wg.Add(initialGetClosestPeers)
	for range initialGetClosestPeers {
		go func() {
			defer wg.Done()
			randomMh := random.Multihashes(1)[0]
			for {
				if s.closed() {
					return
				}
				peers, err := s.router.GetClosestPeers(context.Background(), string(randomMh))
				if err != nil {
					logger.Infof("GetClosestPeers failed during initial prefix len measurement: %s", err)
				} else if len(peers) == 0 {
					logger.Info("GetClosestPeers found not peers during initial prefix len measurement")
				} else {
					if len(peers) <= 2 {
						return // Ignore result if only 2 other peers in DHT.
					}
					cpl := keyspace.KeyLen
					firstPeerKey := keyspace.PeerIDToBit256(peers[0])
					for _, p := range peers[1:] {
						cpl = min(cpl, key.CommonPrefixLength(firstPeerKey, keyspace.PeerIDToBit256(p)))
					}
					cplSum.Add(int32(cpl))
					cplSamples.Add(1)
					return
				}

				s.clock.Sleep(time.Second) // retry every second until success
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
	close(s.avgPrefixLenReady)
}

// getAvgPrefixLenNoLock returns the average prefix length of all scheduled
// prefixes.
//
// Hangs until the first measurement is done if the average prefix length is
// missing.
func (s *SweepingProvider) getAvgPrefixLenNoLock() int {
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
		for _, entry := range keyspace.AllEntries(s.schedule, s.order) {
			prefixLenSum += len(entry.Key)
		}
		s.cachedAvgPrefixLen = prefixLenSum / scheduleSize
		s.lastAvgPrefixLen = s.clock.Now()
	}
	return s.cachedAvgPrefixLen
}

// vanillaProvide provides a single key to the network without any
// optimization. It should be used for providing a small number of keys
// (typically 1 or 2), because exploring the keyspace would add too much
// overhead for a small number of keys.
func (s *SweepingProvider) vanillaProvide(k mh.Multihash) (bitstr.Key, error) {
	// Add provider record to local provider store.
	s.addLocalRecord(k)
	// Get peers to which the record will be allocated.
	peers, err := s.router.GetClosestPeers(context.Background(), string(k))
	if err != nil {
		return "", err
	}
	coveredPrefix, _ := keyspace.ShortestCoveredPrefix(bitstr.Key(key.BitString(keyspace.MhToBit256(k))), peers)
	addrInfo := peer.AddrInfo{ID: s.peerid, Addrs: s.getSelfAddrs()}
	keysAllocations := make(map[peer.ID][]mh.Multihash)
	for _, p := range peers {
		keysAllocations[p] = []mh.Multihash{k}
	}
	return coveredPrefix, s.sendProviderRecords(keysAllocations, addrInfo)
}

// exploreSwarm finds all peers whose kademlia identifier matches `prefix` in
// the DHT swarm, and organizes them in keyspace regions.
//
// A region is identified by a keyspace prefix, and contains all the peers
// matching this prefix. A region always has at least s.replicationFactor
// peers. Regions are non-overlapping.
//
// If there less than s.replicationFactor peers match `prefix`, explore
// shorter prefixes until at least s.replicationFactor peers are included in
// the region.
//
// The returned `coveredPrefix` represents the keyspace prefix covered by all
// returned regions combined. It is different to the supplied `prefix` if there
// aren't enough peers matching `prefix`.
func (s *SweepingProvider) exploreSwarm(prefix bitstr.Key) (regions []keyspace.Region, coveredPrefix bitstr.Key, err error) {
	peers, err := s.closestPeersToPrefix(prefix)
	if err != nil {
		return nil, "", fmt.Errorf("exploreSwarm '%s': %w", prefix, err)
	}
	if len(peers) == 0 {
		return nil, "", fmt.Errorf("no peers found when exploring prefix %s", prefix)
	}
	regions, coveredPrefix = keyspace.RegionsFromPeers(peers, s.replicationFactor, s.order)
	return regions, coveredPrefix, nil
}

// maxPrefixSearches is the maximum number of GetClosestPeers operations that
// are allowed to explore a prefix, preventing an infinite loop, since the exit
// condition depends on the network topology.
//
// A lower bound estimate on the number of fresh peers returned by GCP is
// replicationFactor/2. Hence, 64 GCP are expected to return at least
// 32*replicatonFactor peers, which should be more than enough, even if the
// supplied prefix is too short.
const maxPrefixSearches = 64

// closestPeersToPrefix returns at least s.replicationFactor peers
// corresponding to the branch of the network peers trie matching the provided
// prefix. In the case there aren't enough peers matching the provided prefix,
// it will find and return the closest peers to the prefix, even if they don't
// exactly match it.
func (s *SweepingProvider) closestPeersToPrefix(prefix bitstr.Key) ([]peer.ID, error) {
	allClosestPeers := make(map[peer.ID]struct{})

	nextPrefix := prefix
	startTime := time.Now()
	coveredPrefixesStack := []bitstr.Key{}

	i := 0
	// Go down the trie to fully cover prefix.
exploration:
	for {
		if i == maxPrefixSearches {
			return nil, errors.New("closestPeersToPrefix needed more than maxPrefixSearches iterations")
		}
		if !s.connectivity.IsOnline() {
			return nil, errors.New("provider: node is offline")
		}
		i++
		fullKey := keyspace.FirstFullKeyWithPrefix(nextPrefix, s.order)
		closestPeers, err := s.closestPeersToKey(fullKey)
		if err != nil {
			// We only get an err if something really bad happened, e.g no peers in
			// routing table, invalid key, etc.
			return nil, err
		}
		if len(closestPeers) == 0 {
			return nil, errors.New("dht lookup did not return any peers")
		}
		coveredPrefix, coveredPeers := keyspace.ShortestCoveredPrefix(fullKey, closestPeers)
		for _, p := range coveredPeers {
			allClosestPeers[p] = struct{}{}
		}

		coveredPrefixLen := len(coveredPrefix)
		if i == 1 {
			if coveredPrefixLen <= len(prefix) && coveredPrefix == prefix[:coveredPrefixLen] && len(allClosestPeers) >= s.replicationFactor {
				// Exit early if the prefix is fully covered at the first request and
				// we have enough (at least replicationFactor) peers.
				break exploration
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
					if coveredPrefixLen <= len(prefix) && len(allClosestPeers) >= s.replicationFactor {
						break exploration
					}
					// Not enough peers -> add coveredPrefix to stack and continue.
					break
				}
				if coveredPrefixLen == 0 {
					logger.Error("coveredPrefixLen==0, coveredPrefixStack ", coveredPrefixesStack)
					break exploration
				}
				latestPrefix = coveredPrefixesStack[len(coveredPrefixesStack)-1]
			}
		}
		// Push coveredPrefix to stack
		coveredPrefixesStack = append(coveredPrefixesStack, coveredPrefix)
		// Flip last bit of last covered prefix
		nextPrefix = keyspace.FlipLastBit(coveredPrefixesStack[len(coveredPrefixesStack)-1])
	}

	peers := make([]peer.ID, 0, len(allClosestPeers))
	for p := range allClosestPeers {
		peers = append(peers, p)
	}
	logger.Debugf("Region %s exploration required %d requests to discover %d peers in %s", prefix, i, len(allClosestPeers), time.Since(startTime))
	return peers, nil
}

// closestPeersToKey returns a valid peer ID sharing a long common prefix with
// the provided key. Note that the returned peer IDs aren't random, they are
// taken from a static list of preimages.
func (s *SweepingProvider) closestPeersToKey(k bitstr.Key) ([]peer.ID, error) {
	p, _ := kb.GenRandPeerIDWithCPL(keyspace.KeyToBytes(k), kb.PeerIDPreimageMaxCpl)
	return s.router.GetClosestPeers(context.Background(), string(p))
}

const minimalRegionReachablePeersRatio float32 = 0.2

type provideJob struct {
	pid  peer.ID
	keys []mh.Multihash
}

// sendProviderRecords manages reprovides for all given peer ids and allocated
// keys. Upon failure to reprovide a key, or to connect to a peer, it will NOT
// retry.
//
// Returns an error if we were unable to reprovide keys to a given threshold of
// peers. In this case, the region reprovide is considered failed and the
// caller is responsible for trying again. This allows detecting if we are
// offline.
func (s *SweepingProvider) sendProviderRecords(keysAllocations map[peer.ID][]mh.Multihash, addrInfo peer.AddrInfo) error {
	nPeers := len(keysAllocations)
	if nPeers == 0 {
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
				err := s.provideKeysToPeer(job.pid, job.keys, pmes)
				if err != nil {
					errCount.Add(1)
				}
			}
		}()
	}

	for p, keys := range keysAllocations {
		jobChan <- provideJob{p, keys}
	}
	close(jobChan)
	wg.Wait()

	errCountLoaded := int(errCount.Load())
	logger.Infof("sent provider records to peers in %s, errors %d/%d", s.clock.Since(startTime), errCountLoaded, len(keysAllocations))

	if errCountLoaded == nPeers || errCountLoaded > int(float32(nPeers)*(1-minimalRegionReachablePeersRatio)) {
		return fmt.Errorf("unable to provide to enough peers (%d/%d)", nPeers-errCountLoaded, nPeers)
	}
	return nil
}

// genProvideMessage generates a new provide message with the supplied
// AddrInfo. The message contains no keys, as they will be set later before
// sending the message.
func genProvideMessage(addrInfo peer.AddrInfo) *pb.Message {
	pmes := pb.NewMessage(pb.Message_ADD_PROVIDER, []byte{}, 0)
	pmes.ProviderPeers = pb.RawPeerInfosToPBPeers([]peer.AddrInfo{addrInfo})
	return pmes
}

// maxConsecutiveProvideFailuresAllowed is the maximum number of consecutive
// provides that are allowed to fail to the same remote peer before cancelling
// all pending requests to this peer.
const maxConsecutiveProvideFailuresAllowed = 2

// provideKeysToPeer performs the network operation to advertise to the given
// DHT server (p) that we serve all the given keys.
func (s *SweepingProvider) provideKeysToPeer(p peer.ID, keys []mh.Multihash, pmes *pb.Message) error {
	errCount := 0
	for _, mh := range keys {
		pmes.Key = mh
		err := s.msgSender.SendMessage(context.Background(), p, pmes)
		if err != nil {
			errCount++

			if errCount == len(keys) || errCount > maxConsecutiveProvideFailuresAllowed {
				return fmt.Errorf("failed to provide to %s: %s", p, err.Error())
			}
		} else if errCount > 0 {
			// Reset error count
			errCount = 0
		}
	}
	return nil
}

// handleProvide provides supplied keys to the network if needed and schedules
// the keys to be reprovided if needed.
func (s *SweepingProvider) handleProvide(force, reprovide bool, keys ...mh.Multihash) {
	if len(keys) == 0 {
		return
	}
	if reprovide {
		// Add keys to list of keys to be reprovided. Returned keys are deduplicated
		// newly added keys.
		newKeys, err := s.keyStore.Put(context.Background(), keys...)
		if err != nil {
			logger.Errorf("couldn't add keys to keystore: %s", err)
			return
		}
		if !force {
			keys = newKeys
		}
	}

	prefixes := s.groupAndScheduleKeysByPrefix(keys, reprovide)
	if len(prefixes) == 0 {
		return
	}
	// Sort prefixes by number of keys.
	sortedPrefixesAndKeys := keyspace.SortPrefixesBySize(prefixes)
	// Add keys to the provide queue.
	for _, prefixAndKeys := range sortedPrefixesAndKeys {
		s.provideQueue.Enqueue(prefixAndKeys.Prefix, prefixAndKeys.Keys...)
	}

	go s.provideLoop()
}

// groupAndScheduleKeysByPrefix groups the supplied keys by their prefixes as
// present in the schedule, and if `schedule` is set to true, add these
// prefixes to the schedule to be reprovided.
func (s *SweepingProvider) groupAndScheduleKeysByPrefix(keys []mh.Multihash, schedule bool) map[bitstr.Key][]mh.Multihash {
	seen := make(map[string]struct{})
	prefixTrie := trie.New[bitstr.Key, struct{}]()
	prefixes := make(map[bitstr.Key][]mh.Multihash)
	avgPrefixLen := -1

	s.scheduleLk.Lock()
	defer s.scheduleLk.Unlock()
	for _, h := range keys {
		k := keyspace.MhToBit256(h)
		kStr := string(keyspace.KeyToBytes(k))
		// Don't add duplicates
		if _, ok := seen[kStr]; ok {
			continue
		}
		seen[kStr] = struct{}{}

		if prefix, ok := keyspace.FindPrefixOfKey(prefixTrie, k); ok {
			prefixes[prefix] = append(prefixes[prefix], h)
		} else {
			if prefix, ok = keyspace.FindPrefixOfKey(s.schedule, k); !ok {
				if avgPrefixLen == -1 {
					avgPrefixLen = s.getAvgPrefixLenNoLock()
				}
				prefix = bitstr.Key(key.BitString(k)[:avgPrefixLen])
				if schedule {
					s.schedulePrefixNoLock(prefix, false)
				}
			}
			mhs := []mh.Multihash{h}
			if subtrie, ok := keyspace.FindSubtrie(prefixTrie, prefix); ok {
				// If prefixes already contains superstrings of prefix, consolidate the
				// keys to prefix.
				for _, entry := range keyspace.AllEntries(subtrie, s.order) {
					mhs = append(mhs, prefixes[entry.Key]...)
					delete(prefixes, entry.Key)
				}
				keyspace.PruneSubtrie(prefixTrie, prefix)
			}
			prefixTrie.Add(prefix, struct{}{})
			prefixes[prefix] = mhs
		}
	}
	return prefixes
}

// provideLoop is the loop providing keys to the DHT swarm as long as the
// provide queue isn't empty.
//
// The s.provideRunning mutex prevents concurrent executions of the loop.
func (s *SweepingProvider) provideLoop() {
	if !s.provideRunning.TryLock() {
		// Ensure that only one goroutine is running the provide loop at a time.
		return
	}
	defer s.provideRunning.Unlock()

	for !s.provideQueue.IsEmpty() {
		if s.closed() {
			// Exit loop if provider is closed.
			return
		}
		if !s.connectivity.IsOnline() {
			// Don't try to provide if node is offline.
			return
		}
		// Block until we can acquire a worker from the pool.
		err := s.workerPool.Acquire(burstWorker)
		if err != nil {
			// Provider was closed while waiting for a worker.
			return
		}
		prefix, keys, ok := s.provideQueue.Dequeue()
		if ok {
			go func(prefix bitstr.Key, keys []mh.Multihash) {
				defer s.workerPool.Release(burstWorker)
				s.batchProvide(prefix, keys)
			}(prefix, keys)
		} else {
			s.workerPool.Release(burstWorker)
		}
	}
}

func (s *SweepingProvider) batchProvide(prefix bitstr.Key, keys []mh.Multihash) {
	if len(keys) == 0 {
		return
	}
	addrInfo, ok := s.selfAddrInfo()
	if !ok {
		// Don't provide if the node doesn't have a valid address to include in the
		// provider record.
		return
	}
	if len(keys) <= individualProvideThreshold {
		// Don't fully explore the region, execute simple DHT provides for these
		// keys. It isn't worth it to fully explore a region for just a few keys.
		s.individualProvide(prefix, keys, false, false)
		return
	}

	regions, coveredPrefix, err := s.exploreSwarm(prefix)
	if err != nil {
		s.failedProvide(prefix, keys, fmt.Errorf("reprovide '%s': %w", prefix, err))
		return
	}
	logger.Debugf("provide: requested prefix '%s' (len %d), prefix covered '%s' (len %d)", prefix, len(prefix), coveredPrefix, len(coveredPrefix))

	// Add any key matching the covered prefix from the provide queue to the
	// current provide batch.
	extraKeys := s.provideQueue.DequeueMatching(coveredPrefix)
	keys = append(keys, extraKeys...)
	regions = keyspace.AssignKeysToRegions(regions, keys)

	if !s.provideRegions(regions, addrInfo, false, false) {
		logger.Errorf("failed to reprovide any region for prefix %s", prefix)
	}
}

func (s *SweepingProvider) batchReprovide(prefix bitstr.Key, periodicReprovide bool) {
	addrInfo, ok := s.selfAddrInfo()
	if !ok {
		// Don't provide if the node doesn't have a valid address to include in the
		// provider record.
		return
	}

	// Load keys matching prefix from the keystore.
	keys, err := s.keyStore.Get(context.Background(), prefix)
	if err != nil {
		s.failedReprovide(prefix, fmt.Errorf("couldn't reprovide, error when loading keys: %s", err))
		if periodicReprovide {
			s.reschedulePrefix(prefix)
		}
		return
	}
	if len(keys) == 0 {
		logger.Infof("No keys to reprovide for prefix %s", prefix)
		return
	}
	if len(keys) <= individualProvideThreshold {
		// Don't fully explore the region, execute simple DHT provides for these
		// keys. It isn't worth it to fully explore a region for just a few keys.
		s.individualProvide(prefix, keys, true, periodicReprovide)
		return
	}

	regions, coveredPrefix, err := s.exploreSwarm(prefix)
	if err != nil {
		s.failedReprovide(prefix, fmt.Errorf("reprovide '%s': %w", prefix, err))
		if periodicReprovide {
			s.reschedulePrefix(prefix)
		}
		return
	}
	logger.Debugf("reprovide: requested prefix '%s' (len %d), prefix covered '%s' (len %d)", prefix, len(prefix), coveredPrefix, len(coveredPrefix))

	regions = s.claimRegionReprovide(regions)

	// Remove all keys matching coveredPrefix from provide queue. No need to
	// provide them anymore since they are about to be reprovided.
	s.provideQueue.DequeueMatching(coveredPrefix)
	// Remove covered prefix from the reprovide queue, so since we are about the
	// reprovide the region.
	s.reprovideQueue.Remove(coveredPrefix)

	// When reproviding a region, remove all scheduled regions starting with
	// the currently covered prefix.
	s.scheduleLk.Lock()
	s.unscheduleSubsumedPrefixesNoLock(coveredPrefix)
	s.scheduleLk.Unlock()

	if len(coveredPrefix) < len(prefix) {
		// Covered prefix is shorter than the requested one, load all the keys
		// matching the covered prefix from the keystore.
		keys, err = s.keyStore.Get(context.Background(), coveredPrefix)
		if err != nil {
			err = fmt.Errorf("couldn't reprovide, error when loading keys: %s", err)
			s.failedReprovide(prefix, err)
			if periodicReprovide {
				s.reschedulePrefix(prefix)
			}
		}
	}
	regions = keyspace.AssignKeysToRegions(regions, keys)

	if !s.provideRegions(regions, addrInfo, true, periodicReprovide) {
		logger.Errorf("failed to reprovide any region for prefix %s", prefix)
	}
}

func (s *SweepingProvider) failedProvide(prefix bitstr.Key, keys []mh.Multihash, err error) {
	logger.Error(err)
	// Put keys back to the provide queue.
	s.provideQueue.Enqueue(prefix, keys...)

	s.connectivity.TriggerCheck()
}

func (s *SweepingProvider) failedReprovide(prefix bitstr.Key, err error) {
	logger.Error(err)
	// Put prefix in the reprovide queue.
	s.reprovideQueue.Enqueue(prefix)

	s.connectivity.TriggerCheck()
}

// selfAddrInfo returns the current peer.AddrInfo to be used in the provider
// records sent to remote peers.
//
// If the node currently has no valid multiaddress, return an empty AddrInfo
// and false.
func (s *SweepingProvider) selfAddrInfo() (peer.AddrInfo, bool) {
	addrs := s.getSelfAddrs()
	if len(addrs) == 0 {
		logger.Warn("provider: no self addresses available for providing keys")
		return peer.AddrInfo{}, false
	}
	return peer.AddrInfo{ID: s.peerid, Addrs: addrs}, true
}

// individualProvide provides the keys sharing the same prefix to the network
// without exploring the associated keyspace regions. It performs "normal" DHT
// provides for the supplied keys, handles failures and schedules next
// reprovide is necessary.
func (s *SweepingProvider) individualProvide(prefix bitstr.Key, keys []mh.Multihash, reprovide bool, periodicReprovide bool) {
	if len(keys) == 0 {
		return
	}

	var provideErr error
	if len(keys) == 1 {
		coveredPrefix, err := s.vanillaProvide(keys[0])
		if err == nil {
			s.provideCounter.Add(context.Background(), 1)
		} else if !reprovide {
			// Put the key back in the provide queue.
			s.failedProvide(prefix, keys, fmt.Errorf("individual provide failed for prefix '%s', %w", prefix, err))
		}
		provideErr = err
		if periodicReprovide {
			// Schedule next reprovide for the prefix that was actually covered by
			// the GCP, otherwise we may schedule a reprovide for a prefix too short
			// or too long.
			s.reschedulePrefix(coveredPrefix)
		}
	} else {
		wg := sync.WaitGroup{}
		success := atomic.Bool{}
		for _, key := range keys {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, err := s.vanillaProvide(key)
				if err == nil {
					s.provideCounter.Add(context.Background(), 1)
					success.Store(true)
				} else if !reprovide {
					// Individual provide failed, put key back in provide queue.
					s.failedProvide(prefix, []mh.Multihash{key}, err)
				}
			}()
		}
		wg.Wait()

		if !success.Load() {
			// Only errors if all provides failed.
			provideErr = fmt.Errorf("all individual provides failed for prefix %s", prefix)
		}
		if periodicReprovide {
			s.reschedulePrefix(prefix)
		}
	}
	if reprovide && provideErr != nil {
		s.failedReprovide(prefix, provideErr)
	}
}

// provideRegions contains common logic to batchProvide() and batchReprovide().
// It iterate over supplied regions, and allocates the regions provider records
// to the appropriate DHT servers.
func (s *SweepingProvider) provideRegions(regions []keyspace.Region, addrInfo peer.AddrInfo, reprovide, periodicReprovide bool) bool {
	errCount := 0
	for _, r := range regions {
		nKeys := r.Keys.Size()
		if nKeys == 0 {
			if reprovide {
				s.releaseRegionReprovide(r.Prefix)
			}
			continue
		}
		// Add keys to local provider store
		for _, h := range keyspace.AllValues(r.Keys, s.order) {
			s.addLocalRecord(h)
		}
		keysAllocations := keyspace.AllocateToKClosest(r.Keys, r.Peers, s.replicationFactor)
		err := s.sendProviderRecords(keysAllocations, addrInfo)
		if reprovide {
			s.releaseRegionReprovide(r.Prefix)
			if periodicReprovide {
				s.reschedulePrefix(r.Prefix)
			}
		}
		if err != nil {
			errCount++
			err = fmt.Errorf("cannot send provider records for region %s: %s", r.Prefix, err)
			if reprovide {
				s.failedReprovide(r.Prefix, err)
			} else { // provide operation
				s.failedProvide(r.Prefix, keyspace.AllValues(r.Keys, s.order), err)
			}
			continue
		}
		s.provideCounter.Add(context.Background(), int64(nKeys))

	}
	// If at least 1 regions was provided, we don't consider it a failure.
	return errCount < len(regions)
}

// claimRegionReprovide checks if the region is already being reprovided by
// another thread. If not it marks the region as being currently reprovided.
func (s *SweepingProvider) claimRegionReprovide(regions []keyspace.Region) []keyspace.Region {
	out := regions[:0]
	s.ongoingReprovidesLk.Lock()
	defer s.ongoingReprovidesLk.Unlock()
	for _, r := range regions {
		if r.Peers.IsEmptyLeaf() {
			continue
		}
		if _, ok := keyspace.FindPrefixOfKey(s.ongoingReprovides, r.Prefix); !ok {
			// Prune superstrings of r.Prefix if any
			keyspace.PruneSubtrie(s.ongoingReprovides, r.Prefix)
			out = append(out, r)
			s.ongoingReprovides.Add(r.Prefix, struct{}{})
		}
	}
	return out
}

// releaseRegionReprovide marks the region as no longer being reprovided.
func (s *SweepingProvider) releaseRegionReprovide(prefix bitstr.Key) {
	s.ongoingReprovidesLk.Lock()
	defer s.ongoingReprovidesLk.Unlock()
	s.ongoingReprovides.Remove(prefix)
}

// ProvideOnce only sends provider records for the given keys out to the DHT
// swarm. It does NOT take the responsibility to reprovide these keys.
func (s *SweepingProvider) ProvideOnce(keys ...mh.Multihash) {
	s.handleProvide(true, false, keys...)
}

// StartProviding provides the given keys to the DHT swarm unless they were
// already provided in the past. The keys will be periodically reprovided until
// StopProviding is called for the same keys or user defined garbage collection
// deletes the keys.
func (s *SweepingProvider) StartProviding(force bool, keys ...mh.Multihash) {
	s.handleProvide(force, true, keys...)
}

// StopProviding stops reproviding the given keys to the DHT swarm. The node
// stops being referred as a provider when the provider records in the DHT
// swarm expire.
func (s *SweepingProvider) StopProviding(keys ...mh.Multihash) {
	err := s.keyStore.Delete(context.Background(), keys...)
	if err != nil {
		logger.Errorf("failed to stop providing keys: %s", err)
	}
	s.provideQueue.Remove(keys...)
}

// ClearProvideQueue clears the all the keys from the provide queue and returns
// the number of keys that were cleared.
func (s *SweepingProvider) ClearProvideQueue() int {
	return s.provideQueue.Clear()
}

// ProvideState encodes the current relationship between this node and `key`.
type ProvideState uint8

const (
	StateUnknown  ProvideState = iota // we have no record of the key
	StateQueued                       // key is queued to be provided
	StateProvided                     // key was provided at least once
)

// ProvideStatus reports the provider’s view of a key.
//
// When `state == StateProvided`, `lastProvide` is the wall‑clock time of the
// most recent successful provide operation (UTC).
// For `StateQueued` or `StateUnknown`, `lastProvide` is the zero `time.Time`.
func (s *SweepingProvider) ProvideStatus(key mh.Multihash) (state ProvideState, lastProvide time.Time) {
	// TODO: implement me
	return StateUnknown, time.Time{}
}
