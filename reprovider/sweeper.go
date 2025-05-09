package reprovider

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gammazero/deque"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"

	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
	kbucket "github.com/libp2p/go-libp2p-kbucket"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multihash"

	"github.com/probe-lab/go-libdht/kad/key"
	"github.com/probe-lab/go-libdht/kad/key/bit256"
	"github.com/probe-lab/go-libdht/kad/key/bitstr"
	"github.com/probe-lab/go-libdht/kad/trie"
)

// Provider announces blocks to the network
type Provider interface {
	// Provide takes a cid and makes an attempt to announce it to the network
	Provide(context.Context, cid.Cid, bool) error
}

type ProvideMany interface {
	ProvideMany(ctx context.Context, keys []multihash.Multihash) error
}

var (
	_ Provider    = &reprovideSweeper{}
	_ ProvideMany = &reprovideSweeper{}
)

type KadRouter interface {
	GetClosestPeers(context.Context, string) ([]peer.ID, error)
	Provide(context.Context, cid.Cid, bool) error
}

// TODO: add some more logging
var logger = logging.Logger("dht/ReprovideSweep")

var ErrTooManyIterationsDuringExploration = errors.New("closestPeersToPrefix needed more than maxPrefixSearches iterations")

// TODO: support resuming reprovide service after a restart
// persist cids to disk
// persist prefixes waiting to be reprovided and last reprovided prefix (with time) on disk

// TODO: add queue of cids waiting to be reprovided (if node offline, etc.)

type provideReq struct {
	ctx  context.Context
	cids []multihash.Multihash
	done chan error
}

type reprovideSweeper struct {
	ctx    context.Context
	host   host.Host // NOTE: maybe useless
	router KadRouter
	order  bit256.Key

	online atomic.Bool

	replicationFactor int
	now               func() time.Time
	cycleStart        time.Time
	reprovideInterval time.Duration
	maxReprovideDelay time.Duration

	cids   *trie.Trie[bit256.Key, multihash.Multihash]
	cidsLk *sync.Mutex // NOTE: probably useless

	provideChan   chan provideReq
	schedule      *trie.Trie[bitstr.Key, time.Duration]
	scheduleLk    *sync.Mutex
	scheduleTimer *time.Timer

	failedRegionsChan chan bitstr.Key
	lateRegionsQueue  deque.Deque[bitstr.Key]
	retryTimer        *time.Timer

	prefixCursor bitstr.Key

	msgSender    pb.MessageSender
	getSelfAddrs func() peer.AddrInfo
}

// Options should be
// * reprovideInterval
// * maxReprovideDelay
// * now (maybe not even an option)
// * message sender

func NewReproviderSweeper(ctx context.Context, host host.Host, router KadRouter,
	now func() time.Time, reprovideInterval, maxReprovideDelay time.Duration,
) Provider {
	// TODO: options
	sweeper := &reprovideSweeper{
		host:   host,
		router: router,
		order:  peerIDToBit256(host.ID()),

		now:               now,
		cycleStart:        now(),
		reprovideInterval: reprovideInterval,
		maxReprovideDelay: maxReprovideDelay,

		cids:   trie.New[bit256.Key, multihash.Multihash](),
		cidsLk: &sync.Mutex{},

		provideChan:   make(chan provideReq),
		schedule:      trie.New[bitstr.Key, time.Duration](),
		scheduleLk:    &sync.Mutex{},
		scheduleTimer: time.NewTimer(time.Hour),

		failedRegionsChan: make(chan bitstr.Key),
		lateRegionsQueue:  deque.Deque[bitstr.Key]{},
		retryTimer:        time.NewTimer(time.Hour),

		msgSender:    nil,                                                                               // TODO: default msgSender should allow some pipielining
		getSelfAddrs: func() peer.AddrInfo { return peer.AddrInfo{ID: host.ID(), Addrs: host.Addrs()} }, // NOTE: addrs filters should be passed here
	}
	// Don't need to start the timers yet
	sweeper.scheduleTimer.Stop()
	sweeper.retryTimer.Stop()
	// Start in online state
	sweeper.online.Store(true)

	go sweeper.run()
	return sweeper
}

func (s *reprovideSweeper) run() {
	for {
		select {
		case <-s.ctx.Done():
			return
		case provideRequest := <-s.provideChan:
			s.handleProvide(provideRequest)
		case <-s.scheduleTimer.C:
			s.handleReprovide()
		case prefix := <-s.failedRegionsChan:
			if s.lateRegionsQueue.Len() == 0 {
				// TODO: start periodical checks
			}
			s.lateRegionsQueue.PushBack(prefix)
			// TODO: make check to understand if we are online, if not, stop all reprovide retries
		case <-s.retryTimer.C:
			// TODO:
			// 1. Make check to understand if we are online
			// 2. If online, use all available workers to reprovide everything we can
		}
	}
}

func (s *reprovideSweeper) provideOne(provideRequest provideReq) {
	if len(provideRequest.cids) != 1 {
		return
	}
	c := provideRequest.cids[0]
	mh := mhToBit256(c)
	s.cidsLk.Lock()
	if added := s.cids.Add(mh, c); !added {
		s.cidsLk.Unlock()
		// cid is already being provided
		provideRequest.done <- nil
		return
	}
	s.cidsLk.Unlock()

	go func() {
		err := s.router.Provide(provideRequest.ctx, cid.NewCidV0(c), true)
		provideRequest.done <- err
	}()

	scheduleKey := bitstr.Key(key.BitString(mh))
	s.addPrefixToSchedule(scheduleKey)
}

// addPrefixToSchedule adds the given prefix to the schedule if it is not
// already in.
func (s *reprovideSweeper) addPrefixToSchedule(prefix bitstr.Key) {
	s.scheduleLk.Lock()
	defer s.scheduleLk.Unlock()
	if ok, _ := trieHasPrefixOfKey(s.schedule, prefix); !ok {
		// Cid not covered by schedule yet, add it.
		reprovideTime := s.reprovideTimeForPrefix(prefix)
		s.schedule.Add(prefix, reprovideTime)

		followingKey := nextNonEmptyLeaf(s.schedule, prefix, s.order).Key
		if s.prefixCursor == "" || s.prefixCursor == followingKey {
			// Next prefix to be reprovided is current prefix. Advance the alarm.
			s.prefixCursor = prefix
			s.scheduleTimer.Reset(reprovideTime)
		}
	}
}

func (s *reprovideSweeper) getAvgPrefixLen() int {
	prefixLenSum := 0
	s.scheduleLk.Lock()
	defer s.scheduleLk.Unlock()
	scheduleSize := s.schedule.Size()
	if scheduleSize == 0 {
		// TODO: get number from routing table
		return 10
	}
	for _, entry := range allEntries(s.schedule, s.order) {
		prefixLenSum += len(entry.Key)
	}
	return prefixLenSum / scheduleSize
}

func (s *reprovideSweeper) handleProvide(provideRequest provideReq) {
	if len(provideRequest.cids) == 0 {
		provideRequest.done <- nil
		return
	}

	if !s.online.Load() {
		// TODO: handle offline
	}
	if len(provideRequest.cids) == 1 {
		s.provideOne(provideRequest)
		return
	}

	// ProvideMany
	prefixes := make(map[bitstr.Key]*trie.Trie[bit256.Key, multihash.Multihash])
	avgPrefixLen := s.getAvgPrefixLen()
	s.cidsLk.Lock()
	for _, c := range provideRequest.cids {
		mh := mhToBit256(c)
		// Add cid to s.cids if not there already, and add fresh cids to new cids
		// trie. Deduplication is handled here.
		if added := s.cids.Add(mh, c); added {
			// Spread cids by prefixes of avgPrefixLen
			prefix := bitstr.Key(key.BitString(mh)[:avgPrefixLen])
			if _, ok := prefixes[prefix]; !ok {
				prefixes[prefix] = trie.New[bit256.Key, multihash.Multihash]()
			}
			prefixes[prefix].Add(mh, c)
		}
	}
	s.cidsLk.Unlock()

	sortedPrefixes := sortPrefixesBySize(prefixes)

	nWorkers := 8
	jobChan := make(chan prefixAndCids, nWorkers)
	wg := sync.WaitGroup{}

	// Start workers to handle the provides.
	for range nWorkers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for p := range jobChan {
				s.provideForPrefix(p.prefix, p.cids, false)
			}
		}()
	}

	// Feed jobs to the workers.
	go func() {
		defer close(jobChan)
		for _, p := range sortedPrefixes {
			select {
			case <-s.ctx.Done():
				return
			case <-provideRequest.ctx.Done():
				return
			case jobChan <- p:
			}
		}
	}()

	// Wait for workers to finish before reporting completion.
	go func() {
		wg.Wait()
		provideRequest.done <- nil
	}()
}

func (s *reprovideSweeper) handleReprovide() {
	s.scheduleLk.Lock()
	// Remove prefix from trie, new schedule will be added as needed.
	s.schedule.Remove(s.prefixCursor)
	next := nextNonEmptyLeaf(s.schedule, s.prefixCursor, s.order)
	currentPrefix := s.prefixCursor

	s.prefixCursor = next.Key
	nextReprovideDelay := next.Data - s.currentTimeOffset()
	s.scheduleTimer.Reset(nextReprovideDelay)
	s.scheduleLk.Unlock()

	s.cidsLk.Lock()
	cids := s.cids.Copy() // NOTE: if many cids, this may have a large memory footprint
	s.cidsLk.Unlock()

	go s.provideForPrefix(currentPrefix, cids, true)
}

func (s *reprovideSweeper) currentTimeOffset() time.Duration {
	return s.now().Sub(s.cycleStart) % s.reprovideInterval
}

type region struct {
	prefix bitstr.Key
	peers  *trie.Trie[bit256.Key, peer.ID]
	cids   *trie.Trie[bit256.Key, multihash.Multihash]
}

// returned regions ordered according to s.order
func (s *reprovideSweeper) regionsFromPeers(peers []peer.ID, cids *trie.Trie[bit256.Key, multihash.Multihash]) []region {
	peersTrie := trie.New[bit256.Key, peer.ID]()
	for _, p := range peers {
		k := peerIDToBit256(p)
		peersTrie.Add(k, p)
	}
	regions := extractMinimalRegions(peersTrie, "", s.replicationFactor, s.order)
	for i, r := range regions {
		regions[i].cids, _ = subtrieMatchingPrefix(cids, r.prefix)
	}
	return regions
}

func (s *reprovideSweeper) handleReprovideError(prefix bitstr.Key, err error, regularReprovide bool) {
	logger.Error(err)
	s.failedRegionsChan <- prefix
	// We should schedule the next reprovide even if there was an error during
	// the reprovide. Otherwise the schedule for this region will deviate.
	s.scheduleNextReprovide(prefix, s.currentTimeOffset(), regularReprovide)
}

func (s *reprovideSweeper) provideForPrefix(prefix bitstr.Key, cids *trie.Trie[bit256.Key, multihash.Multihash], regularReprovide bool) bool {
	// TODO: if 1 or 2 cids only match prefix, then provide them individually
	peers, err := s.closestPeersToPrefix(prefix)
	if err != nil {
		if err != ErrTooManyIterationsDuringExploration {
			s.handleReprovideError(prefix, err, regularReprovide)
			return false
		}
		logger.Warnf("prefix key exploration not complete: %s", prefix)
	}
	if len(peers) == 0 {
		s.handleReprovideError(prefix, errors.New("no peer found when exploring prefix "+string(prefix)), regularReprovide)
		return false
	}

	regions := s.regionsFromPeers(peers, cids)
	// TODO: pop all scheduled prefixes matching any of the above regions
	// TODO: check if any reprovide is ongoing for any region, and if yes abort according regions
	// maybe logic needs to be implemented in regionsFromPeers
	// TODO: if ErrTooManyIterationsDuringExploration, schedule missing prefixes
	errCount := 0
	for _, r := range regions {
		s.addCidsToLocalProviderStore(r.cids)
		err := s.regionReprovide(r)
		if err != nil {
			errCount++
			s.handleReprovideError(r.prefix, err, regularReprovide)
			continue
		}
		s.scheduleNextReprovide(r.prefix, s.currentTimeOffset(), regularReprovide)
		// TODO: persist to datastore that region identified by prefix was reprovided `now`, only during regular reprovides?
	}
	// returns true if at least one region was successfully reprovided
	return errCount < len(regions)
}

// closestPeersToPrefix returns more than s.replicationFactor peers
// corresponding to the branch of the network peers trie matching the provided
// prefix. In the case there aren't enough peers matching the provided prefix,
// it will find and return the closest peers to the prefix, even if they don't
// exactly match it.
func (s *reprovideSweeper) closestPeersToPrefix(prefix bitstr.Key) ([]peer.ID, error) {
	allClosestPeers := make([]peer.ID, 0, 2*s.replicationFactor)

	maxPrefixSearches := 64
	nextPrefix := prefix
	coveredPrefixesStack := []bitstr.Key{}

	// Go down the trie to fully cover prefix.
	for i := range maxPrefixSearches {
		fullKey := s.firstFullKeyWithPrefix(nextPrefix)
		closestPeers, err := s.closestPeersToKey(fullKey)
		if err != nil {
			// We only get an err if something really bad happened, e.g no peers in
			// routing table, invalid key, etc.
			return allClosestPeers, err
		}
		coveredPrefix, coveredPeers := shortestCoveredPrefix(fullKey, closestPeers)
		allClosestPeers = append(allClosestPeers, coveredPeers...)

		coveredPrefixLen := len(coveredPrefix)
		if i == 0 {
			if coveredPrefixLen <= len(prefix) && coveredPrefix == prefix[:coveredPrefixLen] && len(allClosestPeers) > s.replicationFactor {
				// Exit early if the prefix is fully covered at the first request and
				// we have enough peers.
				return allClosestPeers, nil
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

				if len(coveredPrefixesStack) == 0 {
					if len(allClosestPeers) > s.replicationFactor {
						return allClosestPeers, nil
					}
					// Not enough peers -> add coveredPrefix to stack and continue.
					break
				}
				latestPrefix = coveredPrefixesStack[len(coveredPrefixesStack)-1]
			}
		}
		// Push coveredPrefix to stack
		coveredPrefixesStack = append(coveredPrefixesStack, coveredPrefix)
		// Flip last bit of last covered prefix
		nextPrefix = flipLastBit(coveredPrefixesStack[len(coveredPrefixesStack)-1])
	}
	return allClosestPeers, ErrTooManyIterationsDuringExploration
}

// flipLastBit returns to closest 256-bit key to s.order, starting with the
// given k as a prefix.
func (s *reprovideSweeper) firstFullKeyWithPrefix(k bitstr.Key) bitstr.Key {
	kLen := k.BitLen()
	if kLen > 256 {
		panic("bitstr.Key: key length exceeds 256 bits")
	}
	return k + bitstr.Key(key.BitString(s.order))[kLen:]
}

// closestPeersToKey returns a valid peer ID sharing a long common prefix with
// the provided key. Note that the returned peer IDs aren't random, they are
// taken from a static list of preimages.
func (s *reprovideSweeper) closestPeersToKey(k bitstr.Key) ([]peer.ID, error) {
	p, _ := kbucket.GenRandPeerIDWithCPL(keyToBytes(k), kbucket.PeerIDPreimageMaxCpl)
	return s.router.GetClosestPeers(s.ctx, string(p))
}

const minimalRegionReachablePeersRatio float32 = 0.2

// regionReprovide manages reprovides for all cids matching the region's prefix
// to appropriate peers in this keyspace region. Upon failure to reprovide a
// CID, or to connect to a peer, it will NOT retry.
//
// Returns an error if we were unable to reprovide cids to a given threshold of
// peers. In this case, the region reprovide is considered failed and the
// caller is responsible for trying again. This allows detecting if we are
// offline.
func (s *reprovideSweeper) regionReprovide(r region) error {
	cidsAllocations := s.cidsAllocationsToPeers(r)
	selfAddrs := s.getSelfAddrs()
	pmes := pb.NewMessage(pb.Message_ADD_PROVIDER, []byte{}, 0)
	pmes.ProviderPeers = pb.RawPeerInfosToPBPeers([]peer.AddrInfo{selfAddrs})

	errCount := 0
	for p, cids := range cidsAllocations {
		// TODO: allow some reasonable parallelism
		err := s.provideCidsToPeer(p, cids, pmes)
		if err != nil {
			logger.Warn(err)
			errCount++
		}
	}
	if errCount > int(float32(len(cidsAllocations))*minimalRegionReachablePeersRatio) {
		return errors.New("unable to reprovide to enough peers in region" + string(r.prefix))
	}
	return nil
}

// cidsAllocationsToPeers assigns each cid of the provided region to the
// replicationFactor closest peers from the region (XOR distance).
func (s *reprovideSweeper) cidsAllocationsToPeers(r region) map[peer.ID][]multihash.Multihash {
	// TODO: this is a very greedy approach, can be greatly optimized
	keysPerPeer := make(map[peer.ID][]multihash.Multihash)
	for _, cidEntry := range allEntries(r.cids, s.order) {
		for _, peerEntry := range trie.Closest(r.peers, cidEntry.Key, s.replicationFactor) {
			pid := peerEntry.Data
			if _, ok := keysPerPeer[pid]; !ok {
				keysPerPeer[pid] = []multihash.Multihash{cidEntry.Data}
			} else {
				keysPerPeer[pid] = append(keysPerPeer[pid], cidEntry.Data)
			}
		}
	}
	return keysPerPeer
}

const (
	reprovideFailureTolerence       float32 = 0.5
	reprovideInitialFailuresAllowed int     = 3
)

// provideCidsToPeer performs the network operation to advertise to the given
// DHT server (p) that we serve all the given cids.
func (s *reprovideSweeper) provideCidsToPeer(p peer.ID, cids []multihash.Multihash, pmes *pb.Message) error {
	errCount := 0
	for i, mh := range cids {
		pmes.Key = mh
		err := s.msgSender.SendMessage(s.ctx, p, pmes)
		if err != nil {
			logger.Infow("reproviding", "cid", mh, "to", p, "error", err)
			errCount++

			if i > reprovideInitialFailuresAllowed && errCount > int(float32(i)*reprovideFailureTolerence) {
				return errors.New("failed to reprovide to " + string(p.String()))
			}
		}
	}
	return nil
}

// addCidsToLocalProviderStore adds the cids to the local DHT node provider
// store.
func (s *reprovideSweeper) addCidsToLocalProviderStore(cids *trie.Trie[bit256.Key, multihash.Multihash]) {
	for _, entry := range allEntries(cids, s.order) {
		s.router.Provide(s.ctx, cid.NewCidV0(entry.Data), false)
	}
}

// scheduleNextReprovide schedules the next reprovide for the given prefix, at
// the earliest between the time offset associated with this prefix, and
// reprovideInterval+maxReprovideDelay after the last reprovide, allowing a
// maximum delay in the reprovide of a region to be at most maxReprovideDelay
// when needed.
//
// If s.reprovideInterval(prefix) is more than maxReprovideDelay after the last
// reprovide+reprovideInterval, then the next reprovide time is shifted by
// maxReprovideDelay in the periodic schedule.
//
// If the given prefix is already on the schedule, this function is a no op.
func (s *reprovideSweeper) scheduleNextReprovide(prefix bitstr.Key, lastReprovide time.Duration, regularReprovide bool) {
	if regularReprovide {
		nextReprovideTime := min(s.reprovideTimeForPrefix(prefix), lastReprovide+s.reprovideInterval+s.maxReprovideDelay)
		s.scheduleLk.Lock()
		s.schedule.Add(prefix, nextReprovideTime)
		s.scheduleLk.Unlock()
		return
	}
	s.addPrefixToSchedule(prefix)
}

const maxPrefixSize = 30

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
func (s *reprovideSweeper) reprovideTimeForPrefix(prefix bitstr.Key) time.Duration {
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

// Provide returns an error if the cid failed to be provided to the network.
// However, it will keep reproviding the cid regardless of whether the first
// provide succeeded.
func (s *reprovideSweeper) Provide(ctx context.Context, c cid.Cid, broadcast bool) error {
	if !broadcast {
		return s.router.Provide(s.ctx, c, false)
	}
	req := provideReq{
		ctx:  ctx,
		cids: []multihash.Multihash{c.Hash()},
		done: make(chan error),
	}
	s.provideChan <- req
	// Wait for initial provide to complete before returning.
	return <-req.done
}

// ProvideMany provides multiple cids to the network. It will return an error
// if none of the cids could be provided, however it will keep reproviding
// these cids regardless of the initial provide success.
func (s *reprovideSweeper) ProvideMany(ctx context.Context, keys []multihash.Multihash) error {
	req := provideReq{
		ctx:  ctx,
		cids: keys,
		done: make(chan error),
	}
	s.provideChan <- req
	// Wait for all cids to be provided before returning.
	return <-req.done
}
