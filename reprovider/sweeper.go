package reprovider

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

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
	_ Provider    = &reprovideSweeper{}
	_ ProvideMany = &reprovideSweeper{}
)

type KadRouter interface {
	GetClosestPeers(context.Context, string) ([]peer.ID, error)
	Provide(context.Context, cid.Cid, bool) error
}

// TODO: add some more logging
var logger = logging.Logger("dht/ReprovideSweep")

var errTooManyIterationsDuringExploration = errors.New("closestPeersToPrefix needed more than maxPrefixSearches iterations")

const keyLen = bit256.KeyLen * 8 // 256

// TODO: support resuming reprovide service after a restart
// persist cids to disk
// persist prefixes waiting to be reprovided and last reprovided prefix (with time) on disk

type provideReq struct {
	ctx  context.Context
	cids []mh.Multihash
	done chan error
}

type reprovideSweeper struct {
	ctx    context.Context
	peerid peer.ID
	router KadRouter
	order  bit256.Key

	online                  atomic.Bool
	burstProvideParallelism int

	replicationFactor int
	now               func() time.Time
	cycleStart        time.Time
	reprovideInterval time.Duration
	maxReprovideDelay time.Duration

	cids *trie.Trie[bit256.Key, mh.Multihash]

	provideChan   chan provideReq
	schedule      *trie.Trie[bitstr.Key, time.Duration]
	scheduleLk    sync.Mutex
	scheduleTimer *time.Timer

	failedRegionsChan  chan bitstr.Key
	lateRegionsQueue   []bitstr.Key
	retryTicker        *time.Ticker
	pendingCids        []mh.Multihash
	pendingCidsChan    chan []mh.Multihash
	catchupPendingChan chan struct{}

	prefixCursor        bitstr.Key
	ongoingReprovides   *trie.Trie[bitstr.Key, struct{}]
	ongoingReprovidesLk sync.Mutex

	msgSender               pb.MessageSender
	getSelfAddrs            func() []ma.Multiaddr
	localNearestPeersToSelf func(int) []peer.ID
}

func NewReprovider(ctx context.Context, opts ...Option) (Provider, error) {
	cfg := DefaultConfig
	err := cfg.apply(opts...)
	if err != nil {
		return nil, err
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	reprovider := &reprovideSweeper{
		ctx:    ctx,
		peerid: cfg.peerid,
		router: cfg.router,
		order:  peerIDToBit256(cfg.peerid),

		replicationFactor: cfg.replicationFactor,
		reprovideInterval: cfg.reprovideInterval,
		maxReprovideDelay: cfg.maxReprovideDelay,

		burstProvideParallelism: cfg.provideWorkers,

		now:        cfg.now,
		cycleStart: cfg.now(),

		msgSender:               cfg.msgSender,
		getSelfAddrs:            cfg.selfAddrs,
		localNearestPeersToSelf: cfg.localNearestPeersToSelf,

		cids: trie.New[bit256.Key, mh.Multihash](),

		provideChan:   make(chan provideReq),
		schedule:      trie.New[bitstr.Key, time.Duration](),
		scheduleTimer: time.NewTimer(time.Hour),

		failedRegionsChan:  make(chan bitstr.Key),
		retryTicker:        time.NewTicker(5 * time.Minute),
		pendingCidsChan:    make(chan []mh.Multihash),
		catchupPendingChan: make(chan struct{}, 1),

		ongoingReprovides: trie.New[bitstr.Key, struct{}](),
	}
	// Don't need to start schedule timer yet
	reprovider.scheduleTimer.Stop()
	// Start in online state
	reprovider.online.Store(true)

	go reprovider.run()

	return reprovider, nil
}

func NewDHTReprovider(ctx context.Context, dht *dht.IpfsDHT, opts ...Option) (Provider, error) {
	localNearestPeersToSelf := func(count int) []peer.ID {
		return dht.RoutingTable().NearestPeers(dht.PeerKey(), count)
	}
	opts = append([]Option{
		WithPeerID(dht.Host().ID()),
		WithRouter(dht),
		WithSelfAddrs(dht.FilteredAddrs),
		WithLocalNearestPeersToSelf(localNearestPeersToSelf),
		WithMessageSender(net.NewMessageSenderImpl(dht.Host(), dht.Protocols())),
	}, opts...,
	)
	return NewReprovider(ctx, opts...)
}

func (s *reprovideSweeper) run() {
	defer s.retryTicker.Stop()
	defer s.scheduleTimer.Stop()

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
			s.lateRegionsQueue = append(s.lateRegionsQueue, prefix)
			if s.online.Load() {
				go s.onlineCheck()
			}
		case cids := <-s.pendingCidsChan:
			s.pendingCids = append(s.pendingCids, cids...)
			if s.online.Load() {
				go s.onlineCheck()
			}
		case <-s.retryTicker.C:
			if !s.online.Load() {
				go s.onlineCheck()
			} else {
				s.catchupPendingWork()
			}
		case <-s.catchupPendingChan:
			s.catchupPendingWork()
		}
	}
}

func (s *reprovideSweeper) handleProvide(provideRequest provideReq) {
	if len(provideRequest.cids) == 0 {
		provideRequest.done <- nil
		return
	}
	newCids := make([]mh.Multihash, 0, len(provideRequest.cids))
	for _, c := range provideRequest.cids {
		k := mhToBit256(c)
		// Add cid to s.cids if not there already, and add fresh cids to newCids.
		// Deduplication is handled here.
		if added := s.cids.Add(k, c); added {
			newCids = append(newCids, c)
		}
	}

	if len(newCids) == 0 {
		// All cids are already tracked and have been provided.
		provideRequest.done <- nil
		return
	}

	if !s.online.Load() {
		s.pendingCidsChan <- provideRequest.cids
		provideRequest.done <- errors.New("node is offline")
		return
	}

	go func() {
		err := s.networkProvide(newCids)
		provideRequest.done <- err
	}()
}

func (s *reprovideSweeper) networkProvide(cids []mh.Multihash) error {
	if len(cids) == 0 {
		// No cids to provide.
		return nil
	}
	prefixes := make(map[bitstr.Key]*trie.Trie[bit256.Key, mh.Multihash])
	avgPrefixLen := s.getAvgPrefixLen()
	for _, c := range cids {
		k := mhToBit256(c)
		// Add cid to s.cids if not there already, and add fresh cids to new cids
		// trie. Deduplication is handled here.
		// Spread cids by prefixes of avgPrefixLen
		prefix := bitstr.Key(key.BitString(k)[:avgPrefixLen])
		if _, ok := prefixes[prefix]; !ok {
			prefixes[prefix] = trie.New[bit256.Key, mh.Multihash]()
		}
		prefixes[prefix].Add(k, c)
	}

	if len(cids) == 1 {
		// Provide a single cid (NOT ProvideMany)
		for prefix, cid := range prefixes {
			return s.provideForPrefix(prefix, cid, initialProvide)
		}
	}

	sortedPrefixes := sortPrefixesBySize(prefixes)

	nWorkers := min(s.burstProvideParallelism, len(sortedPrefixes))
	if nWorkers == 0 {
		// 0 means unlimited parallelism
		nWorkers = len(sortedPrefixes)
	}

	// Atomic flag: 0 = no cids successfully provided yet, 1 = at least one
	// success
	var anySuccess uint32
	jobChan := make(chan prefixAndCids, 1)
	defer close(jobChan)
	wg := sync.WaitGroup{}

	// Start workers to handle the provides.
	for range nWorkers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for p := range jobChan {
				if err := s.provideForPrefix(p.prefix, p.cids, initialProvide); err == nil {
					// Track if we were able to provide at least one cid.
					atomic.StoreUint32(&anySuccess, 1)
				}
			}
		}()
	}

	// Feed jobs to the workers.
	onlineTicker := time.NewTicker(5 * time.Second)
	defer onlineTicker.Stop()

	earlyExit := -1
	for i, p := range sortedPrefixes {
		select {
		case jobChan <- p:
		case <-onlineTicker.C:
			if !s.online.Load() {
				earlyExit = i
			}
		case <-s.ctx.Done():
			// TODO: save pending cids to disk to exit gracefully
			earlyExit = i
		}
		if earlyExit != -1 {
			break
		}
	}

	if earlyExit != -1 {
		// Add cids that haven't been provided yet to the pending queue.
		cids := make([]mh.Multihash, 0)
		for _, prefix := range sortedPrefixes[earlyExit:] {
			cids = append(cids, allValues(prefix.cids, s.order)...)
		}
		s.pendingCidsChan <- cids
	} else {
		// Wait for workers to finish before reporting completion.
		wg.Wait()
	}

	if atomic.LoadUint32(&anySuccess) == 0 {
		return errors.New("failed to provide any cid")
	}
	return nil
}

func (s *reprovideSweeper) handleReprovide() {
	online := s.online.Load()
	s.scheduleLk.Lock()
	if online {
		// Remove prefix from trie if online, new schedule will be added as needed
		// after reprovide.
		s.schedule.Remove(s.prefixCursor)
	}
	// Get next prefix to reprovide, and set timer for it.
	next := nextNonEmptyLeaf(s.schedule, s.prefixCursor, s.order)
	currentPrefix := s.prefixCursor

	s.prefixCursor = next.Key
	nextReprovideDelay := next.Data - s.currentTimeOffset()
	s.scheduleTimer.Reset(nextReprovideDelay)
	s.scheduleLk.Unlock()

	// If we are offline, don't even try to reprovide region.
	if !online {
		s.failedRegionsChan <- currentPrefix
		return
	}

	cids := s.cids.Copy() // NOTE: if many cids, this may have a large memory footprint
	go s.provideForPrefix(currentPrefix, cids, regularReprovide)
}

type provideType uint8

const (
	regularReprovide provideType = iota
	lateRegionReprovide
	initialProvide
)

func (s *reprovideSweeper) handleProvideError(prefix bitstr.Key, cids *trie.Trie[bit256.Key, mh.Multihash], op provideType) {
	switch op {
	case initialProvide:
		s.pendingCidsChan <- allValues(cids, s.order)
	case regularReprovide, lateRegionReprovide:
		s.failedRegionsChan <- prefix
	}
	// We should schedule the next reprovide even if there was an error during
	// the reprovide. Otherwise the schedule for this region will deviate.
	s.scheduleNextReprovide(prefix, s.currentTimeOffset(), op)
}

func (s *reprovideSweeper) provideForPrefix(prefix bitstr.Key, cids *trie.Trie[bit256.Key, mh.Multihash], op provideType) error {
	subtrie, ok := subtrieMatchingPrefix(cids, prefix)
	if !ok {
		// There was no cid matching prefix to provide
		return nil
	}
	if subtrie.Size() <= 2 {
		// Region has 1 or 2 cids, it is more optimized to provide them naively.
		cidsSlice := allValues(subtrie, s.order)
		return s.individualProvideForPrefix(s.ctx, prefix, cidsSlice, op)
	}
	peers, explorationErr := s.closestPeersToPrefix(prefix)
	if explorationErr != nil {
		if explorationErr != errTooManyIterationsDuringExploration {
			s.handleProvideError(prefix, subtrie, op)
			return explorationErr
		}
		// NOTE: there is probably a better way to handle this error
		logger.Errorf("prefix key exploration not complete: %s", prefix)
	}
	if len(peers) == 0 {
		err := errors.New("no peers found when exploring prefix " + string(prefix))
		s.handleProvideError(prefix, subtrie, op)
		return err
	}

	regions, coveredPrefix := s.regionsFromPeers(peers, cids)

	if (op == regularReprovide || op == lateRegionReprovide) && explorationErr != errTooManyIterationsDuringExploration {
		// When reproviding a region, remove all scheduled regions starting with
		// the currently covered prefix.
		s.unscheduleSubsumedPrefixes(coveredPrefix)
		// Claim the regions to be reprovided, so that no other thread will try to
		// reprovdie them concurrently.
		regions = s.claimRegionReprovide(regions)
	}

	errCount := 0
	for _, r := range regions {
		s.addCidsToLocalProviderStore(r.cids)
		err := s.regionReprovide(r)
		if err != nil {
			logger.Error(err)
			errCount++
			s.handleProvideError(r.prefix, r.cids, op)
			continue
		}
		s.releaseRegionReprovide(r.prefix)

		s.scheduleNextReprovide(r.prefix, s.currentTimeOffset(), op)
		if op == regularReprovide || op == lateRegionReprovide {
			// TODO: persist to datastore that region identified by prefix was reprovided `now`, only during regular reprovides?
		}
	}
	// If at least 1 regions was reprovided, we don't consider it a failure.
	if errCount == len(regions) {
		return errors.New("failed to reprovide any region")
	}
	return nil
}

func (s *reprovideSweeper) individualProvideForPrefix(ctx context.Context, prefix bitstr.Key, cids []mh.Multihash, op provideType) error {
	if len(cids) == 0 {
		return nil
	}

	fullRegionReprovide := op == regularReprovide || op == lateRegionReprovide
	var err error

	if len(cids) == 1 {
		err = s.vanillaProvide(ctx, cids[0])
		if err != nil && !fullRegionReprovide {
			s.pendingCidsChan <- cids
		}
	} else {
		wg := sync.WaitGroup{}
		success := atomic.Bool{}
		for _, cid := range cids {
			wg.Add(1)
			go func() {
				defer wg.Done()
				err := s.vanillaProvide(ctx, cid)
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
			err = errors.New("all individual provides failed for prefix " + string(prefix))
		}
	}
	if fullRegionReprovide {
		// TODO: persist to datastore that region identified by prefix was reprovided `now`
		if err != nil {
			s.failedRegionsChan <- prefix
		}
	}
	s.scheduleNextReprovide(prefix, s.currentTimeOffset(), op)
	return err
}

// vanillaProvide provides a single cid to the network without any
// optimization. It should be used for providing a small number of cids
// (typically 1 or 2), because exploring the keyspace would add too much
// overhead for a small number of cids.
func (s *reprovideSweeper) vanillaProvide(ctx context.Context, mh mh.Multihash) error {
	return s.router.Provide(ctx, cid.NewCidV0(mh), true)
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
		if len(closestPeers) == 0 {
			return allClosestPeers, errors.New("dht lookup didn't return any peers")
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
	return allClosestPeers, errTooManyIterationsDuringExploration
}

// flipLastBit returns to closest 256-bit key to s.order, starting with the
// given k as a prefix.
func (s *reprovideSweeper) firstFullKeyWithPrefix(k bitstr.Key) bitstr.Key {
	kLen := k.BitLen()
	if kLen > keyLen {
		panic(fmt.Errorf("bitstr.Key: key length exceeds %d bits", keyLen))
	}
	return k + bitstr.Key(key.BitString(s.order))[kLen:]
}

// closestPeersToKey returns a valid peer ID sharing a long common prefix with
// the provided key. Note that the returned peer IDs aren't random, they are
// taken from a static list of preimages.
func (s *reprovideSweeper) closestPeersToKey(k bitstr.Key) ([]peer.ID, error) {
	p, _ := kb.GenRandPeerIDWithCPL(keyToBytes(k), kb.PeerIDPreimageMaxCpl)
	return s.router.GetClosestPeers(s.ctx, string(p))
}

// regionsFromPeers build the keyspace regions from the given peers, and
// assigns cids to the appropriate region. It returns the created regions
// ordered according to s.order and the Common Prefix shared by all peers.
func (s *reprovideSweeper) regionsFromPeers(peers []peer.ID, cids *trie.Trie[bit256.Key, mh.Multihash]) ([]region, bitstr.Key) {
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
	regions := extractMinimalRegions(peersTrie, "", s.replicationFactor, s.order)
	for i, r := range regions {
		regions[i].cids, _ = subtrieMatchingPrefix(cids, r.prefix)
	}
	commonPrefix := bitstr.Key(key.BitString(firstPeerKey)[:minCpl])
	return regions, commonPrefix
}

func (s *reprovideSweeper) unscheduleSubsumedPrefixes(prefix bitstr.Key) {
	s.scheduleLk.Lock()
	// Pop prefixes scheduled in the future being covered by the explored peers.
	if subtrie, ok := subtrieMatchingPrefix(s.schedule, prefix); ok {
		for _, entry := range allEntries(subtrie, s.order) {
			s.schedule.Remove(entry.Key)
		}
	}
	s.scheduleLk.Unlock()
}

// claimRegionReprovide checks if the region is already being reprovided by
// another thread. If not it marks the region as being currently reprovided.
func (s *reprovideSweeper) claimRegionReprovide(regions []region) []region {
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
func (s *reprovideSweeper) releaseRegionReprovide(prefix bitstr.Key) {
	s.ongoingReprovidesLk.Lock()
	defer s.ongoingReprovidesLk.Unlock()
	s.ongoingReprovides.Remove(prefix)
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
	addrInfo := peer.AddrInfo{ID: s.peerid, Addrs: s.getSelfAddrs()}
	pmes := pb.NewMessage(pb.Message_ADD_PROVIDER, []byte{}, 0)
	pmes.ProviderPeers = pb.RawPeerInfosToPBPeers([]peer.AddrInfo{addrInfo})

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
func (s *reprovideSweeper) cidsAllocationsToPeers(r region) map[peer.ID][]mh.Multihash {
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

const (
	reprovideFailureTolerence       float32 = 0.5
	reprovideInitialFailuresAllowed int     = 3
)

// provideCidsToPeer performs the network operation to advertise to the given
// DHT server (p) that we serve all the given cids.
func (s *reprovideSweeper) provideCidsToPeer(p peer.ID, cids []mh.Multihash, pmes *pb.Message) error {
	errCount := 0
	for i, mh := range cids {
		pmes.Key = mh
		err := s.msgSender.SendMessage(s.ctx, p, pmes)
		if err != nil {
			logger.Infow("providing", "cid", mh, "to", p, "error", err)
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
func (s *reprovideSweeper) addCidsToLocalProviderStore(cids *trie.Trie[bit256.Key, mh.Multihash]) {
	for _, entry := range allEntries(cids, s.order) {
		s.router.Provide(s.ctx, cid.NewCidV0(entry.Data), false)
	}
}

// scheduleNextReprovide schedules the next reprovide for the given prefix,
// according to the kind of provide that just happened.
//
// If the given prefix is already on the schedule, this function is a no op.
func (s *reprovideSweeper) scheduleNextReprovide(prefix bitstr.Key, lastReprovide time.Duration, op provideType) {
	switch op {
	case initialProvide:
		// Prefix may never have been scheduled before, so we may need to add it to
		// the schedule at the corresponding time.
		s.scheduleLk.Lock()
		if ok, _ := trieHasPrefixOfKey(s.schedule, prefix); !ok {
			// Prefix not covered by schedule yet, add it.
			reprovideTime := s.reprovideTimeForPrefix(prefix)
			s.schedule.Add(prefix, reprovideTime)

			followingKey := nextNonEmptyLeaf(s.schedule, prefix, s.order).Key
			if s.prefixCursor == "" || s.prefixCursor == followingKey {
				// Next prefix to be reprovided is the prefix we just added to the
				// schedule. Advance the alarm.
				s.prefixCursor = prefix
				s.scheduleTimer.Reset(reprovideTime)
			}
		}
		s.scheduleLk.Unlock()
	case regularReprovide:
		// Schedule next reprovide given that the prefix was just reprovided on
		// schedule. In the case the next reprovide time should be delayed due to a
		// growth in the number of network peers matching the prefix, don't delay
		// more than s.maxReprovideDelay.
		nextReprovideTime := min(s.reprovideTimeForPrefix(prefix), lastReprovide+s.reprovideInterval+s.maxReprovideDelay)
		s.scheduleLk.Lock()
		s.schedule.Add(prefix, nextReprovideTime)
		s.scheduleLk.Unlock()
	case lateRegionReprovide:
		// Do nothing, next reprovide is already scheduled.
	}
}

// currentTimeOffset returns the current time offset in the reprovide cycle.
func (s *reprovideSweeper) currentTimeOffset() time.Duration {
	return s.now().Sub(s.cycleStart) % s.reprovideInterval
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

// onlineCheck checks if the node is online by making a GetClosestPeers network
// request. It updates s.online accordingly.
//
// If the state changed from offline to online, signal that we can catch up on
// pending work.
func (s *reprovideSweeper) onlineCheck() {
	peers, err := s.router.GetClosestPeers(s.ctx, string(s.peerid))
	isOnline := err == nil && len(peers) > 0
	wasOnline := s.online.Load()
	if wasOnline != isOnline {
		// We just changed connectivity state.
		s.online.Store(isOnline)
		if isOnline {
			// Signal that we are able to catch up on pending work.
			s.catchupPendingChan <- struct{}{}
		}
	}
}

func (s *reprovideSweeper) catchupPendingWork() {
	pendingCids := s.pendingCids
	lateRegions := s.lateRegionsQueue
	if len(pendingCids) == 0 && len(lateRegions) == 0 {
		// No pending work :)
		return
	}
	// Empty pending lists
	s.pendingCids = s.pendingCids[:0]
	s.lateRegionsQueue = s.lateRegionsQueue[:0]
	// TODO: if catchup in process, limit provideMany workers, use atomicBool
	//

	// 1. Iterate on cids, and remove those matching any prefix in the lateRegions (they will be provided as part of the region)
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
	// 2. Iterate on lateRegions, and reprovide them
	allCids := s.cids.Copy()
	for _, r := range lateRegions {
		// TODO: go routines, limit workers
		go s.provideForPrefix(r, allCids, lateRegionReprovide)
	}
	// 3. ProvideMany the remaining cids (that haven't been provided yet)
	go s.networkProvide(cidsInNoRegions)
}

// localNearestPeersToSelf returns the CommonPrefixLength of all the
// replicationFactor closest peers to self locally stored share.
func (s *reprovideSweeper) localNearestPeersCPL() int {
	closestToSelf := s.localNearestPeersToSelf(s.replicationFactor)
	if len(closestToSelf) == 0 {
		// This means that all cids will be provided under the empty prefix. Not
		// optimized, but we won't make a wild static guess on the network size.
		return 0
	}
	minCpl := keyLen
	for _, p := range closestToSelf {
		minCpl = min(minCpl, s.order.CommonPrefixLength(peerIDToBit256(p)))
	}
	return minCpl
}

func (s *reprovideSweeper) getAvgPrefixLen() int {
	prefixLenSum := 0
	s.scheduleLk.Lock()
	defer s.scheduleLk.Unlock()
	scheduleSize := s.schedule.Size()
	if scheduleSize == 0 {
		// No reprovide has been scheduled yet. Try to get a good prefix length
		// estimate from the routing table.
		return s.localNearestPeersCPL()
	}
	for _, entry := range allEntries(s.schedule, s.order) {
		prefixLenSum += len(entry.Key)
	}
	return prefixLenSum / scheduleSize
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
		cids: []mh.Multihash{c.Hash()},
		done: make(chan error),
	}
	s.provideChan <- req
	// Wait for initial provide to complete before returning.
	return <-req.done
}

// ProvideMany provides multiple cids to the network. It will return an error
// if none of the cids could be provided, however it will keep reproviding
// these cids regardless of the initial provide success.
func (s *reprovideSweeper) ProvideMany(ctx context.Context, keys []mh.Multihash) error {
	req := provideReq{
		ctx:  ctx,
		cids: keys,
		done: make(chan error),
	}
	s.provideChan <- req
	// Wait for all cids to be provided before returning.
	return <-req.done
}
