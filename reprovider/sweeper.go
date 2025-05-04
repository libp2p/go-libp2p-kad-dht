package reprovider

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/ipfs/go-cid"
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

var ErrTooManyIterationsDuringExploration = errors.New("closestPeersToPrefix needed more than maxPrefixSearches iterations")

// TODO: support resuming reprovide service after a restart

// TODO: add queue of cids waiting to be reprovided (if node offline, etc.)

type scheduleEntry struct {
	key    bitstr.Key
	offset time.Duration
}

type provideReq struct {
	ctx  context.Context
	cids []multihash.Multihash
	done chan error
}

type reprovideSweeper struct {
	ctx    context.Context
	host   host.Host
	router KadRouter
	order  bit256.Key

	replicationFactor int
	now               func() time.Time
	cycleStart        time.Time
	reprovideInterval time.Duration
	maxReprovideDelay time.Duration

	cids   *trie.Trie[bit256.Key, multihash.Multihash]
	cidsLk *sync.Mutex

	provideChan   chan provideReq
	scheduleChan  chan scheduleEntry
	schedule      *trie.Trie[bitstr.Key, time.Duration]
	scheduleTimer *time.Timer

	prefixCursor bitstr.Key
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
		host:              host,
		router:            router,
		order:             peerIDToBit256(host.ID()),
		now:               now,
		cycleStart:        now(),
		reprovideInterval: reprovideInterval,
		maxReprovideDelay: maxReprovideDelay,
		cids:              trie.New[bit256.Key, multihash.Multihash](),
		cidsLk:            &sync.Mutex{},
		provideChan:       make(chan provideReq),
		scheduleChan:      make(chan scheduleEntry),
		schedule:          trie.New[bitstr.Key, time.Duration](),
		scheduleTimer:     time.NewTimer(time.Hour),
	}
	sweeper.scheduleTimer.Stop()
	go sweeper.run()
	return sweeper
}

func (s *reprovideSweeper) run() {
	for {
		select {
		case <-s.ctx.Done():
			return
		case entry := <-s.scheduleChan:
			s.schedule.Add(entry.key, entry.offset)
		case provideRequest := <-s.provideChan:
			s.handleProvide(provideRequest)
		case <-s.scheduleTimer.C:
			s.handleReprovide()
		}
	}
}

func (s *reprovideSweeper) handleProvide(provideRequest provideReq) {
	if len(provideRequest.cids) == 1 {
		c := provideRequest.cids[0]
		k := mhToBit256(c)
		if added := s.cids.Add(k, c); !added {
			// cid is already being provided
			provideRequest.done <- nil
			return
		}
		go func() {
			err := s.router.Provide(provideRequest.ctx, cid.NewCidV0(c), true)
			provideRequest.done <- err
		}()

		if !trieHasPrefixOfKey(s.schedule, k) {
			// Cid not covered by schedule yet, add it.
			scheduleKey := bitstr.Key(key.BitString(k))
			reprovideTime := s.reprovideTimeForPrefix(scheduleKey)
			s.schedule.Add(scheduleKey, reprovideTime)

			followingKey := nextNonEmptyLeaf(s.schedule, scheduleKey, s.order).Key
			if s.prefixCursor == "" || s.prefixCursor == followingKey {
				// Either first cid is added or current cid is next in line for
				// reproviding.
				s.prefixCursor = scheduleKey
				s.scheduleTimer.Reset(reprovideTime)
			}
		}
	}
	// TODO: handle multiple provides
}

func (s *reprovideSweeper) handleReprovide() {
	// Remove prefix from trie, new schedule will be added as needed.
	s.schedule.Remove(s.prefixCursor)
	// TODO: handle error if I was unable to provide
	go s.reprovideForPrefix(s.prefixCursor)

	next := nextNonEmptyLeaf(s.schedule, s.prefixCursor, s.order)
	s.prefixCursor = next.Key
	nextReprovideDelay := next.Data - s.currentTimeOffset()
	s.scheduleTimer.Reset(nextReprovideDelay)
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
func (s *reprovideSweeper) regionsFromPeers(peers []peer.ID) []region {
	peersTrie := trie.New[bit256.Key, peer.ID]()
	for _, p := range peers {
		k := peerIDToBit256(p)
		peersTrie.Add(k, p)
	}
	regions := extractMinimalRegions(peersTrie, "", s.replicationFactor, s.order)
	s.cidsLk.Lock()
	for i, r := range regions {
		t := s.cids
		// Navigate to the subtrie matching the prefix
		for i := range r.prefix {
			t = t.Branch(int(r.prefix.Bit(i)))
		}
		regions[i].cids = t
	}
	s.cidsLk.Unlock()
	return regions
}

func (s *reprovideSweeper) reprovideForPrefix(prefix bitstr.Key) error {
	peers, err := s.closestPeersToPrefix(prefix)
	if err != nil {
		if err != ErrTooManyIterationsDuringExploration {
			return err
		}
		// TODO: set appropriate logging
		fmt.Println("warn: prefix key exploration not complete")
	}
	if len(peers) == 0 {
		// TODO: do something
	}
	regions := s.regionsFromPeers(peers)
	for _, r := range regions {
		// NOTE: allow parallelism here?
		s.regionReprovide(r)
		s.addCidsToLocalProviderStore(r.cids)
		s.scheduleNextReprovide(r.prefix, s.currentTimeOffset())
		// TODO: persist to datastore that region identified by prefix was reprovided `now`
	}
	return nil
}

// closestPeersToPrefix returns more than s.replicationFactor peers
// corresponding to the branch of the network peers trie matching the provided
// prefix. In the case there aren't enough peers matching the provided prefix,
// it will find and return the closest peers to the prefix, even if they don't
// exactly match it.
// TODO: test this function!
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

func (s *reprovideSweeper) firstFullKeyWithPrefix(k bitstr.Key) bitstr.Key {
	kLen := k.BitLen()
	if kLen > 256 {
		panic("bitstr.Key: key length exceeds 256 bits")
	}
	return k + bitstr.Key(key.BitString(s.order))[kLen:]
}

// TODO: ideally stop depending on go-libp2p-kbucket. we would need to have preimage list in boxo, or elsewhere.
func (s *reprovideSweeper) closestPeersToKey(k bitstr.Key) ([]peer.ID, error) {
	// TODO: export func in go-libp2p-kbucket so that we don't need to build a rt
	rt, _ := kbucket.NewRoutingTable(0, keyToBytes(k), 0, nil, 0, nil)
	// TODO: justify 15 (kubcket.maxCplForRefresh)
	p, _ := rt.GenRandPeerID(min(uint(k.BitLen()), 15))
	return s.router.GetClosestPeers(s.ctx, string(p))
}

func (s *reprovideSweeper) regionReprovide(r region) {
	// assume all peers from region are reachable (we connected to them before)
	// we don't try again on failure, skip all missing keys
	cidsAllocations := s.cidsAllocationsToPeers(r)
	for p, cids := range cidsAllocations {
		// TODO: allow some reasonable parallelism
		s.provideCidsToPeer(p, cids)
	}
}

func (s *reprovideSweeper) cidsAllocationsToPeers(r region) map[peer.ID][]multihash.Multihash {
	// TODO: check if prefix longer than r.prefix was reprovided less than
	// maxReprovideDelay ago, and if yes, don't reprovide these cids
	//
	// TODO: this is a very greedy approach, can be greatly optimized
	keysPerPeer := make(map[peer.ID][]multihash.Multihash)
	for _, cidEntry := range allKeys(r.cids, s.order) {
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

func (s *reprovideSweeper) provideCidsToPeer(p peer.ID, cids []multihash.Multihash) {
	// TODO: handle this with custom msgSender
	// TODO: maybe allow "some" pipelining?
}

func (s *reprovideSweeper) addCidsToLocalProviderStore(cids *trie.Trie[bit256.Key, multihash.Multihash]) {
	for _, entry := range allKeys(cids, s.order) {
		s.router.Provide(s.ctx, cid.NewCidV0(entry.Data), false)
	}
}

// scheduleNextReprovide schedules the next reprovide for the given prefix, at
// the earliest between the time offset associated with this prefix, and
// reprovideInterval+maxReprovideDelay after the last reprovide, allowing a
// maximum delay in the reprovide of a region to be at most maxReprovideDelay
// when needed.
func (s *reprovideSweeper) scheduleNextReprovide(prefix bitstr.Key, lastReprovide time.Duration) {
	nextReprovideTime := min(s.reprovideTimeForPrefix(prefix), lastReprovide+s.reprovideInterval+s.maxReprovideDelay)

	s.scheduleChan <- scheduleEntry{key: prefix, offset: nextReprovideTime}
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
