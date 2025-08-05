package provider

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/filecoin-project/go-clock"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipfs/go-test/random"
	kb "github.com/libp2p/go-libp2p-kbucket"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	mh "github.com/multiformats/go-multihash"

	"github.com/probe-lab/go-libdht/kad/key"
	"github.com/probe-lab/go-libdht/kad/key/bit256"
	"github.com/probe-lab/go-libdht/kad/key/bitstr"
	"github.com/probe-lab/go-libdht/kad/trie"

	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
	"github.com/libp2p/go-libp2p-kad-dht/provider/internal/keyspace"
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

var logger = logging.Logger("dht/SweepingProvider")

type KadClosestPeersRouter interface {
	GetClosestPeers(context.Context, string) ([]peer.ID, error)
}

type SweepingProvider struct {
	// TODO: complete me
	done      chan struct{}
	closeOnce sync.Once

	peerid peer.ID
	order  bit256.Key
	router KadClosestPeersRouter

	clock clock.Clock

	maxProvideConnsPerWorker int

	schedule *trie.Trie[bitstr.Key, time.Duration]

	avgPrefixLenLk       sync.Mutex
	avgPrefixLenReady    chan struct{}
	cachedAvgPrefixLen   int
	lastAvgPrefixLen     time.Time
	avgPrefixLenValidity time.Duration

	msgSender      pb.MessageSender
	getSelfAddrs   func() []ma.Multiaddr
	addLocalRecord func(mh.Multihash) error
}

// FIXME: remove me
func (s *SweepingProvider) SatisfyLinter() {
	s.vanillaProvide([]byte{})
	s.closestPeersToKey("")
	s.measureInitialPrefixLen()
	s.getAvgPrefixLenNoLock()
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
	s.avgPrefixLenReady <- struct{}{}
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

// ProvideOnce sends provider records for the specified keys to the DHT swarm
// only once. It does not automatically reprovide those keys afterward.
//
// Add the supplied multihashes to the provide queue, and return immediately.
// The provide operation happens asynchronously.
func (s *SweepingProvider) ProvideOnce(keys ...mh.Multihash) {
	// TODO: implement me
}

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
func (s *SweepingProvider) StartProviding(force bool, keys ...mh.Multihash) {
	// TODO: implement me
}

// StopProviding stops reproviding the given keys to the DHT swarm. The node
// stops being referred as a provider when the provider records in the DHT
// swarm expire.
//
// Remove the `keys` from the schedule and return immediately. Valid records
// can remain in the DHT swarm up to the provider record TTL after calling
// `StopProviding`.
func (s *SweepingProvider) StopProviding(keys ...mh.Multihash) {
	// TODO: implement me
}
