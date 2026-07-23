package provider

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/guillaumemichel/reservedpool"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/namespace"
	"github.com/ipfs/go-datastore/query"
	dssync "github.com/ipfs/go-datastore/sync"
	pebble "github.com/ipfs/go-ds-pebble"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipfs/go-test/random"
	"github.com/libp2p/go-libp2p/core/peer"
	bhost "github.com/libp2p/go-libp2p/p2p/host/basic"
	swarmt "github.com/libp2p/go-libp2p/p2p/net/swarm/testing"
	ma "github.com/multiformats/go-multiaddr"
	mh "github.com/multiformats/go-multihash"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/ipfs/go-libdht/kad/key"
	"github.com/ipfs/go-libdht/kad/key/bit256"
	"github.com/ipfs/go-libdht/kad/key/bitstr"
	"github.com/ipfs/go-libdht/kad/trie"

	"github.com/libp2p/go-libp2p-kad-dht/amino"
	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
	"github.com/libp2p/go-libp2p-kad-dht/provider/internal/connectivity"
	"github.com/libp2p/go-libp2p-kad-dht/provider/internal/keyspace"
	"github.com/libp2p/go-libp2p-kad-dht/provider/internal/queue"
	"github.com/libp2p/go-libp2p-kad-dht/provider/keystore"
	kb "github.com/libp2p/go-libp2p-kbucket"

	"github.com/stretchr/testify/require"
)

func genMultihashes(n int) []mh.Multihash {
	mhs := make([]mh.Multihash, n)
	var err error
	for i := range n {
		mhs[i], err = mh.Sum([]byte(strconv.Itoa(i)), mh.SHA2_256, -1)
		if err != nil {
			panic(err)
		}
	}
	return mhs
}

// genBalancedMultihashes generates 2^exponent multihashes, with balanced
// prefixes, in a random order.
//
// e.g genBalancedMultihashes(3) will generate 8 multihashes, with each
// kademlia identifier starting with a distinct prefix (000, 001, 010, ...,
// 111) of len 3.
func genBalancedMultihashes(exponent int) []mh.Multihash {
	n := 1 << exponent
	mhs := make([]mh.Multihash, 0, n)
	seen := make(map[bitstr.Key]struct{}, n)
	for i := 0; len(mhs) < n; i++ {
		h, err := mh.Sum([]byte(strconv.Itoa(i)), mh.SHA2_256, -1)
		if err != nil {
			panic(err)
		}
		prefix := bitstr.Key(key.BitString(keyspace.MhToBit256(h))[:exponent])
		if _, ok := seen[prefix]; !ok {
			mhs = append(mhs, h)
			seen[prefix] = struct{}{}
		}
	}
	return mhs
}

func genMultihashesMatchingPrefix(prefix bitstr.Key, n int) []mh.Multihash {
	rnd := random.New()
	mhs := make([]mh.Multihash, 0, n)
	for {
		for _, h := range rnd.Multihashes(max(n-len(mhs), 8)) {
			k := keyspace.MhToBit256(h)
			if keyspace.IsPrefix(prefix, k) {
				mhs = append(mhs, h)
				if len(mhs) == n {
					return mhs
				}
			}
		}
	}
}

// genPeersWithPrefix generates n peer IDs whose kademlia identifiers all share
// the given common prefix, simulating a small clustered DHT.
func genPeersWithPrefix(prefix bitstr.Key, n int) []peer.ID {
	rnd := random.New()
	peers := make([]peer.ID, 0, n)
	for len(peers) < n {
		p := rnd.Peers(1)[0]
		if keyspace.IsPrefix(prefix, keyspace.PeerIDToBit256(p)) {
			peers = append(peers, p)
		}
	}
	return peers
}

func getMockAddrs(t *testing.T) func() []ma.Multiaddr {
	return func() []ma.Multiaddr {
		addr, err := ma.NewMultiaddr("/ip4/127.0.0.1/tcp/4001")
		require.NoError(t, err)
		return []ma.Multiaddr{addr}
	}
}

var _ pb.MessageSender = (*mockMsgSender)(nil)

type mockMsgSender struct {
	sendMessageFunc func(ctx context.Context, p peer.ID, m *pb.Message) error
}

func (ms *mockMsgSender) SendRequest(ctx context.Context, p peer.ID, m *pb.Message) (*pb.Message, error) {
	// Unused
	return nil, nil
}

func (ms *mockMsgSender) SendMessage(ctx context.Context, p peer.ID, m *pb.Message) error {
	if ms.sendMessageFunc == nil {
		return nil
	}
	return ms.sendMessageFunc(ctx, p, m)
}

var _ KadClosestPeersRouter = (*mockRouter)(nil)

type mockRouter struct {
	getClosestPeersFunc func(ctx context.Context, k string) ([]peer.ID, error)
}

func (r *mockRouter) GetClosestPeers(ctx context.Context, k string) ([]peer.ID, error) {
	if r.getClosestPeersFunc == nil {
		return nil, nil
	}
	return r.getClosestPeersFunc(ctx, k)
}

func TestProvideKeysToPeer(t *testing.T) {
	msgCount := 0
	msgSender := &mockMsgSender{
		sendMessageFunc: func(ctx context.Context, p peer.ID, m *pb.Message) error {
			msgCount++
			return errors.New("error")
		},
	}
	prov := SweepingProvider{
		ctx:                       context.Background(),
		msgSender:                 msgSender,
		sendProviderRecordTimeout: DefaultSendProviderRecordTimeout,
	}

	nKeys := 16
	pid, err := peer.Decode("12BoooooPEER")
	require.NoError(t, err)
	mhs := [][]mh.Multihash{genMultihashes(nKeys)}
	pmes := &pb.Message{}

	// All ADD_PROVIDER RPCs fail, return an error after reprovideInitialFailuresAllowed+1 attempts
	err = prov.provideKeysToPeer(pid, mhs, pmes)
	require.Error(t, err)
	require.Equal(t, maxConsecutiveProvideFailuresAllowed+1, msgCount)

	// Only fail 33% of requests. The operation should be considered a success.
	msgCount = 0
	msgSender.sendMessageFunc = func(ctx context.Context, p peer.ID, m *pb.Message) error {
		msgCount++
		if msgCount%3 == 0 {
			return errors.New("error")
		}
		return nil
	}
	err = prov.provideKeysToPeer(pid, mhs, pmes)
	require.NoError(t, err)
	require.Equal(t, nKeys, msgCount)
}

// TestProvideKeysToPeerSendMessageTimeout verifies that a peer which accepts
// the libp2p stream but never replies cannot pin the provideKeysToPeer worker
// indefinitely. Each ADD_PROVIDER send is bounded by sendProviderRecordTimeout;
// after maxConsecutiveProvideFailuresAllowed+1 consecutive timeouts the call
// returns the underlying error.
func TestProvideKeysToPeerSendMessageTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var sendCount atomic.Int32
		msgSender := &mockMsgSender{
			sendMessageFunc: func(ctx context.Context, _ peer.ID, _ *pb.Message) error {
				sendCount.Add(1)
				<-ctx.Done()
				return ctx.Err()
			},
		}
		prov := SweepingProvider{
			ctx:                       context.Background(),
			msgSender:                 msgSender,
			sendProviderRecordTimeout: DefaultSendProviderRecordTimeout,
		}

		nKeys := 16
		pid, err := peer.Decode("12BoooooPEER")
		require.NoError(t, err)
		mhs := [][]mh.Multihash{genMultihashes(nKeys)}
		pmes := &pb.Message{}

		done := make(chan error, 1)
		start := time.Now()
		go func() {
			done <- prov.provideKeysToPeer(pid, mhs, pmes)
		}()
		synctest.Wait()

		var providerErr error
		select {
		case providerErr = <-done:
		case <-time.After(time.Hour):
			t.Fatal("provideKeysToPeer did not return within a synthetic hour")
		}
		elapsed := time.Since(start)

		require.Error(t, providerErr)
		require.Contains(t, providerErr.Error(), "failed to provide")

		// We expect exactly maxConsecutiveProvideFailuresAllowed+1 send attempts:
		// the first 1+maxConsecutiveProvideFailuresAllowed each time out, then the
		// next attempt would exceed the streak and the call bails. No additional
		// sends should leak after the bail.
		require.Equal(t, int32(maxConsecutiveProvideFailuresAllowed+1), sendCount.Load(),
			"expected %d send attempts before bailing on streak", maxConsecutiveProvideFailuresAllowed+1)

		// Total elapsed time must be bounded by per-send timeout × attempts and
		// must not approach the unbounded behaviour we are fixing.
		maxElapsed := prov.sendProviderRecordTimeout*time.Duration(maxConsecutiveProvideFailuresAllowed+1) + time.Second
		require.Less(t, elapsed, maxElapsed,
			"provideKeysToPeer should complete in at most per-send timeout × attempts")
	})
}

func TestKeysAllocationsToPeers(t *testing.T) {
	nKeys := 1024
	nPeers := 128
	replicationFactor := 10

	mhs := genMultihashes(nKeys)
	keysTrie := trie.New[bit256.Key, mh.Multihash]()
	keyEntries := make([]trie.Entry[bit256.Key, mh.Multihash], len(mhs))
	for i, c := range mhs {
		keyEntries[i] = trie.Entry[bit256.Key, mh.Multihash]{Key: keyspace.MhToBit256(c), Data: c}
	}
	keysTrie.AddMany(keyEntries...)

	peers := random.Peers(nPeers)
	peersTrie := trie.New[bit256.Key, peer.ID]()
	peerEntries := make([]trie.Entry[bit256.Key, peer.ID], len(peers))
	for i, p := range peers {
		peerEntries[i] = trie.Entry[bit256.Key, peer.ID]{Key: keyspace.PeerIDToBit256(p), Data: p}
	}
	peersTrie.AddMany(peerEntries...)
	keysAllocations := keyspace.AllocateToKClosest(keysTrie, peersTrie, replicationFactor)
	// Flatten the batches into a single slice per peer
	flattened := make(map[peer.ID][]mh.Multihash)
	for dest, batches := range keysAllocations {
		for _, batch := range batches {
			flattened[dest] = append(flattened[dest], batch...)
		}
	}

	for _, c := range mhs {
		k := sha256.Sum256(c)
		closestPeers := kb.SortClosestPeers(peers, k[:])[:replicationFactor]
		for _, p := range closestPeers[:replicationFactor] {
			require.Contains(t, flattened[p], c)
		}
		for _, p := range closestPeers[replicationFactor:] {
			require.NotContains(t, flattened[p], c)
		}
	}
}

func TestReprovideTimeForPrefixWithOrderZero(t *testing.T) {
	s := SweepingProvider{
		reprovideInterval: 16 * time.Second,
		order:             bit256.ZeroKey(),
	}

	require.Equal(t, 0*time.Second, s.reprovideTimeForPrefix("0"))
	require.Equal(t, 8*time.Second, s.reprovideTimeForPrefix("1"))
	require.Equal(t, 0*time.Second, s.reprovideTimeForPrefix("000"))
	require.Equal(t, 8*time.Second, s.reprovideTimeForPrefix("1000"))
	require.Equal(t, 10*time.Second, s.reprovideTimeForPrefix("1010"))
	require.Equal(t, 15*time.Second, s.reprovideTimeForPrefix("1111"))
}

var defaultLogger = logging.Logger(DefaultLoggerName)

func TestReprovideTimeForPrefixWithCustomOrder(t *testing.T) {
	s := SweepingProvider{
		reprovideInterval: 16 * time.Second,
		order:             bit256.NewKey(bytes.Repeat([]byte{0xFF}, 32)), // 111...1
	}

	require.Equal(t, 0*time.Second, s.reprovideTimeForPrefix("1"))
	require.Equal(t, 8*time.Second, s.reprovideTimeForPrefix("0"))
	require.Equal(t, 0*time.Second, s.reprovideTimeForPrefix("111"))
	require.Equal(t, 8*time.Second, s.reprovideTimeForPrefix("0111"))
	require.Equal(t, 10*time.Second, s.reprovideTimeForPrefix("0101"))
	require.Equal(t, 15*time.Second, s.reprovideTimeForPrefix("0000"))
}

func TestClosestPeersToPrefixRandom(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		replicationFactor := 10
		nPeers := 128
		peers := random.Peers(nPeers)
		peersTrie := trie.New[bit256.Key, peer.ID]()
		peerEntries := make([]trie.Entry[bit256.Key, peer.ID], len(peers))
		for i, p := range peers {
			peerEntries[i] = trie.Entry[bit256.Key, peer.ID]{Key: keyspace.PeerIDToBit256(p), Data: p}
		}
		peersTrie.AddMany(peerEntries...)

		router := &mockRouter{
			getClosestPeersFunc: func(ctx context.Context, k string) ([]peer.ID, error) {
				sortedPeers := kb.SortClosestPeers(peers, kb.ConvertKey(k))
				return sortedPeers[:min(replicationFactor, len(peers))], nil
			},
		}

		r := SweepingProvider{
			router:            router,
			replicationFactor: replicationFactor,
			connectivity:      noopConnectivityChecker(),
			logger:            defaultLogger,
		}
		r.connectivity.Start()
		defer r.connectivity.Close()

		synctest.Wait()
		require.True(t, r.connectivity.IsOnline())

		for _, prefix := range []bitstr.Key{"", "0", "1", "00", "01", "10", "11", "000", "001", "010", "011", "100", "101", "110", "111"} {
			closestPeers, _, err := r.closestPeersToPrefix(prefix)
			require.NoError(t, err, "failed for prefix %s", prefix)
			subtrieSize := 0
			currPrefix := prefix
			// Reduce prefix if necessary as closestPeersToPrefix always returns at
			// least replicationFactor peers if possible.
			for {
				subtrie, ok := keyspace.FindSubtrie(peersTrie, currPrefix)
				require.True(t, ok)
				subtrieSize = subtrie.Size()
				if subtrieSize >= replicationFactor {
					break
				}
				currPrefix = currPrefix[:len(currPrefix)-1]
			}
			require.Len(t, closestPeers, subtrieSize, "prefix: %s", prefix)
		}
	})
}

func TestClosestPeersToPrefixSameCPL(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		replFactor := 20
		cpl := 4
		zeroID := [32]byte{}
		peers := make([]peer.ID, 0, replFactor)
		seen := make(map[peer.ID]struct{}, replFactor)
		for len(peers) < replFactor {
			// Generate peers that all share the same CPL with the zero key.
			p, err := kb.GenRandPeerIDWithCPL(zeroID[:], uint(cpl))
			require.NoError(t, err)
			if _, ok := seen[p]; !ok {
				seen[p] = struct{}{}
				peers = append(peers, p)
			}
		}

		router := &mockRouter{
			// GetClosestPeers always return the same set of peers, all sharing the
			// same CPL with the zero key.
			getClosestPeersFunc: func(ctx context.Context, k string) ([]peer.ID, error) {
				return peers, nil
			},
		}

		prov := SweepingProvider{
			router:            router,
			logger:            defaultLogger,
			connectivity:      noopConnectivityChecker(),
			replicationFactor: replFactor,
		}
		prov.connectivity.Start()
		defer prov.connectivity.Close()

		synctest.Wait()
		require.True(t, prov.connectivity.IsOnline())

		// No matter what the requested prefix is, the returned closest peers
		// should be the ones supplied by the GetClosestPeers function.
		for _, prefix := range []bitstr.Key{"1111", "0111", "0011", "0001", "0000", "1", ""} {
			closestPeers, _, err := prov.closestPeersToPrefix(prefix)
			require.NoError(t, err)
			require.ElementsMatch(t, peers, closestPeers)
		}
	})
}

func TestClosestPeersToPrefixSinglePeer(t *testing.T) {
	// Assert that closestPeersToPrefix works as expected when GetClosestPeers
	// only returns a single peer.
	timeout := time.Second
	timer := time.AfterFunc(timeout, func() {
		panic("closestPeersToPrefix timed out")
	})
	synctest.Test(t, func(t *testing.T) {
		defer timer.Stop()
		replFactor := 20
		p := random.Peers(1)[0]

		router := &mockRouter{
			// Simulate a network with only 1 other peer. GetClosestPeers always
			// return this peer.
			getClosestPeersFunc: func(ctx context.Context, k string) ([]peer.ID, error) {
				return []peer.ID{p}, nil
			},
		}

		prov := SweepingProvider{
			router:            router,
			logger:            defaultLogger,
			connectivity:      noopConnectivityChecker(),
			replicationFactor: replFactor,
		}
		prov.connectivity.Start()
		defer prov.connectivity.Close()

		synctest.Wait()
		require.True(t, prov.connectivity.IsOnline())

		// No matter what the requested prefix is, the closest peers to prefix
		// should always be the only peer.
		for _, prefix := range []bitstr.Key{"1111", "0111", "0011", "0001", "0000", "1", ""} {
			closestPeers, _, err := prov.closestPeersToPrefix(prefix)
			require.NoError(t, err)
			require.Len(t, closestPeers, 1)
			require.Contains(t, closestPeers, p)
		}
	})
}

// TestExploreSwarmClusteredPeersCoversAllKeys reproduces #1263: in a small DHT
// where all peers share a common prefix (e.g. "11"), exploreSwarm must still
// produce regions that cover the entire keyspace, since these peers are the
// closest peers to every key. Otherwise keys outside the peers' common prefix
// are silently dropped when assigned to regions, which is what causes
// batchReprovide to drop them and the schedule to collapse to a single prefix.
func TestExploreSwarmClusteredPeersCoversAllKeys(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		replFactor := 20
		// Small DHT: 4 peers, all sharing the common prefix "11".
		peers := genPeersWithPrefix("11", 4)

		router := &mockRouter{
			getClosestPeersFunc: func(ctx context.Context, k string) ([]peer.ID, error) {
				return peers, nil
			},
		}

		prov := SweepingProvider{
			router:            router,
			logger:            defaultLogger,
			connectivity:      noopConnectivityChecker(),
			replicationFactor: replFactor,
			stats:             newOperationStats(time.Hour, time.Minute),
		}
		prov.connectivity.Start()
		defer prov.connectivity.Close()

		synctest.Wait()
		require.True(t, prov.connectivity.IsOnline())

		// Keys spread uniformly across the keyspace: one per 2-bit prefix
		// (00, 01, 10, 11).
		keys := genBalancedMultihashes(2)

		// Reprovide a region that does NOT match the peer cluster.
		regions, _, err := prov.exploreSwarm("10")
		require.NoError(t, err)

		regions = keyspace.AssignKeysToRegions(regions, keys)

		assigned := 0
		for _, r := range regions {
			assigned += r.Keys.Size()
		}
		require.Equalf(t, len(keys), assigned,
			"keys silently dropped: only %d/%d keys were assigned to a region", assigned, len(keys))
	})
}

// TestExploreSwarmClusteredPeersBelowBucketSizeCoversAllKeys is the
// rf < bucketSize variant of #1263. With a custom WithReplicationFactor set
// below the bucket size, closestPeersToPrefix can satisfy the enough-peers
// break (len(allClosestPeers) >= replicationFactor) after exploring only one
// sub-cluster of a clustered swarm. exploreSwarm must keep exploring until the
// zone the gathered peers actually occupy is fully covered, so that both
// sub-clusters are discovered. Otherwise the unexplored sibling's peers are
// missing, its keys are provided to the wrong peers (or dropped), and that
// sibling is collapsed into the broadened prefix and never reprovided.
func TestExploreSwarmClusteredPeersBelowBucketSizeCoversAllKeys(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// rf below the bucket size, clustered swarm: 12 peers all under "11",
		// split 6 under "110" and 6 under "111". None under the requested "10".
		replFactor := 4
		under110 := genPeersWithPrefix("110", 6)
		under111 := genPeersWithPrefix("111", 6)
		peers := append(append([]peer.ID{}, under110...), under111...)

		router := &mockRouter{
			getClosestPeersFunc: func(ctx context.Context, k string) ([]peer.ID, error) {
				return peers, nil
			},
		}

		prov := SweepingProvider{
			router:            router,
			logger:            defaultLogger,
			connectivity:      noopConnectivityChecker(),
			replicationFactor: replFactor,
			stats:             newOperationStats(time.Hour, time.Minute),
		}
		prov.connectivity.Start()
		defer prov.connectivity.Close()

		synctest.Wait()
		require.True(t, prov.connectivity.IsOnline())

		keys := genBalancedMultihashes(2)

		// Reprovide a region that does NOT match the peer cluster.
		regions, coveredPrefix, err := prov.exploreSwarm("10")
		require.NoError(t, err)

		// The covered prefix must enclose every gathered peer (all peers sit
		// under "11", so the broadened zone is "1").
		require.Equalf(t, bitstr.Key("1"), coveredPrefix,
			"covered prefix %q does not match the peers' common ancestor", coveredPrefix)

		// Both sub-clusters must be fully discovered: missing peers mean keys
		// would be provided to the wrong peers.
		foundPeers := make(map[peer.ID]struct{})
		for _, r := range regions {
			for _, p := range keyspace.AllValues(r.Peers, bit256.ZeroKey()) {
				foundPeers[p] = struct{}{}
			}
		}
		require.Lenf(t, foundPeers, len(peers),
			"exploration found %d/%d peers; a sub-cluster was left unexplored", len(foundPeers), len(peers))
		for _, p := range peers {
			require.Containsf(t, foundPeers, p, "peer %s under %s was not discovered", p,
				key.BitString(keyspace.PeerIDToBit256(p))[:3])
		}

		regions = keyspace.AssignKeysToRegions(regions, keys)

		assigned := 0
		for _, r := range regions {
			assigned += r.Keys.Size()
		}
		require.Equalf(t, len(keys), assigned,
			"keys silently dropped: only %d/%d keys were assigned to a region", assigned, len(keys))
	})
}

// TestExploreSwarmPrefixWithEnoughPeersNotOverBroadened guards against
// over-broadening: when the requested prefix already has >= replicationFactor
// peers under it, exploreSwarm must NOT widen the covered prefix just because a
// GetClosestPeers response also returned farther sibling peers (which happens
// whenever the prefix's subtree is smaller than the bucket size). Over-broadening
// would make a reprovide of "111" reprovide the whole "1" subtree and collapse
// the schedule.
func TestExploreSwarmPrefixWithEnoughPeersNotOverBroadened(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		replFactor := 4
		// "111" has 5 peers (>= rf). Siblings exist so GetClosestPeers responses
		// for a "111" query also include peers outside "111".
		under111 := genPeersWithPrefix("111", 5)
		peers := slices.Concat(
			under111,
			genPeersWithPrefix("110", 3),
			genPeersWithPrefix("10", 4),
			genPeersWithPrefix("0", 8),
		)

		router := &mockRouter{
			getClosestPeersFunc: func(ctx context.Context, k string) ([]peer.ID, error) {
				return peers, nil
			},
		}

		prov := SweepingProvider{
			router:            router,
			logger:            defaultLogger,
			connectivity:      noopConnectivityChecker(),
			replicationFactor: replFactor,
			stats:             newOperationStats(time.Hour, time.Minute),
		}
		prov.connectivity.Start()
		defer prov.connectivity.Close()

		synctest.Wait()
		require.True(t, prov.connectivity.IsOnline())

		regions, coveredPrefix, err := prov.exploreSwarm("111")
		require.NoError(t, err)

		require.Equalf(t, bitstr.Key("111"), coveredPrefix,
			"requested '111' (5 >= rf peers) but covered prefix widened to %q", coveredPrefix)

		foundPeers := make(map[peer.ID]struct{})
		for _, r := range regions {
			for _, p := range keyspace.AllValues(r.Peers, bit256.ZeroKey()) {
				foundPeers[p] = struct{}{}
			}
		}
		require.Lenf(t, foundPeers, len(under111),
			"region holds %d peers, expected the %d peers under '111'", len(foundPeers), len(under111))
		for _, p := range under111 {
			require.Containsf(t, foundPeers, p, "peer under '111' missing from region")
		}
	})
}

// peersToTrie builds a kademlia-id trie from a list of peers.
func peersToTrie(peers []peer.ID) *trie.Trie[bit256.Key, peer.ID] {
	t := trie.New[bit256.Key, peer.ID]()
	for _, p := range peers {
		t.Add(keyspace.PeerIDToBit256(p), p)
	}
	return t
}

// peersUnderTrie returns all peers in `peersTrie` whose kademlia id is under
// `prefix`.
func peersUnderTrie(peersTrie *trie.Trie[bit256.Key, peer.ID], prefix bitstr.Key) []peer.ID {
	sub, ok := keyspace.FindSubtrie(peersTrie, prefix)
	if !ok {
		return nil
	}
	return keyspace.AllValues(sub, bit256.ZeroKey())
}

// referenceCoveredPrefix independently computes the prefix exploreSwarm should
// cover for a request: the longest ancestor-or-equal of `prefix` whose subtrie
// holds at least `rf` peers, or "" if even the whole network has fewer.
func referenceCoveredPrefix(peersTrie *trie.Trie[bit256.Key, peer.ID], prefix bitstr.Key, rf int) bitstr.Key {
	cp := prefix
	for {
		size := 0
		if sub, ok := keyspace.FindSubtrie(peersTrie, cp); ok {
			size = sub.Size()
		}
		if size >= rf || len(cp) == 0 {
			return cp
		}
		cp = cp[:len(cp)-1]
	}
}

// realisticRouter mimics a real DHT GetClosestPeers: it returns the bucketSize
// peers closest to the queried key, unlike the all-peers mocks used elsewhere.
func realisticRouter(peers []peer.ID, bucketSize int) *mockRouter {
	return &mockRouter{
		getClosestPeersFunc: func(ctx context.Context, k string) ([]peer.ID, error) {
			sorted := kb.SortClosestPeers(peers, kb.ConvertKey(k))
			return sorted[:min(bucketSize, len(sorted))], nil
		},
	}
}

// TestExploreSwarmDifferential cross-checks exploreSwarm against an independent
// reference across many network topologies, requested prefixes, and replication
// factors, using a realistic SortClosestPeers[:bucketSize] router with
// rf < bucketSize (the regime where queries also return sibling peers). For
// every case it asserts the covered prefix equals the reference and the regions
// hold exactly the peers under it (i.e. the responsible zone was fully explored,
// neither under- nor over-broadened), and that no key is dropped.
func TestExploreSwarmDifferential(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const bucketSize = 20
		rnd := random.New()
		networks := map[string][]peer.ID{
			"uniform-100":         rnd.Peers(100),
			"uniform-60":          rnd.Peers(60),
			"uniform-25":          rnd.Peers(25),
			"cluster-10-under-10": genPeersWithPrefix("10", 10),
			"dense-under-1":       genPeersWithPrefix("1", 50),
			"split-110-111":       slices.Concat(genPeersWithPrefix("110", 6), genPeersWithPrefix("111", 6)),
			"skewed-1-vs-11":      slices.Concat(genPeersWithPrefix("100", 1), genPeersWithPrefix("101", 11)),
			"deep-cluster-1011":   genPeersWithPrefix("1011", 8),
			"far-clusters":        slices.Concat(genPeersWithPrefix("0", 15), genPeersWithPrefix("111", 5)),
		}
		prefixes := []bitstr.Key{"", "0", "1", "00", "01", "10", "11", "000", "001", "010", "011", "100", "101", "110", "111"}
		rfs := []int{1, 3, 5, 8}

		keys := genBalancedMultihashes(4)

		for name, peers := range networks {
			peersTrie := peersToTrie(peers)
			for _, rf := range rfs {
				prov := SweepingProvider{
					router:            realisticRouter(peers, bucketSize),
					order:             bit256.ZeroKey(),
					logger:            defaultLogger,
					connectivity:      noopConnectivityChecker(),
					replicationFactor: rf,
					stats:             newOperationStats(time.Hour, time.Minute),
				}
				prov.connectivity.Start()
				synctest.Wait()
				require.True(t, prov.connectivity.IsOnline())

				for _, prefix := range prefixes {
					regions, coveredPrefix, err := prov.exploreSwarm(prefix)
					require.NoErrorf(t, err, "net=%s rf=%d prefix=%q", name, rf, prefix)

					wantPrefix := referenceCoveredPrefix(peersTrie, prefix, rf)
					require.Equalf(t, wantPrefix, coveredPrefix,
						"net=%s rf=%d prefix=%q: wrong covered prefix", name, rf, prefix)

					regionPeers := []peer.ID{}
					for _, r := range regions {
						regionPeers = append(regionPeers, keyspace.AllValues(r.Peers, bit256.ZeroKey())...)
					}
					require.ElementsMatchf(t, peersUnderTrie(peersTrie, wantPrefix), regionPeers,
						"net=%s rf=%d prefix=%q: regions don't hold exactly the peers under %q",
						name, rf, prefix, wantPrefix)

					// No under-replication: when the covered zone holds enough
					// peers, every region must too (extractMinimalRegions only
					// splits when both sides keep >= rf peers).
					if len(regionPeers) >= rf {
						for _, r := range regions {
							require.GreaterOrEqualf(t, r.Peers.Size(), rf,
								"net=%s rf=%d prefix=%q: region %q has %d < rf peers",
								name, rf, prefix, r.Prefix, r.Peers.Size())
						}
					}

					assigned := 0
					for _, r := range keyspace.AssignKeysToRegions(regions, keys) {
						assigned += r.Keys.Size()
					}
					require.Equalf(t, len(keys), assigned,
						"net=%s rf=%d prefix=%q: %d/%d keys dropped", name, rf, prefix, assigned, len(keys))
				}
				prov.connectivity.Close()
			}
		}
	})
}

// TestExploreSwarmFuzz randomizes network size, membership, requested prefix,
// and replication factor (rf < bucketSize) against the realistic router, and
// checks the same invariants as the differential test: the covered prefix and
// the explored peer set must match the independent reference for every case.
func TestExploreSwarmFuzz(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const bucketSize = 20
		rng := rand.New(rand.NewPCG(1, 1))
		pool := random.Peers(200)

		checker := noopConnectivityChecker()
		checker.Start()
		defer checker.Close()
		synctest.Wait()
		require.True(t, checker.IsOnline())

		prov := SweepingProvider{
			order:        bit256.ZeroKey(),
			logger:       defaultLogger,
			connectivity: checker,
			stats:        newOperationStats(time.Hour, time.Minute),
		}

		for c := range 250 {
			size := 1 + rng.IntN(len(pool))
			peers := make([]peer.ID, size)
			for i, idx := range rng.Perm(len(pool))[:size] {
				peers[i] = pool[idx]
			}
			peersTrie := peersToTrie(peers)
			rf := 1 + rng.IntN(12)

			var sb strings.Builder
			for range rng.IntN(7) {
				sb.WriteByte('0' + byte(rng.IntN(2)))
			}
			prefix := bitstr.Key(sb.String())

			prov.router = realisticRouter(peers, bucketSize)
			prov.replicationFactor = rf

			regions, coveredPrefix, err := prov.exploreSwarm(prefix)
			require.NoErrorf(t, err, "case=%d size=%d rf=%d prefix=%q", c, size, rf, prefix)

			wantPrefix := referenceCoveredPrefix(peersTrie, prefix, rf)
			require.Equalf(t, wantPrefix, coveredPrefix,
				"case=%d size=%d rf=%d prefix=%q: wrong covered prefix", c, size, rf, prefix)

			regionPeers := []peer.ID{}
			for _, r := range regions {
				regionPeers = append(regionPeers, keyspace.AllValues(r.Peers, bit256.ZeroKey())...)
			}
			require.ElementsMatchf(t, peersUnderTrie(peersTrie, wantPrefix), regionPeers,
				"case=%d size=%d rf=%d prefix=%q: regions don't hold exactly the peers under %q",
				c, size, rf, prefix, wantPrefix)
		}
	})
}

// TestExploreSwarmTinyNetwork covers networks smaller than the replication
// factor: exploreSwarm must broaden all the way to the empty prefix, return
// every peer, and never drop a key, for any requested prefix.
func TestExploreSwarmTinyNetwork(t *testing.T) {
	for _, nPeers := range []int{1, 2, 3} {
		t.Run(strconv.Itoa(nPeers), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				peers := random.Peers(nPeers)
				prov := SweepingProvider{
					router:            realisticRouter(peers, 20),
					order:             bit256.ZeroKey(),
					logger:            defaultLogger,
					connectivity:      noopConnectivityChecker(),
					replicationFactor: 8, // larger than the network
					stats:             newOperationStats(time.Hour, time.Minute),
				}
				prov.connectivity.Start()
				defer prov.connectivity.Close()
				synctest.Wait()
				require.True(t, prov.connectivity.IsOnline())

				keys := genBalancedMultihashes(3)
				for _, prefix := range []bitstr.Key{"", "0", "1", "01", "10", "111"} {
					regions, coveredPrefix, err := prov.exploreSwarm(prefix)
					require.NoErrorf(t, err, "prefix=%q", prefix)
					require.Emptyf(t, coveredPrefix,
						"network smaller than rf must broaden to empty prefix, got %q for %q", coveredPrefix, prefix)

					regionPeers := []peer.ID{}
					for _, r := range regions {
						regionPeers = append(regionPeers, keyspace.AllValues(r.Peers, bit256.ZeroKey())...)
					}
					require.ElementsMatchf(t, peers, regionPeers, "prefix=%q: not all peers returned", prefix)

					assigned := 0
					for _, r := range keyspace.AssignKeysToRegions(regions, keys) {
						assigned += r.Keys.Size()
					}
					require.Equalf(t, len(keys), assigned, "prefix=%q: keys dropped", prefix)
				}
			})
		})
	}
}

// TestProvidePipelineRoutesKeysToClosestPeers is the end-to-end correctness
// property: a key explored, regioned, and assigned must land in a region that
// contains the key's true rf closest peers in the whole network. If exploration
// missed a peer, mis-split a region, or mis-assigned a key, the record would be
// sent to the wrong peers — this catches all three.
func TestProvidePipelineRoutesKeysToClosestPeers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const bucketSize = 20
		networks := map[string][]peer.ID{
			"uniform-80":     random.Peers(80),
			"split-110-111":  slices.Concat(genPeersWithPrefix("110", 7), genPeersWithPrefix("111", 7)),
			"skewed-1-vs-11": slices.Concat(genPeersWithPrefix("100", 1), genPeersWithPrefix("101", 13)),
			"far-clusters":   slices.Concat(genPeersWithPrefix("0", 18), genPeersWithPrefix("111", 6)),
		}
		keys := genMultihashes(64)

		for _, rf := range []int{3, 6} {
			for name, peers := range networks {
				prov := SweepingProvider{
					router:            realisticRouter(peers, bucketSize),
					order:             bit256.ZeroKey(),
					logger:            defaultLogger,
					connectivity:      noopConnectivityChecker(),
					replicationFactor: rf,
					stats:             newOperationStats(time.Hour, time.Minute),
				}
				prov.connectivity.Start()
				synctest.Wait()
				require.True(t, prov.connectivity.IsOnline())

				for _, k := range keys {
					prefix := bitstr.Key(key.BitString(keyspace.MhToBit256(k))[:4])
					regions, _, err := prov.exploreSwarm(prefix)
					require.NoErrorf(t, err, "net=%s rf=%d key prefix=%q", name, rf, prefix)
					regions = keyspace.AssignKeysToRegions(regions, []mh.Multihash{k})

					var region *keyspace.Region
					for i := range regions {
						if regions[i].Keys.Size() > 0 {
							region = &regions[i]
							break
						}
					}
					require.NotNilf(t, region, "net=%s rf=%d: key not assigned to any region", name, rf)

					wantClosest := kb.SortClosestPeers(peers, kb.ConvertKey(string(k)))
					wantClosest = wantClosest[:min(rf, len(wantClosest))]
					regionPeers := keyspace.AllValues(region.Peers, bit256.ZeroKey())
					for _, want := range wantClosest {
						require.Containsf(t, regionPeers, want,
							"net=%s rf=%d: key's region %q missing one of its %d closest peers",
							name, rf, region.Prefix, len(wantClosest))
					}
				}
				prov.connectivity.Close()
			}
		}
	})
}

func TestGroupAndScheduleKeysByPrefix(t *testing.T) {
	prov := SweepingProvider{
		order:             bit256.ZeroKey(),
		reprovideInterval: time.Hour,

		schedule:      trie.New[bitstr.Key, time.Duration](),
		scheduleTimer: time.NewTimer(time.Hour),

		cachedAvgPrefixLen: 3,
		lastAvgPrefixLen:   time.Now(),
	}

	mhs00000 := genMultihashesMatchingPrefix("00000", 3)
	mhs00000 = append(mhs00000, mhs00000[0])
	mhs1000 := genMultihashesMatchingPrefix("0100", 2)

	mhs := append(mhs00000, mhs1000...)

	prefixes := prov.groupAndScheduleKeysByPrefix(mhs, false)
	require.Len(t, prefixes, 2)
	require.Contains(t, prefixes, bitstr.Key("000"))
	require.Len(t, prefixes["000"], 3) // no duplicate entry
	require.Contains(t, prefixes, bitstr.Key("010"))
	require.Len(t, prefixes["010"], 2)

	// Schedule is still empty
	require.True(t, prov.schedule.IsEmptyLeaf())

	prefixes = prov.groupAndScheduleKeysByPrefix(mhs, true)
	require.Len(t, prefixes, 2)
	require.Contains(t, prefixes, bitstr.Key("000"))
	require.Len(t, prefixes["000"], 3)
	require.Contains(t, prefixes, bitstr.Key("010"))
	require.Len(t, prefixes["010"], 2)

	// Schedule now contains the 2 prefixes
	require.Equal(t, 2, prov.schedule.Size())

	// Manually add prefix to schedule
	prov.schedule.Add(bitstr.Key("11111"), 0*time.Second)
	mhs11111 := genMultihashesMatchingPrefix("11111", 4)
	mhs1110 := genMultihashesMatchingPrefix("1110", 4)
	mhs = append(mhs11111, mhs1110...)
	prefixes = prov.groupAndScheduleKeysByPrefix(mhs, true)
	// All keys should be consolidated into "111"
	require.Len(t, prefixes, 1)
	require.Contains(t, prefixes, bitstr.Key("111"))
	require.Len(t, prefixes["111"], 8)

	// "11111" is removed from schedule
	found, _ := trie.Find(prov.schedule, bitstr.Key("11111"))
	require.False(t, found)
	found, _ = trie.Find(prov.schedule, bitstr.Key("111"))
	require.True(t, found)

	prov.schedule.Add(bitstr.Key("10"), 0*time.Second)

	mhs1 := genMultihashesMatchingPrefix("10", 6)
	prefixes = prov.groupAndScheduleKeysByPrefix(mhs1, true)
	require.Len(t, prefixes, 1)
	require.Contains(t, prefixes, bitstr.Key("10"))
	require.Len(t, prefixes["10"], 6)
}

func noWarningsNorAbove(obsLogs *observer.ObservedLogs) bool {
	return obsLogs.Filter(func(le observer.LoggedEntry) bool {
		return le.Level >= zap.WarnLevel
	}).Len() == 0
}

func takeAllContainsErr(obsLogs *observer.ObservedLogs, errStr string) bool {
	for _, le := range obsLogs.TakeAll() {
		if le.Level >= zap.WarnLevel && strings.Contains(le.Message, errStr) {
			return true
		}
	}
	return false
}

func noopConnectivityChecker() *connectivity.ConnectivityChecker {
	connChecker, err := connectivity.New(func() bool { return true })
	if err != nil {
		panic(err)
	}
	return connChecker
}

func provideCounter() metric.Int64Counter {
	meter := otel.Meter("github.com/libp2p/go-libp2p-kad-dht/provider")
	provideCounter, err := meter.Int64Counter(
		"provider.provides",
		metric.WithDescription("Number of successful provides since node is running"),
	)
	if err != nil {
		panic(err)
	}
	return provideCounter
}

func TestIndividualProvideSingle(t *testing.T) {
	obsCore, obsLogs := observer.New(zap.WarnLevel)
	logging.SetPrimaryCore(obsCore)
	logging.SetAllLoggers(logging.LevelError)
	logging.SetLogLevel(DefaultLoggerName, "warn")

	mhs := genMultihashes(1)
	prefix := bitstr.Key("1011101111")

	closestPeers := []peer.ID{peer.ID("12BoooooPEER1"), peer.ID("12BoooooPEER2")}
	router := &mockRouter{
		getClosestPeersFunc: func(ctx context.Context, k string) ([]peer.ID, error) {
			return closestPeers, nil
		},
	}

	advertisements := make(map[peer.ID]int, len(closestPeers))
	msgSenderLk := sync.Mutex{}
	msgSender := &mockMsgSender{
		sendMessageFunc: func(ctx context.Context, p peer.ID, m *pb.Message) error {
			msgSenderLk.Lock()
			defer msgSenderLk.Unlock()
			advertisements[p]++
			return nil
		},
	}
	r := SweepingProvider{
		ctx:                       context.Background(),
		router:                    router,
		msgSender:                 msgSender,
		reprovideInterval:         time.Hour,
		maxProvideConnsPerWorker:  2,
		sendProviderRecordTimeout: DefaultSendProviderRecordTimeout,
		provideQueue:              queue.NewProvideQueue(),
		reprovideQueue:            queue.NewReprovideQueue(),
		connectivity:              noopConnectivityChecker(),
		schedule:                  trie.New[bitstr.Key, time.Duration](),
		scheduleTimer:             time.NewTimer(time.Hour),
		getSelfAddrs:              func() []ma.Multiaddr { return nil },
		addLocalRecord:            func(mh mh.Multihash) error { return nil },
		provideCounter:            provideCounter(),
		logger:                    defaultLogger,
	}

	assertAdvertisementCount := func(n int) {
		msgSenderLk.Lock()
		defer msgSenderLk.Unlock()
		for _, count := range advertisements {
			require.Equal(t, n, count)
		}
	}

	// Providing no keys returns no error
	r.individualProvide(prefix, nil, false)
	require.True(t, noWarningsNorAbove(obsLogs))
	assertAdvertisementCount(0)

	// Providing a single key - success
	r.individualProvide(prefix, mhs, false)
	require.True(t, noWarningsNorAbove(obsLogs))
	assertAdvertisementCount(1)

	// Providing a single key - failure
	gcpErr := errors.New("GetClosestPeers error")
	router.getClosestPeersFunc = func(ctx context.Context, k string) ([]peer.ID, error) {
		return nil, gcpErr
	}
	r.individualProvide(prefix, mhs, false)
	require.True(t, takeAllContainsErr(obsLogs, gcpErr.Error()))
	assertAdvertisementCount(1)
	// Verify failed key ends up in the provide queue.
	_, keys, ok := r.provideQueue.Dequeue()
	require.True(t, ok)
	require.Equal(t, mhs, keys)

	// Reproviding a single key - failure
	r.individualProvide(prefix, mhs, true)
	require.True(t, takeAllContainsErr(obsLogs, gcpErr.Error()))
	assertAdvertisementCount(1)
	// Verify failed prefix ends up in the reprovide queue.
	dequeued, ok := r.reprovideQueue.Dequeue()
	require.True(t, ok)
	require.Equal(t, prefix, dequeued)
}

func TestIndividualProvideMultiple(t *testing.T) {
	obsCore, obsLogs := observer.New(zap.WarnLevel)
	logging.SetPrimaryCore(obsCore)
	logging.SetAllLoggers(logging.LevelError)
	logging.SetLogLevel(DefaultLoggerName, "warn")

	ks := genMultihashes(2)
	prefix := bitstr.Key("")
	closestPeers := []peer.ID{peer.ID("12BoooooPEER1"), peer.ID("12BoooooPEER2")}
	router := &mockRouter{
		getClosestPeersFunc: func(ctx context.Context, k string) ([]peer.ID, error) {
			return closestPeers, nil
		},
	}
	advertisements := make(map[string]map[peer.ID]int, len(closestPeers))
	for _, k := range ks {
		advertisements[string(k)] = make(map[peer.ID]int, len(closestPeers))
	}
	msgSenderLk := sync.Mutex{}
	msgSender := &mockMsgSender{
		sendMessageFunc: func(ctx context.Context, p peer.ID, m *pb.Message) error {
			msgSenderLk.Lock()
			defer msgSenderLk.Unlock()
			_, k, err := mh.MHFromBytes(m.GetKey())
			require.NoError(t, err)
			advertisements[string(k)][p]++
			return nil
		},
	}
	reprovideInterval := time.Hour
	maxDelay := time.Minute
	ds := datastore.NewMapDatastore()
	r := SweepingProvider{
		ctx:                       context.Background(),
		router:                    router,
		msgSender:                 msgSender,
		reprovideInterval:         reprovideInterval,
		maxReprovideDelay:         maxDelay,
		maxProvideConnsPerWorker:  2,
		sendProviderRecordTimeout: DefaultSendProviderRecordTimeout,
		provideQueue:              queue.NewProvideQueue(),
		reprovideQueue:            queue.NewReprovideQueue(),
		connectivity:              noopConnectivityChecker(),
		schedule:                  trie.New[bitstr.Key, time.Duration](),
		scheduleTimer:             time.NewTimer(time.Hour),
		getSelfAddrs:              func() []ma.Multiaddr { return nil },
		addLocalRecord:            func(mh mh.Multihash) error { return nil },
		provideCounter:            provideCounter(),
		stats:                     newOperationStats(reprovideInterval, maxDelay),
		datastore:                 ds,
		logger:                    defaultLogger,
	}

	assertAdvertisementCount := func(n int) {
		msgSenderLk.Lock()
		defer msgSenderLk.Unlock()
		for _, peerAllocs := range advertisements {
			for _, count := range peerAllocs {
				require.Equal(t, n, count)
			}
		}
	}

	// Providing two keys - success
	r.individualProvide(prefix, ks, false)
	require.True(t, noWarningsNorAbove(obsLogs))
	assertAdvertisementCount(1)

	// Providing two keys - failure
	gcpErr := errors.New("GetClosestPeers error")
	router.getClosestPeersFunc = func(ctx context.Context, k string) ([]peer.ID, error) {
		return nil, gcpErr
	}
	r.individualProvide(prefix, ks, false)
	require.True(t, takeAllContainsErr(obsLogs, gcpErr.Error()))
	assertAdvertisementCount(1)
	// Assert keys are added to provide queue
	require.Equal(t, len(ks), r.provideQueue.Size())
	pendingKeys := []mh.Multihash{}
	for !r.provideQueue.IsEmpty() {
		_, keys, ok := r.provideQueue.Dequeue()
		require.True(t, ok)
		pendingKeys = append(pendingKeys, keys...)
	}
	require.ElementsMatch(t, pendingKeys, ks)

	// Reproviding two keys - failure
	r.individualProvide(prefix, ks, true)
	require.True(t, takeAllContainsErr(obsLogs, "all individual provides failed for prefix"))
	assertAdvertisementCount(1)
	// Assert prefix is added to reprovide queue.
	dequeued, ok := r.reprovideQueue.Dequeue()
	require.True(t, ok)
	require.Equal(t, prefix, dequeued)

	// Providing two keys - 1 success, 1 failure
	lk := sync.Mutex{}
	counter := 0
	router.getClosestPeersFunc = func(ctx context.Context, k string) ([]peer.ID, error) {
		lk.Lock()
		defer lk.Unlock()
		counter++
		if counter%2 == 1 {
			return nil, errors.New("GetClosestPeers error")
		}
		return closestPeers, nil
	}

	r.individualProvide(prefix, ks, false)
	require.True(t, takeAllContainsErr(obsLogs, gcpErr.Error()))
	// Verify one key was now provided 2x, and other key only 1x since it just failed.
	msgSenderLk.Lock()
	require.Equal(t, 3, advertisements[string(ks[0])][closestPeers[0]]+advertisements[string(ks[1])][closestPeers[0]])
	require.Equal(t, 3, advertisements[string(ks[0])][closestPeers[1]]+advertisements[string(ks[1])][closestPeers[1]])
	msgSenderLk.Unlock()

	// Failed key was added to provide queue
	require.Equal(t, 1, r.provideQueue.Size())
	_, pendingKeys, ok = r.provideQueue.Dequeue()
	require.True(t, ok)
	require.Len(t, pendingKeys, 1)
	require.Contains(t, ks, pendingKeys[0])
	require.True(t, r.reprovideQueue.IsEmpty())
	require.True(t, r.provideQueue.IsEmpty())

	r.individualProvide(prefix, ks, true)
	require.True(t, noWarningsNorAbove(obsLogs))
	// Verify only one of the 2 keys was provided. Providing failed for the other.
	msgSenderLk.Lock()
	require.Equal(t, 4, advertisements[string(ks[0])][closestPeers[0]]+advertisements[string(ks[1])][closestPeers[0]])
	require.Equal(t, 4, advertisements[string(ks[0])][closestPeers[1]]+advertisements[string(ks[1])][closestPeers[1]])
	msgSenderLk.Unlock()

	// Failed key shouldn't be added to provide nor reprovide queue, since the
	// reprovide didn't completely failed.
	require.True(t, r.reprovideQueue.IsEmpty())
	require.True(t, r.provideQueue.IsEmpty())
}

func TestHandleReprovide(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		online := atomic.Bool{}
		online.Store(true)
		connectivityCheckInterval := time.Second
		offlineDelay := time.Minute
		connChecker, err := connectivity.New(
			func() bool { return online.Load() },
			connectivity.WithOfflineDelay(offlineDelay),
			connectivity.WithOnlineCheckInterval(connectivityCheckInterval),
		)
		require.NoError(t, err)
		defer connChecker.Close()

		prov := SweepingProvider{
			order: bit256.ZeroKey(),

			connectivity: connChecker,

			cycleStart:    time.Now(),
			scheduleTimer: time.NewTimer(time.Hour),
			schedule:      trie.New[bitstr.Key, time.Duration](),

			reprovideQueue: queue.NewReprovideQueue(),
			workerPool:     reservedpool.New[workerType](1, nil), // single worker

			reprovideInterval: time.Minute,
			maxReprovideDelay: 5 * time.Second,

			getSelfAddrs: func() []ma.Multiaddr { return nil },
			logger:       defaultLogger,
		}
		prov.scheduleTimer.Stop()
		connChecker.Start()
		defer connChecker.Close()

		prefixes := []bitstr.Key{
			"00",
			"10",
			"11",
		}

		// Empty schedule -> early return
		prov.handleReprovide()
		require.Zero(t, prov.scheduleCursor)

		// Single prefix in schedule
		prov.schedule.Add(prefixes[0], prov.reprovideTimeForPrefix(prefixes[0]))
		prov.scheduleCursor = prefixes[0]
		prov.handleReprovide()
		require.Equal(t, prefixes[0], prov.scheduleCursor)

		// Two prefixes in schedule
		time.Sleep(time.Nanosecond) // advance 1 tick into the reprovide cycle
		prov.schedule.Add(prefixes[1], prov.reprovideTimeForPrefix(prefixes[1]))
		prov.handleReprovide() // reprovides prefixes[0], set scheduleCursor to prefixes[1]
		require.Equal(t, prefixes[1], prov.scheduleCursor)

		// Wait more than reprovideInterval to call handleReprovide again.
		// All prefixes should be added to the reprovide queue.
		time.Sleep(prov.reprovideInterval + 1)
		require.True(t, prov.reprovideQueue.IsEmpty())
		prov.handleReprovide()
		require.Equal(t, prefixes[1], prov.scheduleCursor)

		require.Equal(t, 2, prov.reprovideQueue.Size())
		dequeued, ok := prov.reprovideQueue.Dequeue()
		require.True(t, ok)
		require.Equal(t, prefixes[0], dequeued)
		dequeued, ok = prov.reprovideQueue.Dequeue()
		require.True(t, ok)
		require.Equal(t, prefixes[1], dequeued)
		require.True(t, prov.reprovideQueue.IsEmpty())

		// Go in time past prefixes[1] and prefixes[2]
		prov.schedule.Add(prefixes[2], prov.reprovideTimeForPrefix(prefixes[2]))
		time.Sleep(3 * prov.reprovideInterval / 4)
		// reprovides prefixes[1], add prefixes[2] to reprovide queue, set
		// scheduleCursor to prefixes[0]
		prov.handleReprovide()
		require.Equal(t, prefixes[0], prov.scheduleCursor)

		require.Equal(t, 1, prov.reprovideQueue.Size())
		dequeued, ok = prov.reprovideQueue.Dequeue()
		require.True(t, ok)
		require.Equal(t, prefixes[2], dequeued)
		require.True(t, prov.reprovideQueue.IsEmpty())

		time.Sleep(prov.reprovideInterval / 4)

		// Node goes offline -> prefixes are queued
		online.Store(false)
		prov.connectivity.TriggerCheck()
		synctest.Wait()
		require.False(t, prov.connectivity.IsOnline())
		require.True(t, prov.reprovideQueue.IsEmpty())
		prov.handleReprovide()
		require.Equal(t, 1, prov.reprovideQueue.Size())

		// Node comes back online
		online.Store(true)
		time.Sleep(connectivityCheckInterval)
		synctest.Wait()
		require.True(t, prov.connectivity.IsOnline())
	})
}

func TestClose(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pid, err := peer.Decode("12BoooooPEER")
		require.NoError(t, err)
		router := &mockRouter{
			getClosestPeersFunc: func(ctx context.Context, k string) ([]peer.ID, error) {
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				return []peer.ID{peer.ID("12BoooooPEER1"), peer.ID("12BoooooPEER2")}, nil
			},
		}
		msgSender := &mockMsgSender{
			sendMessageFunc: func(ctx context.Context, p peer.ID, m *pb.Message) error {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				return nil
			},
		}
		prov, err := New(
			WithPeerID(pid),
			WithRouter(router),
			WithMessageSender(msgSender),
			WithReplicationFactor(1),
			WithMaxWorkers(4),
			WithDedicatedBurstWorkers(0),
			WithDedicatedPeriodicWorkers(0),
			WithSelfAddrs(getMockAddrs(t)),
		)
		require.NoError(t, err)
		synctest.Wait()

		mhs := genMultihashes(128)
		err = prov.StartProviding(false, mhs...)
		require.NoError(t, err)
		synctest.Wait()                        // wait for connectivity check
		time.Sleep(prov.reprovideInterval / 2) // some keys should have been reprovided
		synctest.Wait()

		err = prov.Close()
		require.NoError(t, err)
		synctest.Wait()

		newMh := random.Multihashes(1)[0]

		err = prov.StartProviding(false, newMh)
		require.ErrorIs(t, err, ErrClosed)
		err = prov.StopProviding(newMh)
		require.ErrorIs(t, err, ErrClosed)
		err = prov.ProvideOnce(newMh)
		require.ErrorIs(t, err, ErrClosed)
		require.NoError(t, prov.Clear())

		err = prov.workerPool.Acquire(burstWorker)
		require.ErrorIs(t, err, reservedpool.ErrClosed)
	})
}

func TestProvideOnce(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pid, err := peer.Decode("12BoooooPEER")
		require.NoError(t, err)

		online := atomic.Bool{} // false, start offline
		router := &mockRouter{
			getClosestPeersFunc: func(ctx context.Context, k string) ([]peer.ID, error) {
				if online.Load() {
					return []peer.ID{pid}, nil
				}
				return nil, errors.New("offline")
			},
		}
		provideCount := atomic.Int32{}
		msgSender := &mockMsgSender{
			sendMessageFunc: func(ctx context.Context, p peer.ID, m *pb.Message) error {
				provideCount.Add(1)
				return nil
			},
		}

		checkInterval := time.Second
		offlineDelay := time.Minute

		opts := []Option{
			WithPeerID(pid),
			WithRouter(router),
			WithMessageSender(msgSender),
			WithSelfAddrs(getMockAddrs(t)),
			WithOfflineDelay(offlineDelay),
			WithConnectivityCheckOnlineInterval(checkInterval),
		}
		prov, err := New(opts...)
		require.NoError(t, err)
		defer prov.Close()

		h := genMultihashes(1)[0]

		// Node is offline, no provide should happen
		err = prov.ProvideOnce(h)
		require.NoError(t, err)
		require.True(t, prov.provideQueue.IsEmpty())
		require.Equal(t, int32(0), provideCount.Load(), "should not have provided when offline 0")

		// Wait for provider to come online
		online.Store(true)
		time.Sleep(checkInterval) // trigger connectivity check
		synctest.Wait()
		require.True(t, prov.connectivity.IsOnline())

		// Set the provider as disconnected
		online.Store(false)
		synctest.Wait()
		err = prov.ProvideOnce(h)
		require.NoError(t, err)
		synctest.Wait() // wait for ProvideOnce to finish
		require.Equal(t, int32(0), provideCount.Load(), "should not have provided when offline 1")
		// Ensure the key is in the provide queue
		_, keys, ok := prov.provideQueue.Dequeue()
		require.True(t, ok)
		require.Equal(t, 1, len(keys))
		require.Equal(t, h, keys[0])

		// Set the provider as online
		online.Store(true)
		time.Sleep(checkInterval) // trigger connectivity check
		synctest.Wait()
		require.True(t, prov.connectivity.IsOnline())
		err = prov.ProvideOnce(h)
		require.NoError(t, err)
		synctest.Wait()
		require.Equal(t, int32(1), provideCount.Load())
	})
}

func TestStartProvidingSingle(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pid, err := peer.Decode("12BoooooPEER")
		require.NoError(t, err)
		replicationFactor := 4
		h := genMultihashes(1)[0]

		reprovideInterval := time.Hour

		prefixLen := 4
		peers := make([]peer.ID, replicationFactor)
		seen := make(map[peer.ID]struct{}, replicationFactor)
		peers[0], err = peer.Decode("12BooooPEER1")
		require.NoError(t, err)
		seen[peers[0]] = struct{}{}
		kbKey := keyspace.KeyToBytes(keyspace.PeerIDToBit256(peers[0]))
		for i := 1; i < len(peers); {
			p, err := kb.GenRandPeerIDWithCPL(kbKey, uint(prefixLen))
			require.NoError(t, err)
			if _, ok := seen[p]; ok {
				continue
			}
			seen[p] = struct{}{}
			peers[i] = p
			i++
		}
		// Sort peers from closest to h, to furthest
		slices.SortFunc(peers, func(a, b peer.ID) int {
			targetKey := keyspace.MhToBit256(h)
			return keyspace.PeerIDToBit256(a).Xor(targetKey).Compare(keyspace.PeerIDToBit256(b).Xor(targetKey))
		})

		getClosestPeersCount := atomic.Int32{}
		router := &mockRouter{
			getClosestPeersFunc: func(ctx context.Context, k string) ([]peer.ID, error) {
				getClosestPeersCount.Add(1)
				return peers, nil
			},
		}
		provideCount := atomic.Int32{}
		msgSender := &mockMsgSender{
			sendMessageFunc: func(ctx context.Context, p peer.ID, m *pb.Message) error {
				provideCount.Add(1)
				return nil
			},
		}
		checkInterval := time.Second
		offlineDelay := time.Minute
		opts := []Option{
			WithReplicationFactor(replicationFactor),
			WithReprovideInterval(reprovideInterval),
			WithPeerID(pid),
			WithRouter(router),
			WithMessageSender(msgSender),
			WithSelfAddrs(getMockAddrs(t)),
			WithOfflineDelay(offlineDelay),
			WithConnectivityCheckOnlineInterval(checkInterval),
		}
		prov, err := New(opts...)
		require.NoError(t, err)
		defer prov.Close()

		synctest.Wait()
		require.True(t, prov.connectivity.IsOnline())
		prov.avgPrefixLenLk.Lock()
		require.GreaterOrEqual(t, prov.cachedAvgPrefixLen, 0)
		prov.avgPrefixLenLk.Unlock()

		err = prov.StartProviding(true, h)
		require.NoError(t, err)
		synctest.Wait()
		require.Equal(t, int32(len(peers)), provideCount.Load())
		expectedGCPCount := 1 + approxPrefixLenGCPCount + 1 // 1 for initial, approxPrefixLenGCPCount for prefix length estimation, 1 for the provide
		require.Equal(t, expectedGCPCount, int(getClosestPeersCount.Load()))

		// Verify reprovide is scheduled.
		prov.scheduleLk.Lock()
		require.Equal(t, 1, prov.schedule.Size())
		scheduledPrefix, found := keyspace.FindPrefixOfKey(prov.schedule, keyspace.MhToBit256(h))
		require.True(t, found, "prefix covering key not inserted in schedule")
		_, reprovideTime := trie.Find(prov.schedule, scheduledPrefix)
		require.Equal(t, prov.reprovideTimeForPrefix(scheduledPrefix), reprovideTime)
		prov.scheduleLk.Unlock()

		// Try to provide the same key again -> noop
		err = prov.StartProviding(false, h)
		require.NoError(t, err)
		synctest.Wait()
		require.Equal(t, int32(len(peers)), provideCount.Load())
		require.Equal(t, expectedGCPCount, int(getClosestPeersCount.Load()))

		// Verify reprovide happens as scheduled.
		time.Sleep(reprovideTime)
		synctest.Wait()
		expectedGCPCount++ // for the reprovide
		require.Equal(t, 2*int32(len(peers)), provideCount.Load())
		require.Equal(t, expectedGCPCount, int(getClosestPeersCount.Load()))

		time.Sleep(reprovideInterval)
		synctest.Wait()
		expectedGCPCount++ // for the reprovide
		require.Equal(t, 3*int32(len(peers)), provideCount.Load())
		require.Equal(t, expectedGCPCount, int(getClosestPeersCount.Load()))

		time.Sleep(reprovideInterval)
		synctest.Wait()
		expectedGCPCount++ // for the reprovide
		require.Equal(t, 4*int32(len(peers)), provideCount.Load())
		require.Equal(t, expectedGCPCount, int(getClosestPeersCount.Load()))
	})
}

func TestNoScheduleMode(t *testing.T) {
	t.Run("WithReprovideInterval(0) constructs successfully", func(t *testing.T) {
		pid, err := peer.Decode("12BoooooPEER")
		require.NoError(t, err)

		prov, err := New(
			WithPeerID(pid),
			WithRouter(&mockRouter{
				getClosestPeersFunc: func(_ context.Context, _ string) ([]peer.ID, error) {
					return []peer.ID{pid}, nil
				},
			}),
			WithMessageSender(&mockMsgSender{
				sendMessageFunc: func(_ context.Context, _ peer.ID, _ *pb.Message) error { return nil },
			}),
			WithSelfAddrs(getMockAddrs(t)),
			WithReprovideInterval(0),
		)
		require.NoError(t, err)
		require.NotNil(t, prov)
		require.False(t, prov.scheduleEnabled(), "scheduleEnabled should report false with interval=0")
		require.NoError(t, prov.Close())
	})

	t.Run("WithReprovideInterval(-1) is rejected", func(t *testing.T) {
		pid, err := peer.Decode("12BoooooPEER")
		require.NoError(t, err)
		_, err = New(
			WithPeerID(pid),
			WithRouter(&mockRouter{}),
			WithMessageSender(&mockMsgSender{}),
			WithSelfAddrs(getMockAddrs(t)),
			WithReprovideInterval(-1),
		)
		require.Error(t, err)
	})

	t.Run("StartProviding publishes records and skips schedule in no-schedule mode", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			pid, err := peer.Decode("12BoooooPEER")
			require.NoError(t, err)
			replicationFactor := 4
			h := genMultihashes(1)[0]

			prefixLen := 4
			peers := make([]peer.ID, replicationFactor)
			seen := make(map[peer.ID]struct{}, replicationFactor)
			peers[0], err = peer.Decode("12BooooPEER1")
			require.NoError(t, err)
			seen[peers[0]] = struct{}{}
			kbKey := keyspace.KeyToBytes(keyspace.PeerIDToBit256(peers[0]))
			for i := 1; i < len(peers); {
				p, err := kb.GenRandPeerIDWithCPL(kbKey, uint(prefixLen))
				require.NoError(t, err)
				if _, ok := seen[p]; ok {
					continue
				}
				seen[p] = struct{}{}
				peers[i] = p
				i++
			}

			router := &mockRouter{
				getClosestPeersFunc: func(_ context.Context, _ string) ([]peer.ID, error) {
					return peers, nil
				},
			}
			provideCount := atomic.Int32{}
			msgSender := &mockMsgSender{
				sendMessageFunc: func(_ context.Context, _ peer.ID, _ *pb.Message) error {
					provideCount.Add(1)
					return nil
				},
			}

			checkInterval := time.Second
			offlineDelay := time.Minute
			prov, err := New(
				WithReplicationFactor(replicationFactor),
				WithReprovideInterval(0),
				WithPeerID(pid),
				WithRouter(router),
				WithMessageSender(msgSender),
				WithSelfAddrs(getMockAddrs(t)),
				WithOfflineDelay(offlineDelay),
				WithConnectivityCheckOnlineInterval(checkInterval),
			)
			require.NoError(t, err)
			defer prov.Close()

			synctest.Wait()
			require.True(t, prov.connectivity.IsOnline())

			err = prov.StartProviding(true, h)
			require.NoError(t, err)
			synctest.Wait()
			require.Equal(t, int32(len(peers)), provideCount.Load(), "key should be published to every peer")

			// Stats must not panic on the modulo-by-zero schedule math.
			s, err := prov.Stats(t.Context())
			require.NoError(t, err)
			require.False(t, s.Closed)
			require.Equal(t, time.Duration(0), s.Timing.ReprovidesInterval)
			require.Equal(t, time.Duration(0), s.Timing.CurrentTimeOffset)

			// Schedule must remain empty in no-schedule mode.
			prov.scheduleLk.Lock()
			require.Equal(t, 0, prov.schedule.Size(), "schedule should be empty in no-schedule mode")
			prov.scheduleLk.Unlock()

			// Keystore must stay empty: StartProviding collapses to
			// ProvideOnce semantics in no-schedule mode.
			ksSize, err := prov.keystore.Size(t.Context())
			require.NoError(t, err)
			require.Equal(t, 0, ksSize, "keystore should be empty when StartProviding runs in no-schedule mode")

			// AddToSchedule and RefreshSchedule are no-ops.
			require.NoError(t, prov.AddToSchedule(h))
			require.NoError(t, prov.RefreshSchedule())
			prov.scheduleLk.Lock()
			require.Equal(t, 0, prov.schedule.Size(), "no-op AddToSchedule/RefreshSchedule should leave schedule empty")
			prov.scheduleLk.Unlock()

			// No periodic reprovide ever fires.
			time.Sleep(time.Hour)
			synctest.Wait()
			require.Equal(t, int32(len(peers)), provideCount.Load(), "no reprovide should have fired in no-schedule mode")
		})
	})
}

const bitsPerByte = 8

func TestStartProvidingMany(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pid, err := peer.Decode("12BoooooPEER")
		require.NoError(t, err)

		nKeysExponent := 10
		nKeys := 1 << nKeysExponent
		mhs := genBalancedMultihashes(nKeysExponent)

		replicationFactor := 4
		peerPrefixBitlen := 6
		require.LessOrEqual(t, peerPrefixBitlen, bitsPerByte)
		var nPeers byte = 1 << peerPrefixBitlen // 2**peerPrefixBitlen
		peers := make([]peer.ID, nPeers)
		for i := range nPeers {
			b := i << (bitsPerByte - peerPrefixBitlen)
			k := [32]byte{b}
			peers[i], err = kb.GenRandPeerIDWithCPL(k[:], uint(peerPrefixBitlen))
			require.NoError(t, err)
		}

		reprovideInterval := time.Hour

		router := &mockRouter{
			getClosestPeersFunc: func(ctx context.Context, k string) ([]peer.ID, error) {
				sortedPeers := kb.SortClosestPeers(peers, kb.ConvertKey(k))
				return sortedPeers[:min(replicationFactor, len(peers))], nil
			},
		}
		msgSenderLk := sync.Mutex{}
		addProviderRpcs := make(map[string]map[peer.ID]int) // key -> peerid -> count
		provideCount := atomic.Int32{}
		msgSender := &mockMsgSender{
			sendMessageFunc: func(ctx context.Context, p peer.ID, m *pb.Message) error {
				msgSenderLk.Lock()
				defer msgSenderLk.Unlock()
				_, k, err := mh.MHFromBytes(m.GetKey())
				require.NoError(t, err)
				if _, ok := addProviderRpcs[string(k)]; !ok {
					addProviderRpcs[string(k)] = make(map[peer.ID]int)
				}
				addProviderRpcs[string(k)][p]++
				provideCount.Add(1)
				return nil
			},
		}
		opts := []Option{
			WithReprovideInterval(reprovideInterval),
			WithReplicationFactor(replicationFactor),
			WithMaxWorkers(1),
			WithDedicatedBurstWorkers(0),
			WithDedicatedPeriodicWorkers(0),
			WithPeerID(pid),
			WithRouter(router),
			WithMessageSender(msgSender),
			WithSelfAddrs(getMockAddrs(t)),
		}
		prov, err := New(opts...)
		require.NoError(t, err)
		defer prov.Close()
		synctest.Wait()
		require.True(t, prov.connectivity.IsOnline())

		err = prov.StartProviding(true, mhs...)
		require.NoError(t, err)
		synctest.Wait()
		require.Equal(t, int32(len(mhs)*replicationFactor), provideCount.Load())

		// Each key should have been provided at least once.
		msgSenderLk.Lock()
		require.Equal(t, nKeys, len(addProviderRpcs))
		for k, holders := range addProviderRpcs {
			// Verify that all keys have been provided to exactly replicationFactor
			// distinct peers.
			require.Len(t, holders, replicationFactor)
			for _, count := range holders {
				require.Equal(t, 1, count)
			}
			// Verify provider records are assigned to the closest peers
			closestPeers := kb.SortClosestPeers(peers, kb.ConvertKey(k))[:replicationFactor]
			for _, p := range closestPeers {
				require.Contains(t, holders, p)
			}
		}

		step := 10 * time.Second
		// Test reprovides, clear addProviderRpcs
		clear(addProviderRpcs)
		msgSenderLk.Unlock()
		for range reprovideInterval / step {
			time.Sleep(step)
		}
		synctest.Wait()
		require.Equal(t, 2*int32(len(mhs)*replicationFactor), provideCount.Load(), "should have reprovided all keys at least once")

		msgSenderLk.Lock()
		require.Equal(t, nKeys, len(addProviderRpcs))
		for k, holders := range addProviderRpcs {
			// Verify that all keys have been provided to exactly replicationFactor
			// distinct peers.
			require.Len(t, holders, replicationFactor, key.BitString(keyspace.MhToBit256([]byte(k))))
			for _, count := range holders {
				require.Equal(t, 1, count)
			}
			// Verify provider records are assigned to the closest peers
			closestPeers := kb.SortClosestPeers(peers, kb.ConvertKey(k))[:replicationFactor]
			for _, p := range closestPeers {
				require.Contains(t, holders, p)
			}
		}

		// Test reprovides again, clear addProviderRpcs
		clear(addProviderRpcs)
		msgSenderLk.Unlock()
		for range reprovideInterval / step {
			time.Sleep(step)
		}
		synctest.Wait()
		require.Equal(t, 3*int32(len(mhs)*replicationFactor), provideCount.Load(), "should have reprovided all keys at least twice")

		msgSenderLk.Lock()
		require.Equal(t, nKeys, len(addProviderRpcs))
		for k, holders := range addProviderRpcs {
			// Verify that all keys have been provided to exactly replicationFactor
			// distinct peers.
			require.Len(t, holders, replicationFactor)
			for _, count := range holders {
				require.Equal(t, 1, count)
			}
			// Verify provider records are assigned to the closest peers
			closestPeers := kb.SortClosestPeers(peers, kb.ConvertKey(k))[:replicationFactor]
			for _, p := range closestPeers {
				require.Contains(t, holders, p)
			}
		}
		msgSenderLk.Unlock()
	})
}

func TestStartProvidingUnstableNetwork(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pid, err := peer.Decode("12BoooooPEER")
		require.NoError(t, err)

		nKeysExponent := 10
		nKeys := 1 << nKeysExponent
		mhs := genBalancedMultihashes(nKeysExponent)

		replicationFactor := 4
		peerPrefixBitlen := 6
		require.LessOrEqual(t, peerPrefixBitlen, bitsPerByte)
		var nPeers byte = 1 << peerPrefixBitlen // 2**peerPrefixBitlen
		peers := make([]peer.ID, nPeers)
		for i := range nPeers {
			b := i << (bitsPerByte - peerPrefixBitlen)
			k := [32]byte{b}
			peers[i], err = kb.GenRandPeerIDWithCPL(k[:], uint(peerPrefixBitlen))
			require.NoError(t, err)
		}

		reprovideInterval := time.Hour
		connectivityCheckInterval := time.Minute
		offlineDelay := time.Hour

		routerOffline := atomic.Bool{}
		router := &mockRouter{
			getClosestPeersFunc: func(ctx context.Context, k string) ([]peer.ID, error) {
				if routerOffline.Load() {
					return nil, errors.New("offline")
				}
				sortedPeers := kb.SortClosestPeers(peers, kb.ConvertKey(k))
				return sortedPeers[:min(replicationFactor, len(peers))], nil
			},
		}
		msgSenderLk := sync.Mutex{}
		addProviderRpcs := make(map[string]map[peer.ID]int) // key -> peerid -> count
		provideCount := atomic.Int32{}
		msgSender := &mockMsgSender{
			sendMessageFunc: func(ctx context.Context, p peer.ID, m *pb.Message) error {
				msgSenderLk.Lock()
				defer msgSenderLk.Unlock()
				if routerOffline.Load() {
					return errors.New("offline")
				}
				_, k, err := mh.MHFromBytes(m.GetKey())
				require.NoError(t, err)
				if _, ok := addProviderRpcs[string(k)]; !ok {
					addProviderRpcs[string(k)] = make(map[peer.ID]int)
				}
				addProviderRpcs[string(k)][p]++
				provideCount.Add(1)
				return nil
			},
		}
		opts := []Option{
			WithReprovideInterval(reprovideInterval),
			WithReplicationFactor(replicationFactor),
			WithMaxWorkers(1),
			WithDedicatedBurstWorkers(0),
			WithDedicatedPeriodicWorkers(0),
			WithPeerID(pid),
			WithRouter(router),
			WithMessageSender(msgSender),
			WithSelfAddrs(getMockAddrs(t)),
			WithOfflineDelay(offlineDelay),
			WithConnectivityCheckOnlineInterval(connectivityCheckInterval),
		}
		prov, err := New(opts...)
		require.NoError(t, err)
		defer prov.Close()

		synctest.Wait()
		prov.avgPrefixLenLk.Lock()
		require.GreaterOrEqual(t, prov.cachedAvgPrefixLen, 0)
		prov.avgPrefixLenLk.Unlock()

		routerOffline.Store(true)
		time.Sleep(connectivityCheckInterval) // wait for connectivity check to become available again
		synctest.Wait()

		err = prov.StartProviding(true, mhs...)
		require.NoError(t, err)
		synctest.Wait()
		require.Equal(t, int32(0), provideCount.Load(), "should not have provided when disconnected")
		require.False(t, prov.connectivity.IsOnline())

		routerOffline.Store(false)
		time.Sleep(connectivityCheckInterval) // connectivity check triggered
		synctest.Wait()
		require.True(t, prov.connectivity.IsOnline())

		msgSenderLk.Lock()
		require.Equal(t, nKeys, len(addProviderRpcs))
		for _, peers := range addProviderRpcs {
			// Verify that all keys have been provided to exactly replicationFactor
			// distinct peers.
			require.Len(t, peers, replicationFactor)
		}
		msgSenderLk.Unlock()
	})
}

func TestAddToSchedule(t *testing.T) {
	prov := SweepingProvider{
		reprovideInterval: time.Hour,
		schedule:          trie.New[bitstr.Key, time.Duration](),
		scheduleTimer:     time.NewTimer(time.Hour),

		cachedAvgPrefixLen:   4,
		avgPrefixLenValidity: time.Minute,
		lastAvgPrefixLen:     time.Now(),
	}

	ok, _ := trie.Find(prov.schedule, "0000")

	require.False(t, ok)
	keys := genMultihashesMatchingPrefix("0000", 4)
	prov.AddToSchedule(keys...)
	ok, _ = trie.Find(prov.schedule, "0000")
	require.True(t, ok)
	require.Equal(t, 1, prov.schedule.Size())

	// Nothing should have changed
	prov.AddToSchedule(keys...)
	ok, _ = trie.Find(prov.schedule, "0000")
	require.True(t, ok)
	require.Equal(t, 1, prov.schedule.Size())

	keys = append(keys, append(genMultihashesMatchingPrefix("0111", 1), genMultihashesMatchingPrefix("1000", 3)...)...)
	prov.AddToSchedule(keys...)
	require.Equal(t, 3, prov.schedule.Size())
	ok, _ = trie.Find(prov.schedule, "0000")
	require.True(t, ok)
	ok, _ = trie.Find(prov.schedule, "0111")
	require.True(t, ok)
	ok, _ = trie.Find(prov.schedule, "1000")
	require.True(t, ok)
}

func TestRefreshSchedule(t *testing.T) {
	ctx := context.Background()
	mapDs := datastore.NewMapDatastore()
	defer mapDs.Close()
	ks, err := keystore.NewKeystore(mapDs)
	require.NoError(t, err)

	prov := SweepingProvider{
		ctx:      ctx,
		keystore: ks,

		reprovideInterval: time.Hour,
		schedule:          trie.New[bitstr.Key, time.Duration](),
		scheduleTimer:     time.NewTimer(time.Hour),

		cachedAvgPrefixLen:   4,
		avgPrefixLenValidity: time.Minute,
		lastAvgPrefixLen:     time.Now(),
	}

	// Schedule is empty
	require.Equal(t, 0, prov.schedule.Size())
	prov.RefreshSchedule()
	require.Equal(t, 0, prov.schedule.Size())

	// Add key to keystore
	k := genMultihashesMatchingPrefix("00000", 1)[0]
	ks.Put(ctx, k)

	// Refresh schedule should add the key to the schedule
	require.Equal(t, 0, prov.schedule.Size())
	prov.RefreshSchedule()
	require.Equal(t, 1, prov.schedule.Size())
	ok, _ := trie.Find(prov.schedule, bitstr.Key("0000"))
	require.True(t, ok)

	// Add another key starting with same prefix to keystore
	k = genMultihashesMatchingPrefix("00001", 1)[0]
	ks.Put(ctx, k)
	prov.RefreshSchedule()
	require.Equal(t, 1, prov.schedule.Size())
	ok, _ = trie.Find(prov.schedule, bitstr.Key("0000"))
	require.True(t, ok)

	// Add multiple keys and verify associated prefixes are scheduled.
	newPrefixes := []bitstr.Key{"0100", "0110", "0111"}
	keys := make([]mh.Multihash, 0, len(newPrefixes))
	for _, p := range newPrefixes {
		keys = append(keys, genMultihashesMatchingPrefix(p, 1)...)
	}
	ks.Put(ctx, keys...)
	prov.RefreshSchedule()
	// Assert that only the prefixes containing matching keys in the KeyStore
	// have been added to the schedule.
	require.Equal(t, 1+len(newPrefixes), prov.schedule.Size())
	for _, p := range newPrefixes {
		ok, _ = trie.Find(prov.schedule, p)
		require.True(t, ok)
	}
}

func TestOperationsOffline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pid, err := peer.Decode("12BoooooPEER")
		require.NoError(t, err)

		checkInterval := time.Second
		offlineDelay := time.Minute

		online := atomic.Bool{} // false, start offline

		router := &mockRouter{
			getClosestPeersFunc: func(ctx context.Context, k string) ([]peer.ID, error) {
				if online.Load() {
					return []peer.ID{pid}, nil
				}
				return nil, errors.New("offline")
			},
		}

		provideCount := atomic.Int32{}
		msgSender := &mockMsgSender{
			sendMessageFunc: func(ctx context.Context, p peer.ID, m *pb.Message) error {
				provideCount.Add(1)
				return nil
			},
		}

		opts := []Option{
			WithReprovideInterval(time.Hour),
			WithReplicationFactor(1),
			WithMaxWorkers(1),
			WithDedicatedBurstWorkers(0),
			WithDedicatedPeriodicWorkers(0),
			WithPeerID(pid),
			WithRouter(router),
			WithMessageSender(msgSender),
			WithSelfAddrs(getMockAddrs(t)),
			WithOfflineDelay(offlineDelay),
			WithConnectivityCheckOnlineInterval(checkInterval),
		}
		prov, err := New(opts...)
		require.NoError(t, err)
		defer prov.Close()

		k := random.Multihashes(1)[0]

		// Not bootstrapped yet, OFFLINE
		err = prov.ProvideOnce(k)
		require.NoError(t, err, "ProvideOnce should not error when offline")
		require.Equal(t, int32(0), provideCount.Load(), "no provides should happen when offline")
		err = prov.StartProviding(false, k)
		require.NoError(t, err, "StartProviding should not error when offline")
		require.Equal(t, int32(0), provideCount.Load(), "no provides should happen when offline")
		err = prov.StartProviding(true, k)
		require.NoError(t, err, "StartProviding should not error when offline")
		require.Equal(t, int32(0), provideCount.Load(), "no provides should happen when offline")
		err = prov.RefreshSchedule()
		require.NoError(t, err)
		err = prov.AddToSchedule(k)
		require.NoError(t, err)
		err = prov.StopProviding(k) // no error for StopProviding
		require.NoError(t, err)

		online.Store(true)
		time.Sleep(checkInterval) // trigger connectivity check
		synctest.Wait()
		require.True(t, prov.connectivity.IsOnline())

		// ONLINE, operations shouldn't error and provides should happen
		provideCount.Store(0) // reset counter
		err = prov.ProvideOnce(k)
		require.NoError(t, err)
		synctest.Wait()
		require.Greater(t, provideCount.Load(), int32(0), "provides should happen when online")
		err = prov.StartProviding(false, k)
		require.NoError(t, err)
		err = prov.StartProviding(true, k)
		require.NoError(t, err)
		err = prov.RefreshSchedule()
		require.NoError(t, err)
		err = prov.AddToSchedule(k)
		require.NoError(t, err)
		err = prov.StopProviding(k) // no error for StopProviding
		require.NoError(t, err)

		online.Store(false)
		time.Sleep(checkInterval) // wait for connectivity check to finish
		prov.connectivity.TriggerCheck()
		synctest.Wait()
		require.False(t, prov.connectivity.IsOnline())

		// DISCONNECTED, operations shouldn't error until node is OFFLINE
		provideCount.Store(0) // reset counter
		err = prov.ProvideOnce(k)
		require.NoError(t, err)
		err = prov.StartProviding(false, k)
		require.NoError(t, err)
		err = prov.StartProviding(true, k)
		require.NoError(t, err)
		err = prov.RefreshSchedule()
		require.NoError(t, err)
		err = prov.AddToSchedule(k)
		require.NoError(t, err)
		err = prov.StopProviding(k) // no error for StopProviding
		require.NoError(t, err)
		// Still disconnected, so no provides should have happened
		require.Equal(t, int32(0), provideCount.Load(), "no provides should happen when disconnected")

		prov.provideQueue.Enqueue("0000", k)
		require.Equal(t, 1, prov.provideQueue.Size())
		time.Sleep(offlineDelay)
		synctest.Wait()

		// OFFLINE
		// Verify that provide queue has been emptied by the onOffline callback
		require.True(t, prov.provideQueue.IsEmpty())
		prov.avgPrefixLenLk.Lock()
		require.Equal(t, -1, prov.cachedAvgPrefixLen)
		prov.avgPrefixLenLk.Unlock()

		// Operations should not error but no provides should happen
		provideCount.Store(0) // reset counter
		err = prov.ProvideOnce(k)
		require.NoError(t, err, "ProvideOnce should not error when offline")
		require.Equal(t, int32(0), provideCount.Load(), "no provides should happen when offline")
		err = prov.StartProviding(false, k)
		require.NoError(t, err, "StartProviding should not error when offline")
		require.Equal(t, int32(0), provideCount.Load(), "no provides should happen when offline")
		err = prov.StartProviding(true, k)
		require.NoError(t, err, "StartProviding should not error when offline")
		require.Equal(t, int32(0), provideCount.Load(), "no provides should happen when offline")
		err = prov.RefreshSchedule()
		require.NoError(t, err)
		err = prov.AddToSchedule(k)
		require.NoError(t, err)
		err = prov.StopProviding(k) // no error for StopProviding
		require.NoError(t, err)
	})
}

// TestClosestPeersToPrefixErrors tests error handling in closestPeersToPrefix
func TestClosestPeersToPrefixErrors(t *testing.T) {
	t.Run("OfflineNode", func(t *testing.T) {
		// Test that closestPeersToPrefix returns error when node is offline
		callCount := atomic.Int32{}
		router := &mockRouter{
			getClosestPeersFunc: func(ctx context.Context, k string) ([]peer.ID, error) {
				callCount.Add(1)
				return []peer.ID{"peer1"}, nil
			},
		}

		// Create a connectivity checker that reports offline
		offlineChecker, err := connectivity.New(func() bool { return false })
		require.NoError(t, err)

		prov := &SweepingProvider{
			ctx:               context.Background(),
			router:            router,
			order:             bit256.ZeroKey(),
			replicationFactor: amino.DefaultBucketSize,
			connectivity:      offlineChecker,
		}

		prefix := bitstr.Key("0101")
		_, _, err = prov.closestPeersToPrefix(prefix)

		require.Error(t, err)
		require.Equal(t, int32(0), callCount.Load(), "router should not be called when offline")
	})

	t.Run("NetworkError", func(t *testing.T) {
		// Test error propagation from GetClosestPeers
		synctest.Test(t, func(t *testing.T) {
			networkErr := errors.New("network timeout")
			router := &mockRouter{
				getClosestPeersFunc: func(ctx context.Context, k string) ([]peer.ID, error) {
					return nil, networkErr
				},
			}

			prov := &SweepingProvider{
				ctx:               context.Background(),
				connectivity:      noopConnectivityChecker(),
				router:            router,
				order:             bit256.ZeroKey(),
				replicationFactor: amino.DefaultBucketSize,
			}
			prov.connectivity.Start()
			defer prov.connectivity.Close()

			synctest.Wait()
			require.True(t, prov.connectivity.IsOnline())

			prefix := bitstr.Key("0101")
			_, _, err := prov.closestPeersToPrefix(prefix)

			require.Error(t, err)
			require.Contains(t, err.Error(), "network timeout")
		})
	})
}

// TestQueuePersistence tests that the provide queue is persisted when the
// provider is closed and loaded again when it restarts.
func TestQueuePersistence(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pid, err := peer.Decode("12BoooooPEER")
		require.NoError(t, err)

		// Create a datastore that will be shared across provider instances
		mapDs := dssync.MutexWrap(datastore.NewMapDatastore())
		defer mapDs.Close()

		router := &mockRouter{
			getClosestPeersFunc: func(ctx context.Context, k string) ([]peer.ID, error) {
				return []peer.ID{pid}, nil
			},
		}
		msgSender := &mockMsgSender{}

		checkInterval := time.Second

		// Create the first provider instance with NO workers
		// This ensures keys stay in the queue and won't be processed
		opts0 := []Option{
			WithPeerID(pid),
			WithRouter(router),
			WithMessageSender(&mockMsgSender{}),
			WithDatastore(mapDs),
			WithSelfAddrs(getMockAddrs(t)),
			WithConnectivityCheckOnlineInterval(checkInterval),
			WithMaxWorkers(0),               // No workers = keys stay in queue
			WithDedicatedBurstWorkers(0),    // No dedicated burst workers
			WithDedicatedPeriodicWorkers(0), // No dedicated periodic workers
		}
		prov0, err := New(opts0...)
		require.NoError(t, err)

		synctest.Wait()
		require.True(t, prov0.connectivity.IsOnline())

		// Skip the avg prefix length estimation
		prov0.avgPrefixLenLk.Lock()
		prov0.cachedAvgPrefixLen = 4
		prov0.avgPrefixLenLk.Unlock()

		// Generate test keys
		mhs := random.Multihashes(8)

		// Provide keys - they will be queued but not processed (no workers)
		err = prov0.ProvideOnce(mhs...)
		require.NoError(t, err)
		synctest.Wait()

		// Verify the queue is not empty before closing
		queueSizeBefore := prov0.provideQueue.Size()
		require.Equal(t, len(mhs), queueSizeBefore, "queue size should match number of provided keys")

		// Close the first provider - this should persist the queue
		err = prov0.Close()
		require.NoError(t, err)

		// Create a second provider instance with the same datastore
		opts1 := []Option{
			WithPeerID(pid),
			WithRouter(router),
			WithMessageSender(msgSender),
			WithDatastore(mapDs),
			WithSelfAddrs(getMockAddrs(t)),
			WithConnectivityCheckOnlineInterval(checkInterval),
			WithMaxWorkers(0),               // Still no workers to keep queue stable
			WithDedicatedBurstWorkers(0),    // No dedicated burst workers
			WithDedicatedPeriodicWorkers(0), // No dedicated periodic workers
		}
		prov1, err := New(opts1...)
		require.NoError(t, err)
		defer prov1.Close()

		synctest.Wait()
		require.True(t, prov1.connectivity.IsOnline())

		// Verify the queue was loaded from the datastore
		queueSizeAfter := prov1.provideQueue.Size()
		require.Equal(t, queueSizeBefore, queueSizeAfter, "queue size should match after restart")
		require.False(t, prov1.provideQueue.IsEmpty(), "queue should not be empty after restart")

		// Verify the keys are the same by dequeuing them
		var dequeuedKeys []mh.Multihash
		for !prov1.provideQueue.IsEmpty() {
			_, keys, ok := prov1.provideQueue.Dequeue()
			require.True(t, ok)
			dequeuedKeys = append(dequeuedKeys, keys...)
		}
		require.ElementsMatch(t, mhs, dequeuedKeys, "dequeued keys should match original keys")
	})
}

// TestWithHostOption tests that the WithHost option properly sets the host
// and extracts the peer ID when not provided separately
func TestWithHostOption(t *testing.T) {
	t.Run("WithHostNil", func(t *testing.T) {
		router := &mockRouter{
			getClosestPeersFunc: func(ctx context.Context, k string) ([]peer.ID, error) {
				return []peer.ID{}, nil
			},
		}

		msgSender := &mockMsgSender{}

		// WithHost(nil) should error
		opts := []Option{
			WithHost(nil),
			WithRouter(router),
			WithMessageSender(msgSender),
			WithSelfAddrs(func() []ma.Multiaddr {
				addr, err := ma.NewMultiaddr("/ip4/127.0.0.1/tcp/4001")
				require.NoError(t, err)
				return []ma.Multiaddr{addr}
			}),
		}

		_, err := New(opts...)
		require.Error(t, err)
		require.Contains(t, err.Error(), "host cannot be nil")
	})

	t.Run("WithHostAndNoPeerID", func(t *testing.T) {
		// Create a real libp2p host
		h, err := bhost.NewHost(swarmt.GenSwarm(t, swarmt.OptDisableReuseport), new(bhost.HostOpts))
		require.NoError(t, err)
		defer h.Close()

		router := &mockRouter{
			getClosestPeersFunc: func(ctx context.Context, k string) ([]peer.ID, error) {
				return []peer.ID{}, nil
			},
		}

		msgSender := &mockMsgSender{}

		// Use WithHost without WithPeerID
		opts := []Option{
			WithHost(h), // Don't set peer ID separately
			WithRouter(router),
			WithMessageSender(msgSender),
			WithSelfAddrs(func() []ma.Multiaddr {
				addr, err := ma.NewMultiaddr("/ip4/127.0.0.1/tcp/4001")
				require.NoError(t, err)
				return []ma.Multiaddr{addr}
			}),
		}

		prov, err := New(opts...)
		require.NoError(t, err)
		defer prov.Close()

		// Verify peer ID was extracted from host
		require.Equal(t, h.ID(), prov.peerid, "peer ID should be extracted from host")
		require.Equal(t, h, prov.host, "host should be set")
	})
}

func TestSkipBootstrapReprovide(t *testing.T) {
	testCases := []struct {
		name                   string
		skipBootstrapReprovide *bool // nil means option not set
		expectBootstrapProvide bool
	}{
		{
			name:                   "Default (option not set)",
			skipBootstrapReprovide: nil,
			expectBootstrapProvide: true,
		},
		{
			name:                   "Explicitly set to false",
			skipBootstrapReprovide: func() *bool { b := false; return &b }(),
			expectBootstrapProvide: true,
		},
		{
			name:                   "Set to true",
			skipBootstrapReprovide: func() *bool { b := true; return &b }(),
			expectBootstrapProvide: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx := context.Background()
				pid, err := peer.Decode("12D3KooWCPQTeFYCDkru8nza3Af6u77aoVLA71Vb74eHxeR91Gka")
				require.NoError(t, err)

				replicationFactor := 2
				peerPrefixBitlen := 2
				require.LessOrEqual(t, peerPrefixBitlen, bitsPerByte)
				var nPeers byte = 1 << peerPrefixBitlen
				peers := make([]peer.ID, nPeers)
				for i := range nPeers {
					b := i << (bitsPerByte - peerPrefixBitlen)
					k := [32]byte{b}
					peers[i], err = kb.GenRandPeerIDWithCPL(k[:], uint(peerPrefixBitlen))
					require.NoError(t, err)
				}

				reprovideInterval := time.Hour

				router := &mockRouter{
					getClosestPeersFunc: func(ctx context.Context, k string) ([]peer.ID, error) {
						sortedPeers := kb.SortClosestPeers(peers, kb.ConvertKey(k))
						return sortedPeers[:min(replicationFactor, len(peers))], nil
					},
				}

				provideCount := atomic.Int32{}
				msgSender := &mockMsgSender{
					sendMessageFunc: func(ctx context.Context, p peer.ID, m *pb.Message) error {
						provideCount.Add(1)
						return nil
					},
				}

				ds := dssync.MutexWrap(datastore.NewMapDatastore())
				ks, err := keystore.NewKeystore(ds)
				require.NoError(t, err)
				defer ks.Close()

				// Add keys to keystore before starting provider
				mhs := random.Multihashes(1)
				ks.Put(ctx, mhs...)

				provDs := namespace.Wrap(ds, datastore.NewKey("provider"))

				// Build options
				opts := []Option{
					WithReprovideInterval(reprovideInterval),
					WithReplicationFactor(replicationFactor),
					WithMaxWorkers(1),
					WithDedicatedBurstWorkers(0),
					WithDedicatedPeriodicWorkers(0),
					WithPeerID(pid),
					WithRouter(router),
					WithMessageSender(msgSender),
					WithSelfAddrs(getMockAddrs(t)),
					WithDatastore(provDs),
					WithKeystore(ks),
				}

				// Add the skip option if specified in test case
				if tc.skipBootstrapReprovide != nil {
					opts = append(opts, WithSkipBootstrapReprovide(*tc.skipBootstrapReprovide))
				}

				prov, err := New(opts...)
				require.NoError(t, err)
				defer prov.Close()

				synctest.Wait()
				require.True(t, prov.connectivity.IsOnline())

				// Verify schedule was created
				prov.scheduleLk.Lock()
				nRegions := prov.schedule.Size()
				prov.scheduleLk.Unlock()
				require.Greater(t, nRegions, 0, "schedule should be populated")

				expectedProvides := int32(len(mhs) * replicationFactor)

				if tc.expectBootstrapProvide {
					// Verify that bootstrap reprovide happened
					require.Equal(t, expectedProvides, provideCount.Load(),
						"all keys should have been provided during bootstrap")
				} else {
					// Verify that bootstrap reprovide did NOT happen
					require.Equal(t, int32(0), provideCount.Load(),
						"no provides should occur when skipBootstrapReprovide is true")

					// Verify that the schedule timer was initialized
					prov.scheduleLk.Lock()
					require.False(t, prov.scheduleTimerStartedAt.IsZero(),
						"schedule timer should have been initialized")
					require.NotEqual(t, bitstr.Key(""), prov.scheduleCursor,
						"schedule cursor should have been set")
					require.Greater(t, prov.schedule.Size(), 0, "schedule should have regions")
					prov.scheduleLk.Unlock()

					t.Log(prov.schedule.Size())

					// When skipping bootstrap reprovide, the periodic reprovide cycle
					// starts with regions distributed across the interval. Unlike the
					// default behavior (which enqueues all regions immediately on
					// bootstrap), regions are reprovided as their scheduled times arrive
					// throughout the cycle.
					time.Sleep(reprovideInterval)
					synctest.Wait()

					// All keys should have been reprovided at least once
					require.Equal(t, provideCount.Load(), expectedProvides,
						"all keys should be reprovided after sufficient time when skipping bootstrap")
				}
			})
		})
	}
}

// resumeFixture holds the setup shared by the resume tests. The provider is
// created by the caller so each test controls New and Close across restarts;
// the message sender and helpers always act on the current instance through
// the prov pointer passed to newResumeFixture.
type resumeFixture struct {
	reprovidedLk               *sync.Mutex
	reprovided                 map[bitstr.Key]struct{}
	requireNoPendingReprovides func()
	provDs                     datastore.Batching
	baseOpts                   []Option
}

func newResumeFixture(t *testing.T, reprovideInterval time.Duration, prov **SweepingProvider) resumeFixture {
	pid, err := peer.Decode("12D3KooWCPQTeFYCDkru8nza3Af6u77aoVLA71Vb74eHxeR91Gka") // kadid starts with 16*"0"
	require.NoError(t, err)

	replicationFactor := 4
	peerPrefixBitlen := 6
	require.LessOrEqual(t, peerPrefixBitlen, bitsPerByte)
	var nPeers byte = 1 << peerPrefixBitlen // 2**peerPrefixBitlen
	peers := make([]peer.ID, nPeers)
	for i := range nPeers {
		b := i << (bitsPerByte - peerPrefixBitlen)
		k := [32]byte{b}
		peers[i], err = kb.GenRandPeerIDWithCPL(k[:], uint(peerPrefixBitlen))
		require.NoError(t, err)
	}

	router := &mockRouter{
		getClosestPeersFunc: func(ctx context.Context, k string) ([]peer.ID, error) {
			sortedPeers := kb.SortClosestPeers(peers, kb.ConvertKey(k))
			return sortedPeers[:min(replicationFactor, len(peers))], nil
		},
	}

	getPrefixFromSchedule := func(k bit256.Key) bitstr.Key {
		(*prov).activeReprovidesLk.Lock()
		defer (*prov).activeReprovidesLk.Unlock()
		prefix, ok := keyspace.FindPrefixOfKey((*prov).activeReprovides, k)
		require.True(t, ok)
		return prefix
	}

	reprovidedLk := &sync.Mutex{}
	reprovided := make(map[bitstr.Key]struct{})
	msgSender := &mockMsgSender{
		sendMessageFunc: func(ctx context.Context, p peer.ID, m *pb.Message) error {
			h, err := mh.Cast(m.GetKey())
			require.NoError(t, err)
			k := keyspace.MhToBit256(h)
			regionalPrefix := getPrefixFromSchedule(k)

			reprovidedLk.Lock()
			reprovided[regionalPrefix] = struct{}{}
			reprovidedLk.Unlock()
			return nil
		},
	}

	requireNoPendingReprovides := func() {
		(*prov).activeReprovidesLk.Lock()
		require.Zero(t, (*prov).activeReprovides.Size())
		(*prov).activeReprovidesLk.Unlock()
		require.Zero(t, (*prov).reprovideQueue.Size())
	}

	ds := dssync.MutexWrap(datastore.NewMapDatastore())
	ks, err := keystore.NewKeystore(ds)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, ks.Close()) })
	provDs := namespace.Wrap(ds, datastore.NewKey("provider"))

	mhs := genBalancedMultihashes(10)
	_, err = ks.Put(t.Context(), mhs...)
	require.NoError(t, err)

	baseOpts := []Option{
		WithReprovideInterval(reprovideInterval),
		WithReplicationFactor(replicationFactor),
		WithMaxWorkers(1),
		WithDedicatedBurstWorkers(0),
		WithDedicatedPeriodicWorkers(0),
		WithPeerID(pid),
		WithRouter(router),
		WithMessageSender(msgSender),
		WithSelfAddrs(getMockAddrs(t)),
		WithDatastore(provDs),
		WithKeystore(ks),
	}

	return resumeFixture{
		reprovidedLk:               reprovidedLk,
		reprovided:                 reprovided,
		requireNoPendingReprovides: requireNoPendingReprovides,
		provDs:                     provDs,
		baseOpts:                   baseOpts,
	}
}

func TestResumeReprovides(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		reprovideInterval := time.Hour

		var prov *SweepingProvider
		fx := newResumeFixture(t, reprovideInterval, &prov)
		reprovided, reprovidedLk := fx.reprovided, fx.reprovidedLk
		requireNoPendingReprovides := fx.requireNoPendingReprovides
		opts := fx.baseOpts

		prov, err := New(opts...)
		require.NoError(t, err)

		synctest.Wait()
		require.True(t, prov.connectivity.IsOnline())

		prov.scheduleLk.Lock()
		nRegions := prov.schedule.Size()
		prov.scheduleLk.Unlock()
		require.Len(t, reprovided, nRegions, "should have reprovided all regions")
		requireNoPendingReprovides()

		// All keys have been provided once, clear for cycle of reprovides.
		reprovidedLk.Lock()
		clear(reprovided)
		reprovidedLk.Unlock()

		cycleStart := prov.cycleStart
		require.Equal(t, time.Now(), cycleStart)

		time.Sleep(reprovideInterval / 2) // wait half a reprovide cycle
		synctest.Wait()

		reprovidedLk.Lock()
		require.Len(t, reprovided, nRegions/2, "should have reprovided %d regions", nRegions/2)
		reprovidedLk.Unlock()
		requireNoPendingReprovides()

		time.Sleep(2 * reprovideInterval)
		// Provider has run for 2.5*reprovideInterval
		synctest.Wait()
		requireNoPendingReprovides()

		// Close provider
		err = prov.Close()
		require.NoError(t, err)

		time.Sleep(reprovideInterval / 4) // wait some time before restarting
		// 2.75*reprovideInterval since initial start
		synctest.Wait()

		// Clear reprovided regions
		reprovidedLk.Lock()
		clear(reprovided)
		reprovidedLk.Unlock()

		// Restart provider
		prov, err = New(opts...)
		require.NoError(t, err)
		defer prov.Close()
		synctest.Wait()
		require.True(t, prov.connectivity.IsOnline())

		require.Equal(t, prov.cycleStart, cycleStart, "cycle start should be unchanged after restart")

		// Since the node was offline for reprovideInterval/4, the provider needs
		// to reprovide nRegions/4 ASAP. They should be provided by now.
		require.Len(t, reprovided, nRegions/4, "should have reprovided %d regions in second round", nRegions/4)
		requireNoPendingReprovides()

		time.Sleep(reprovideInterval / 4)
		synctest.Wait()

		// 0.25*reprovideInterval after restart.
		// 0.5*reprovideInterval after shutdown.
		// 3*reprovidesInterval since initial start.
		reprovidedLk.Lock()
		require.Len(t, reprovided, nRegions/2)
		reprovidedLk.Unlock()
		requireNoPendingReprovides()

		// reprovideInterval/2 after restart.
		// 0.75*reprovideInterval after shutdown.
		// 3.25*reprovidesInterval since initial start.
		time.Sleep(reprovideInterval / 4)
		synctest.Wait()
		reprovidedLk.Lock()
		require.Len(t, reprovided, 3*nRegions/4)
		reprovidedLk.Unlock()
		requireNoPendingReprovides()

		// 0.75*reprovideInterval after restart.
		// 1*reprovideInterval after shutdown.
		// 3.5*reprovidesInterval since initial start.
		time.Sleep(reprovideInterval / 4)
		synctest.Wait()
		reprovidedLk.Lock()
		require.Len(t, reprovided, nRegions, "should have reprovided all regions")
		reprovidedLk.Unlock()
		requireNoPendingReprovides()

		// All keys have been provided once, clear for cycle of reprovides.
		reprovidedLk.Lock()
		clear(reprovided)
		reprovidedLk.Unlock()

		// 1.75*reprovideInterval after restart.
		// 2*reprovideInterval after shutdown.
		// 4.5*reprovidesInterval since initial start.
		time.Sleep(reprovideInterval)
		synctest.Wait()
		reprovidedLk.Lock()
		require.Len(t, reprovided, nRegions, "should have reprovided all regions")
		reprovidedLk.Unlock()
		requireNoPendingReprovides()
	})
}

// TestResumeDisabledReprovidesAllRegions verifies the documented behavior of
// WithResumeCycle(false): on a fresh start the provider must discard the
// previous reprovide cycle state and reprovide ALL regions matching its keys
// as soon as it comes online, regardless of when those regions were last
// reprovided before the restart.
//
// This is the counterpart of TestResumeReprovides (which covers
// WithResumeCycle(true)). Here the provider runs long enough that every region
// is "recently reprovided" in the persisted history, then restarts with resume
// disabled. With resume disabled, that history must be ignored and every region
// reprovided ASAP.
func TestResumeDisabledReprovidesAllRegions(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		reprovideInterval := time.Hour

		var prov *SweepingProvider
		fx := newResumeFixture(t, reprovideInterval, &prov)
		reprovided, reprovidedLk := fx.reprovided, fx.reprovidedLk
		requireNoPendingReprovides := fx.requireNoPendingReprovides
		baseOpts := fx.baseOpts
		provDs := fx.provDs

		// First run with resume enabled (the default). Let it run long enough
		// that every region is reprovided and recorded in the persisted
		// reprovide history.
		prov, err := New(baseOpts...)
		require.NoError(t, err)

		synctest.Wait()
		require.True(t, prov.connectivity.IsOnline())

		prov.scheduleLk.Lock()
		nRegions := prov.schedule.Size()
		prov.scheduleLk.Unlock()
		require.Greater(t, nRegions, 1, "test needs multiple regions to be meaningful")
		require.Len(t, reprovided, nRegions, "initial provide should cover all regions")
		requireNoPendingReprovides()

		// Run for more than one full cycle so the reprovide history holds a
		// recent timestamp for every region.
		time.Sleep(2 * reprovideInterval)
		synctest.Wait()
		requireNoPendingReprovides()

		err = prov.Close()
		require.NoError(t, err)

		// Short downtime, much less than one reprovide interval. With resume
		// enabled this would reprovide nothing; with resume disabled it must
		// reprovide everything.
		time.Sleep(reprovideInterval / 100)
		synctest.Wait()

		reprovidedLk.Lock()
		clear(reprovided)
		reprovidedLk.Unlock()

		// Restart with resume disabled.
		restartOpts := append([]Option{WithResumeCycle(false)}, baseOpts...)
		prov, err = New(restartOpts...)
		require.NoError(t, err)
		defer prov.Close()

		synctest.Wait()
		require.True(t, prov.connectivity.IsOnline())

		// The reprovide cycle must restart from the beginning.
		require.Equal(t, time.Now(), prov.cycleStart,
			"resume disabled must reset the cycle start to now")

		// And every region must have been reprovided ASAP on startup, even
		// though all of them were reprovided shortly before the restart.
		reprovidedLk.Lock()
		require.Len(t, reprovided, nRegions,
			"resume disabled must reprovide all regions ASAP on restart")
		reprovidedLk.Unlock()
		requireNoPendingReprovides()

		// Clearing the history on a resume-disabled start must not disable
		// periodic GC. Each successful reprovide writes a history entry, and GC
		// prunes entries older than one interval, so the keyspace stays bounded.
		// Run a few more cycles and check it does not keep growing; a GC throttle
		// poisoned with a far-future timestamp would let history accumulate every
		// cycle.
		countHistory := func() int {
			res, err := provDs.Query(t.Context(), query.Query{Prefix: reprovideHistoryKeyPrefix, KeysOnly: true})
			require.NoError(t, err)
			defer res.Close()
			n := 0
			for r := range res.Next() {
				require.NoError(t, r.Error)
				n++
			}
			return n
		}

		time.Sleep(3 * reprovideInterval)
		synctest.Wait()
		require.LessOrEqual(t, countHistory(), 2*nRegions,
			"history GC must keep pruning after a resume-disabled restart")
		requireNoPendingReprovides()
	})
}

func TestConnectivityCallbacks(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// Connectivity state tracked by callbacks
		type ConnectivityState int
		const (
			StateOffline ConnectivityState = iota
			StateDisconnected
			StateOnline
		)

		var stateLk sync.Mutex
		trackedState := StateOffline
		onlineChan := make(chan struct{}, 10)
		disconnectedChan := make(chan struct{}, 10)
		offlineChan := make(chan struct{}, 10)

		// Define callbacks that update shared state
		onOnline := func() {
			stateLk.Lock()
			trackedState = StateOnline
			stateLk.Unlock()
			onlineChan <- struct{}{}
		}

		onDisconnected := func() {
			stateLk.Lock()
			trackedState = StateDisconnected
			stateLk.Unlock()
			disconnectedChan <- struct{}{}
		}

		onOffline := func() {
			stateLk.Lock()
			trackedState = StateOffline
			stateLk.Unlock()
			offlineChan <- struct{}{}
		}

		// Control connectivity through router
		online := atomic.Bool{}
		online.Store(true) // Start online
		checkInterval := time.Second
		offlineDelay := time.Minute

		pid, err := peer.Decode("12BoooooPEER")
		require.NoError(t, err)

		router := &mockRouter{
			getClosestPeersFunc: func(ctx context.Context, k string) ([]peer.ID, error) {
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				if !online.Load() {
					return nil, errors.New("offline")
				}
				return []peer.ID{peer.ID("12BoooooPEER1")}, nil
			},
		}

		prov, err := New(
			WithPeerID(pid),
			WithRouter(router),
			WithMessageSender(&mockMsgSender{}),
			WithSelfAddrs(getMockAddrs(t)),
			WithReplicationFactor(1),
			WithOfflineDelay(offlineDelay),
			WithConnectivityCheckOnlineInterval(checkInterval),
			WithConnectivityCallbacks(onOnline, onDisconnected, onOffline),
		)
		require.NoError(t, err)
		defer prov.Close()

		// Wait for initial ONLINE state
		<-onlineChan
		synctest.Wait()

		stateLk.Lock()
		require.Equal(t, StateOnline, trackedState)
		stateLk.Unlock()
		require.True(t, prov.connectivity.IsOnline())
		s, err := prov.Stats(t.Context())
		require.NoError(t, err)
		require.Equal(t, "online", s.Connectivity.Status)

		// Transition to DISCONNECTED
		time.Sleep(checkInterval)
		online.Store(false)
		prov.connectivity.TriggerCheck()

		<-disconnectedChan
		synctest.Wait()

		stateLk.Lock()
		require.Equal(t, StateDisconnected, trackedState)
		stateLk.Unlock()
		require.False(t, prov.connectivity.IsOnline())
		s, err = prov.Stats(t.Context())
		require.NoError(t, err)
		require.Equal(t, "disconnected", s.Connectivity.Status)

		// Transition to OFFLINE after offlineDelay
		time.Sleep(offlineDelay)

		<-offlineChan
		synctest.Wait()

		stateLk.Lock()
		require.Equal(t, StateOffline, trackedState)
		stateLk.Unlock()
		require.False(t, prov.connectivity.IsOnline())
		s, err = prov.Stats(t.Context())
		require.NoError(t, err)
		require.Equal(t, "offline", s.Connectivity.Status)

		// Transition back to ONLINE
		online.Store(true)

		<-onlineChan
		synctest.Wait()

		stateLk.Lock()
		require.Equal(t, StateOnline, trackedState)
		stateLk.Unlock()
		require.True(t, prov.connectivity.IsOnline())
		s, err = prov.Stats(t.Context())
		require.NoError(t, err)
		require.Equal(t, "online", s.Connectivity.Status)
	})
}

// TestResettableKeystoreWithPersistence tests the provider with ResettableKeystore
// backed by a pebble datastore, including:
// - Slow reset operations with 128 keys
// - Verification of all keys provided
// - Running for 1.5 reprovide cycles
// - Persistence across restarts
// - Resume behavior after downtime
// - No unnecessary reprovides after reset with same keys
func TestResettableKeystoreWithPersistence(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		pid, err := peer.Decode("12BoooooPEER")
		require.NoError(t, err)

		// Configure for 4 regions: low replication factor and peer count
		replicationFactor := 2
		peerPrefixBitlen := 3
		require.LessOrEqual(t, peerPrefixBitlen, bitsPerByte)
		var nPeers byte = 1 << peerPrefixBitlen // 8 peers, replicationFactor 2 -> 4 regions
		nRegions := int(nPeers) / replicationFactor
		peers := make([]peer.ID, nPeers)
		for i := range nPeers {
			b := i << (bitsPerByte - peerPrefixBitlen)
			k := [32]byte{b}
			peers[i], err = kb.GenRandPeerIDWithCPL(k[:], uint(peerPrefixBitlen))
			require.NoError(t, err)
		}

		reprovideInterval := time.Hour

		router := &mockRouter{
			getClosestPeersFunc: func(ctx context.Context, k string) ([]peer.ID, error) {
				sortedPeers := kb.SortClosestPeers(peers, kb.ConvertKey(k))
				return sortedPeers[:min(replicationFactor, len(peers))], nil
			},
		}

		// Track provides
		msgSenderLk := sync.Mutex{}
		provideCount := atomic.Int32{}
		addProviderRpcs := make(map[string]int) // key -> count
		msgSender := &mockMsgSender{
			sendMessageFunc: func(ctx context.Context, p peer.ID, m *pb.Message) error {
				msgSenderLk.Lock()
				defer msgSenderLk.Unlock()
				_, k, err := mh.MHFromBytes(m.GetKey())
				require.NoError(t, err)
				addProviderRpcs[string(k)]++
				provideCount.Add(1)
				return nil
			},
		}

		// Create pebble datastore in temp directory
		tempDir := t.TempDir()
		pebbleDs, err := pebble.NewDatastore(tempDir, nil)
		require.NoError(t, err)

		keystoreDs := namespace.Wrap(pebbleDs, datastore.NewKey("provider/keystore"))
		ks, err := keystore.NewResettableKeystore(keystoreDs)
		require.NoError(t, err)

		provDs := namespace.Wrap(pebbleDs, datastore.NewKey("provider"))

		// Provider options
		opts := []Option{
			WithReprovideInterval(reprovideInterval),
			WithReplicationFactor(replicationFactor),
			WithMaxWorkers(1),
			WithDedicatedBurstWorkers(0),
			WithDedicatedPeriodicWorkers(0),
			WithPeerID(pid),
			WithRouter(router),
			WithMessageSender(msgSender),
			WithSelfAddrs(getMockAddrs(t)),
			WithSkipBootstrapReprovide(false),
		}

		prov, err := New(append(opts, WithDatastore(provDs), WithKeystore(ks))...)
		require.NoError(t, err)

		synctest.Wait()
		require.True(t, prov.connectivity.IsOnline())

		require.Zero(t, provideCount.Load(), "no provides should have happened yet")

		// Generate 128 keys balanced across the keyspace
		const keysExponent = 7
		firstKeys := genBalancedMultihashes(keysExponent)
		nKeys := len(firstKeys)

		// Use Reset() to update keys with slow writes
		keysChan := make(chan cid.Cid, nKeys)
		go func() {
			for i, h := range firstKeys {
				keysChan <- cid.NewCidV1(cid.Raw, h)
				// Make breaks while writing to channel to simulate slow Reset()
				if i%10 == 0 {
					time.Sleep(time.Minute)
				}
			}
			close(keysChan)
		}()

		// Reset keystore with keys
		err = ks.ResetCids(ctx, keysChan)
		require.NoError(t, err)
		// Add regions to schedule and reprovide queue
		err = prov.RefreshSchedule()
		require.NoError(t, err)

		prov.scheduleLk.Lock()
		require.Equal(t, nRegions, prov.schedule.Size(), "schedule should have 4 regions")
		prov.scheduleLk.Unlock()
		synctest.Wait()

		// Assert all keys have been provided
		initialProvideCount := provideCount.Load()
		require.Equal(t, int32(nKeys*replicationFactor), initialProvideCount,
			"all keys should be provided initially")
		msgSenderLk.Lock()
		require.Equal(t, nKeys, len(addProviderRpcs), "all unique keys should be provided")
		for _, count := range addProviderRpcs {
			require.Equal(t, replicationFactor, count, "each key should be provided to replicationFactor peers")
		}
		msgSenderLk.Unlock()

		// Let provider run for 1.5 reprovide cycles
		time.Sleep(reprovideInterval + reprovideInterval/2)
		synctest.Wait()

		// Verify keys are reprovided according to schedule.
		// We should have 2.5x the initial count, 1x initial reprovide, 1x
		// reprovide cycle, 0.5x half cycle
		expectedProvides := int32(nKeys * replicationFactor * 5 / 2)
		require.Equal(t, expectedProvides, provideCount.Load(), "keys should be reprovided after 1.5 cycles")

		// Get keystore size before closing
		sizeBefore, err := ks.Size(ctx)
		require.NoError(t, err)
		require.Equal(t, nKeys, sizeBefore, "keystore should have %d keys", nKeys)

		// Close provider, close keystore, close pebble datastore
		err = prov.Close()
		require.NoError(t, err)
		err = ks.Close()
		require.NoError(t, err)
		err = pebbleDs.Close()
		require.NoError(t, err)

		// Sleep for a while (synctest)
		downtimeFractionOfCycle := 4                                           // offline for reprovideInterval / 4
		time.Sleep(reprovideInterval / time.Duration(downtimeFractionOfCycle)) // Simulate downtime
		synctest.Wait()

		// Create new pebble datastore (same path), new ResettableKeystore, new SweepingProvider
		pebbleDs2, err := pebble.NewDatastore(tempDir, nil)
		require.NoError(t, err)
		defer pebbleDs2.Close()
		keystoreDs2 := namespace.Wrap(pebbleDs2, datastore.NewKey("provider/keystore"))
		ks2, err := keystore.NewResettableKeystore(keystoreDs2)
		require.NoError(t, err)
		defer ks2.Close()

		// Keys should be persisted automatically by pebble
		sizeAfterReopen, err := ks2.Size(ctx)
		require.NoError(t, err)
		require.Equal(t, sizeBefore, sizeAfterReopen)

		provDs2 := namespace.Wrap(pebbleDs2, datastore.NewKey("provider"))

		// Reset provide tracking
		provideCount.Store(0)

		prov2, err := New(append(opts, WithDatastore(provDs2), WithKeystore(ks2))...)
		require.NoError(t, err)
		defer prov2.Close()

		synctest.Wait()
		require.True(t, prov2.connectivity.IsOnline())
		prov2.scheduleLk.Lock()
		require.Equal(t, nRegions, prov2.schedule.Size(), "all %d regions should be scheduled on restart", nRegions)
		prov2.scheduleLk.Unlock()

		// Don't call reset yet, test that size matches
		sizeAfter, err := ks2.Size(ctx)
		require.NoError(t, err)
		require.Equal(t, sizeBefore, sizeAfter,
			"keystore size should match after reopening (fix verification)")

		// Verify that the regions that should have been reprovided while offline
		// are reprovided by now
		expectedResumeProvides := int32(nKeys * replicationFactor / downtimeFractionOfCycle)
		providesBeforeReset := provideCount.Load()
		// All keys should be provided on restart (bootstrap reprovide)
		require.Equal(t, expectedResumeProvides, providesBeforeReset,
			"missed reprovides should happen on restart")

		// Call Reset() with same keys
		sameKeysChan := make(chan cid.Cid, nKeys)
		for _, h := range firstKeys {
			sameKeysChan <- cid.NewCidV1(cid.Raw, h)
		}
		close(sameKeysChan)

		err = ks2.ResetCids(ctx, sameKeysChan)
		require.NoError(t, err)
		err = prov2.RefreshSchedule()
		require.NoError(t, err)
		synctest.Wait()

		// Assert that Reset with same keys does NOT trigger more reprovides
		require.Equal(t, providesBeforeReset, provideCount.Load())
		require.Zero(t, prov2.reprovideQueue.Size())
	})
}
