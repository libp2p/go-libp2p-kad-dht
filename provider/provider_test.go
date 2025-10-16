//go:build go1.25
// +build go1.25

package provider

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/guillaumemichel/reservedpool"
	"github.com/ipfs/go-datastore"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipfs/go-test/random"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	mh "github.com/multiformats/go-multihash"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/probe-lab/go-libdht/kad/key"
	"github.com/probe-lab/go-libdht/kad/key/bit256"
	"github.com/probe-lab/go-libdht/kad/key/bitstr"
	"github.com/probe-lab/go-libdht/kad/trie"

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
	mhs := make([]mh.Multihash, 0, n)
	for i := 0; len(mhs) < n; i++ {
		h := random.Multihashes(1)[0]
		k := keyspace.MhToBit256(h)
		if keyspace.IsPrefix(prefix, k) {
			mhs = append(mhs, h)
		}
	}
	return mhs
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
		msgSender: msgSender,
	}

	nKeys := 16
	pid, err := peer.Decode("12BoooooPEER")
	require.NoError(t, err)
	mhs := genMultihashes(nKeys)
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

	for _, c := range mhs {
		k := sha256.Sum256(c)
		closestPeers := kb.SortClosestPeers(peers, k[:])[:replicationFactor]
		for _, p := range closestPeers[:replicationFactor] {
			require.Contains(t, keysAllocations[p], c)
		}
		for _, p := range closestPeers[replicationFactor:] {
			require.NotContains(t, keysAllocations[p], c)
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
		}
		r.connectivity.Start()
		defer r.connectivity.Close()

		synctest.Wait()
		require.True(t, r.connectivity.IsOnline())

		for _, prefix := range []bitstr.Key{"", "0", "1", "00", "01", "10", "11", "000", "001", "010", "011", "100", "101", "110", "111"} {
			closestPeers, err := r.closestPeersToPrefix(prefix)
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

	prefixes, err := prov.groupAndScheduleKeysByPrefix(mhs, false)
	require.NoError(t, err)
	require.Len(t, prefixes, 2)
	require.Contains(t, prefixes, bitstr.Key("000"))
	require.Len(t, prefixes["000"], 3) // no duplicate entry
	require.Contains(t, prefixes, bitstr.Key("010"))
	require.Len(t, prefixes["010"], 2)

	// Schedule is still empty
	require.True(t, prov.schedule.IsEmptyLeaf())

	prefixes, err = prov.groupAndScheduleKeysByPrefix(mhs, true)
	require.NoError(t, err)
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
	prefixes, err = prov.groupAndScheduleKeysByPrefix(mhs, true)
	require.NoError(t, err)
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
	prefixes, err = prov.groupAndScheduleKeysByPrefix(mhs1, true)
	require.NoError(t, err)
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
		"total_provide_count",
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
	logging.SetLogLevel(LoggerName, "warn")

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
		router:                   router,
		msgSender:                msgSender,
		reprovideInterval:        time.Hour,
		maxProvideConnsPerWorker: 2,
		provideQueue:             queue.NewProvideQueue(),
		reprovideQueue:           queue.NewReprovideQueue(),
		connectivity:             noopConnectivityChecker(),
		schedule:                 trie.New[bitstr.Key, time.Duration](),
		scheduleTimer:            time.NewTimer(time.Hour),
		getSelfAddrs:             func() []ma.Multiaddr { return nil },
		addLocalRecord:           func(mh mh.Multihash) error { return nil },
		provideCounter:           provideCounter(),
	}

	assertAdvertisementCount := func(n int) {
		msgSenderLk.Lock()
		defer msgSenderLk.Unlock()
		for _, count := range advertisements {
			require.Equal(t, n, count)
		}
	}

	// Providing no keys returns no error
	r.individualProvide(prefix, nil, false, false)
	require.True(t, noWarningsNorAbove(obsLogs))
	assertAdvertisementCount(0)

	// Providing a single key - success
	r.individualProvide(prefix, mhs, false, false)
	require.True(t, noWarningsNorAbove(obsLogs))
	assertAdvertisementCount(1)

	// Providing a single key - failure
	gcpErr := errors.New("GetClosestPeers error")
	router.getClosestPeersFunc = func(ctx context.Context, k string) ([]peer.ID, error) {
		return nil, gcpErr
	}
	r.individualProvide(prefix, mhs, false, false)
	require.True(t, takeAllContainsErr(obsLogs, gcpErr.Error()))
	assertAdvertisementCount(1)
	// Verify failed key ends up in the provide queue.
	_, keys, ok := r.provideQueue.Dequeue()
	require.True(t, ok)
	require.Equal(t, mhs, keys)

	// Reproviding a single key - failure
	r.individualProvide(prefix, mhs, true, true)
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
	logging.SetLogLevel(LoggerName, "warn")

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
		router:                   router,
		msgSender:                msgSender,
		reprovideInterval:        reprovideInterval,
		maxReprovideDelay:        maxDelay,
		maxProvideConnsPerWorker: 2,
		provideQueue:             queue.NewProvideQueue(),
		reprovideQueue:           queue.NewReprovideQueue(),
		connectivity:             noopConnectivityChecker(),
		schedule:                 trie.New[bitstr.Key, time.Duration](),
		scheduleTimer:            time.NewTimer(time.Hour),
		getSelfAddrs:             func() []ma.Multiaddr { return nil },
		addLocalRecord:           func(mh mh.Multihash) error { return nil },
		provideCounter:           provideCounter(),
		stats:                    newOperationStats(reprovideInterval, maxDelay),
		datastore:                ds,
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
	r.individualProvide(prefix, ks, false, false)
	require.True(t, noWarningsNorAbove(obsLogs))
	assertAdvertisementCount(1)

	// Providing two keys - failure
	gcpErr := errors.New("GetClosestPeers error")
	router.getClosestPeersFunc = func(ctx context.Context, k string) ([]peer.ID, error) {
		return nil, gcpErr
	}
	r.individualProvide(prefix, ks, false, false)
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
	r.individualProvide(prefix, ks, true, true)
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

	r.individualProvide(prefix, ks, false, false)
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

	r.individualProvide(prefix, ks, true, true)
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
		require.Equal(t, 0, prov.Clear())

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

		// Node is offline, ProvideOne should error
		err = prov.ProvideOnce(h)
		require.ErrorIs(t, err, ErrOffline)
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
		kbKey := keyspace.KeyToBytes(keyspace.PeerIDToBit256(peers[0]))
		for i := range peers[1:] {
			p, err := kb.GenRandPeerIDWithCPL(kbKey, uint(prefixLen))
			require.NoError(t, err)
			if _, ok := seen[p]; ok {
				continue
			}
			seen[p] = struct{}{}
			peers[i+1] = p
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
		prefix := bitstr.Key(key.BitString(keyspace.MhToBit256(h))[:prefixLen])
		prov.scheduleLk.Lock()
		require.Equal(t, 1, prov.schedule.Size())
		found, reprovideTime := trie.Find(prov.schedule, prefix)
		if !found {
			t.Log(prefix)
			t.Log(keyspace.AllEntries(prov.schedule, prov.order)[0].Key)
			require.FailNow(t, "prefix not inserted in schedule")
		}
		require.Equal(t, prov.reprovideTimeForPrefix(prefix), reprovideTime)
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
		opts := []Option{
			WithReprovideInterval(time.Hour),
			WithReplicationFactor(1),
			WithMaxWorkers(1),
			WithDedicatedBurstWorkers(0),
			WithDedicatedPeriodicWorkers(0),
			WithPeerID(pid),
			WithRouter(router),
			WithMessageSender(&mockMsgSender{}),
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
		require.ErrorIs(t, err, ErrOffline)
		err = prov.StartProviding(false, k)
		require.ErrorIs(t, err, ErrOffline)
		err = prov.StartProviding(true, k)
		require.ErrorIs(t, err, ErrOffline)
		err = prov.RefreshSchedule()
		require.ErrorIs(t, err, ErrOffline)
		err = prov.AddToSchedule(k)
		require.ErrorIs(t, err, ErrOffline)
		err = prov.StopProviding(k) // no error for StopProviding
		require.NoError(t, err)

		online.Store(true)
		time.Sleep(checkInterval) // trigger connectivity check
		synctest.Wait()
		require.True(t, prov.connectivity.IsOnline())

		// ONLINE, operations shouldn't error
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

		online.Store(false)
		time.Sleep(checkInterval) // wait for connectivity check to finish
		prov.connectivity.TriggerCheck()
		synctest.Wait()
		require.False(t, prov.connectivity.IsOnline())

		// DISCONNECTED, operations shoudln't error until node is OFFLINE
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

		// All operations should error again
		err = prov.ProvideOnce(k)
		require.ErrorIs(t, err, ErrOffline)
		err = prov.StartProviding(false, k)
		require.ErrorIs(t, err, ErrOffline)
		err = prov.StartProviding(true, k)
		require.ErrorIs(t, err, ErrOffline)
		err = prov.RefreshSchedule()
		require.ErrorIs(t, err, ErrOffline)
		err = prov.AddToSchedule(k)
		require.ErrorIs(t, err, ErrOffline)
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
		_, err = prov.closestPeersToPrefix(prefix)

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
			_, err := prov.closestPeersToPrefix(prefix)

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
		mapDs := datastore.NewMapDatastore()
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

func TestResumeReprovides(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
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

		reprovideInterval := time.Hour

		router := &mockRouter{
			getClosestPeersFunc: func(ctx context.Context, k string) ([]peer.ID, error) {
				sortedPeers := kb.SortClosestPeers(peers, kb.ConvertKey(k))
				return sortedPeers[:min(replicationFactor, len(peers))], nil
			},
		}

		var prov *SweepingProvider
		getPrefixFromSchedule := func(k bit256.Key) bitstr.Key {
			prov.activeReprovidesLk.Lock()
			defer prov.activeReprovidesLk.Unlock()
			prefix, ok := keyspace.FindPrefixOfKey(prov.activeReprovides, k)
			require.True(t, ok)
			return prefix
		}

		reprovidedLk := sync.Mutex{}
		reprovidedTotal := make(map[bitstr.Key]struct{})
		reprovidedRound2 := make(map[bitstr.Key]struct{})
		secondRound := false
		msgSender := &mockMsgSender{
			sendMessageFunc: func(ctx context.Context, p peer.ID, m *pb.Message) error {
				h, err := mh.Cast(m.GetKey())
				require.NoError(t, err)
				k := keyspace.MhToBit256(h)
				regionalPrefix := getPrefixFromSchedule(k)

				reprovidedLk.Lock()
				reprovidedTotal[regionalPrefix] = struct{}{}
				if secondRound {
					reprovidedRound2[regionalPrefix] = struct{}{}
				}
				reprovidedLk.Unlock()
				return nil
			},
		}

		ds := datastore.NewMapDatastore()
		ks, err := keystore.NewKeystore(ds)
		require.NoError(t, err)
		defer ks.Close()

		mhs := genBalancedMultihashes(10)
		ks.Put(ctx, mhs...)

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
			WithDatastore(ds),
			WithKeystore(ks),
		}
		prov, err = New(opts...)
		require.NoError(t, err)

		synctest.Wait()
		require.True(t, prov.connectivity.IsOnline())

		cycleStart := prov.cycleStart
		require.Equal(t, time.Now(), cycleStart)

		time.Sleep(reprovideInterval / 2) // wait half a reprovide cycle
		synctest.Wait()

		prov.scheduleLk.Lock()
		nRegions := prov.schedule.Size()
		prov.scheduleLk.Unlock()

		reprovidedLk.Lock()
		require.Len(t, reprovidedTotal, nRegions/2, "should have reprovided %d regions", nRegions/2)
		require.Empty(t, reprovidedRound2)
		reprovidedLk.Unlock()

		// Close provider
		err = prov.Close()
		require.NoError(t, err)

		// Restart provider
		secondRound = true
		prov, err = New(opts...)
		require.NoError(t, err)
		defer prov.Close()
		synctest.Wait()
		require.True(t, prov.connectivity.IsOnline())

		require.Equal(t, prov.cycleStart, cycleStart, "cycle start should be unchanged after restart")

		time.Sleep(reprovideInterval / 4)
		synctest.Wait()
		reprovidedLk.Lock()
		require.Len(t, reprovidedRound2, nRegions/4, "should have reprovided %d regions in second round", nRegions/4)
		require.Len(t, reprovidedTotal, 3*nRegions/4, "should have reprovided %d regions", 3*nRegions/4)
		reprovidedLk.Unlock()

		time.Sleep(reprovideInterval / 4)
		synctest.Wait()
		reprovidedLk.Lock()
		require.Len(t, reprovidedRound2, nRegions/2, "should have reprovided %d regions in second round", nRegions/2)
		require.Len(t, reprovidedTotal, nRegions, "should have reprovided all regions")
		reprovidedLk.Unlock()

		time.Sleep(reprovideInterval / 2)
		synctest.Wait()
		reprovidedLk.Lock()
		require.Len(t, reprovidedRound2, nRegions, "should have reprovided all regions in second round")
		require.Len(t, reprovidedTotal, nRegions, "should have reprovided all regions")
		reprovidedLk.Unlock()
	})
}
