package provider

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/filecoin-project/go-clock"
	"github.com/guillaumemichel/reservedpool"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipfs/go-test/random"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	mh "github.com/multiformats/go-multihash"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/probe-lab/go-libdht/kad/key/bit256"
	"github.com/probe-lab/go-libdht/kad/key/bitstr"
	"github.com/probe-lab/go-libdht/kad/trie"

	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
	"github.com/libp2p/go-libp2p-kad-dht/provider/datastore"
	"github.com/libp2p/go-libp2p-kad-dht/provider/internal/connectivity"
	"github.com/libp2p/go-libp2p-kad-dht/provider/internal/keyspace"
	"github.com/libp2p/go-libp2p-kad-dht/provider/internal/queue"
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
	for _, c := range mhs {
		keysTrie.Add(keyspace.MhToBit256(c), c)
	}
	peers := random.Peers(nPeers)
	peersTrie := trie.New[bit256.Key, peer.ID]()
	for i := range peers {
		peersTrie.Add(keyspace.PeerIDToBit256(peers[i]), peers[i])
	}
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
	replicationFactor := 10
	nPeers := 128
	peers := random.Peers(nPeers)
	peersTrie := trie.New[bit256.Key, peer.ID]()
	for _, p := range peers {
		peersTrie.Add(keyspace.PeerIDToBit256(p), p)
	}

	router := &mockRouter{
		getClosestPeersFunc: func(ctx context.Context, k string) ([]peer.ID, error) {
			sortedPeers := kb.SortClosestPeers(peers, kb.ConvertKey(k))
			return sortedPeers[:min(replicationFactor, len(peers))], nil
		},
	}

	connChecker, err := connectivity.New(func() bool { return true }, func() {})
	require.NoError(t, err)
	defer connChecker.Close()
	r := SweepingProvider{
		router:            router,
		replicationFactor: replicationFactor,
		connectivity:      connChecker,
	}

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
}

func TestGroupAndScheduleKeysByPrefix(t *testing.T) {
	mockClock := clock.NewMock()
	prov := SweepingProvider{
		order:             bit256.ZeroKey(),
		clock:             mockClock,
		reprovideInterval: time.Hour,

		schedule:      trie.New[bitstr.Key, time.Duration](),
		scheduleTimer: mockClock.Timer(time.Hour),

		cachedAvgPrefixLen: 3,
		lastAvgPrefixLen:   mockClock.Now(),
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
	connChecker, err := connectivity.New(func() bool { return true }, func() {})
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
	logging.SetLogLevel(loggerName, "warn")

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
	mockClock := clock.NewMock()
	r := SweepingProvider{
		router:                   router,
		msgSender:                msgSender,
		clock:                    mockClock,
		reprovideInterval:        time.Hour,
		maxProvideConnsPerWorker: 2,
		provideQueue:             queue.NewProvideQueue(),
		reprovideQueue:           queue.NewReprovideQueue(),
		connectivity:             noopConnectivityChecker(),
		schedule:                 trie.New[bitstr.Key, time.Duration](),
		scheduleTimer:            mockClock.Timer(time.Hour),
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
	logging.SetLogLevel(loggerName, "warn")

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
	mockClock := clock.NewMock()
	r := SweepingProvider{
		router:                   router,
		msgSender:                msgSender,
		clock:                    mockClock,
		reprovideInterval:        time.Hour,
		maxProvideConnsPerWorker: 2,
		provideQueue:             queue.NewProvideQueue(),
		reprovideQueue:           queue.NewReprovideQueue(),
		connectivity:             noopConnectivityChecker(),
		schedule:                 trie.New[bitstr.Key, time.Duration](),
		scheduleTimer:            mockClock.Timer(time.Hour),
		getSelfAddrs:             func() []ma.Multiaddr { return nil },
		addLocalRecord:           func(mh mh.Multihash) error { return nil },
		provideCounter:           provideCounter(),
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

func waitUntil(t *testing.T, condition func() bool, maxDelay time.Duration, args ...any) {
	step := time.Millisecond
	for range maxDelay / step {
		if condition() {
			return
		}
		time.Sleep(step)
	}
	t.Fatal(args...)
}

func TestHandleReprovide(t *testing.T) {
	mockClock := clock.NewMock()

	online := atomic.Bool{}
	online.Store(true)
	connectivityCheckInterval := time.Second
	connChecker, err := connectivity.New(
		func() bool { return online.Load() },
		func() {},
		connectivity.WithClock(mockClock),
		connectivity.WithOfflineCheckInterval(connectivityCheckInterval),
		connectivity.WithOnlineCheckInterval(connectivityCheckInterval),
	)
	require.NoError(t, err)
	defer connChecker.Close()

	prov := SweepingProvider{
		order: bit256.ZeroKey(),

		connectivity: connChecker,

		clock:         mockClock,
		cycleStart:    mockClock.Now(),
		scheduleTimer: mockClock.Timer(time.Hour),
		schedule:      trie.New[bitstr.Key, time.Duration](),

		reprovideQueue: queue.NewReprovideQueue(),
		workerPool:     reservedpool.New[workerType](1, nil), // single worker

		reprovideInterval: time.Minute,
		maxReprovideDelay: 5 * time.Second,

		getSelfAddrs: func() []ma.Multiaddr { return nil },
	}
	prov.scheduleTimer.Stop()

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
	mockClock.Add(1)
	prov.schedule.Add(prefixes[1], prov.reprovideTimeForPrefix(prefixes[1]))
	prov.handleReprovide() // reprovides prefixes[0], set scheduleCursor to prefixes[1]
	require.Equal(t, prefixes[1], prov.scheduleCursor)

	// Wait more than reprovideInterval to call handleReprovide again.
	// All prefixes should be added to the reprovide queue.
	mockClock.Add(prov.reprovideInterval + 1)
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
	mockClock.Add(3 * prov.reprovideInterval / 4)
	// reprovides prefixes[1], add prefixes[2] to reprovide queue, set
	// scheduleCursor to prefixes[0]
	prov.handleReprovide()
	require.Equal(t, prefixes[0], prov.scheduleCursor)

	require.Equal(t, 1, prov.reprovideQueue.Size())
	dequeued, ok = prov.reprovideQueue.Dequeue()
	require.True(t, ok)
	require.Equal(t, prefixes[2], dequeued)
	require.True(t, prov.reprovideQueue.IsEmpty())

	mockClock.Add(prov.reprovideInterval / 4)

	// Node goes offline -> prefixes are queued
	online.Store(false)
	prov.connectivity.TriggerCheck()
	waitUntil(t, func() bool { return !prov.connectivity.IsOnline() }, 100*time.Millisecond, "connectivity should be offline")
	// require.True(t, prov.reprovideQueue.IsEmpty())
	prov.handleReprovide()
	// require.Equal(t, 1, prov.reprovideQueue.Size())

	// Node comes back online
	online.Store(true)
	mockClock.Add(connectivityCheckInterval)
	waitUntil(t, func() bool { return prov.connectivity.IsOnline() }, 100*time.Millisecond, "connectivity should be online")
}

func TestClose(t *testing.T) {
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
	mockClock := clock.NewMock()
	prov, err := New(
		WithPeerID(pid),
		WithClock(mockClock),
		WithRouter(router),
		WithMessageSender(msgSender),
		WithReplicationFactor(1),

		WithMaxWorkers(4),
		WithDedicatedBurstWorkers(0),
		WithDedicatedPeriodicWorkers(0),

		WithSelfAddrs(func() []ma.Multiaddr {
			addr, err := ma.NewMultiaddr("/ip4/127.0.0.1/tcp/4001")
			require.NoError(t, err)
			return []ma.Multiaddr{addr}
		}),
	)
	require.NoError(t, err)

	mhs := genMultihashes(128)
	prov.StartProviding(false, mhs...)

	time.Sleep(20 * time.Millisecond)         // leave time to perform provide
	mockClock.Add(prov.reprovideInterval / 2) // some keys should have been reprovided
	time.Sleep(20 * time.Millisecond)         // leave time to perform reprovide

	err = prov.Close()
	require.NoError(t, err)

	newMh := random.Multihashes(1)[0]

	prov.StartProviding(false, newMh)
	prov.StopProviding(newMh)
	prov.ProvideOnce(newMh)
	require.Equal(t, 0, prov.ClearProvideQueue())

	_, err = prov.keyStore.Get(context.Background(), "")
	require.ErrorIs(t, err, datastore.ErrKeyStoreClosed)

	err = prov.workerPool.Acquire(burstWorker)
	require.ErrorIs(t, err, reservedpool.ErrClosed)
}
