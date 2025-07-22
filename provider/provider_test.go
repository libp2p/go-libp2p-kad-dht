package provider

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/filecoin-project/go-clock"
	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
	"github.com/libp2p/go-libp2p-kad-dht/provider/internal/connectivity"
	"github.com/libp2p/go-libp2p-kad-dht/provider/internal/helpers"
	"github.com/libp2p/go-libp2p-kad-dht/provider/internal/queue"
	kb "github.com/libp2p/go-libp2p-kbucket"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	mh "github.com/multiformats/go-multihash"
	"github.com/probe-lab/go-libdht/kad/key"
	"github.com/probe-lab/go-libdht/kad/key/bit256"
	"github.com/probe-lab/go-libdht/kad/key/bitstr"
	"github.com/probe-lab/go-libdht/kad/trie"
	"github.com/stretchr/testify/require"
)

const (
	bitsPerByte = 8
)

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
		prefix := bitstr.Key(key.BitString(helpers.MhToBit256(h))[:exponent])
		if _, ok := seen[prefix]; !ok {
			mhs = append(mhs, h)
			seen[prefix] = struct{}{}
		}
	}
	return mhs
}

func genRandPeerID(t *testing.T) peer.ID {
	_, pub, err := crypto.GenerateKeyPair(crypto.Ed25519, -1)
	require.NoError(t, err)
	pid, err := peer.IDFromPublicKey(pub)
	require.NoError(t, err)
	return pid
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

func noopConnectivityChecker() *connectivity.ConnectivityChecker {
	connChecker, err := connectivity.New(func() bool { return true }, func() {})
	if err != nil {
		panic(err)
	}
	return connChecker
}

func TestClosestPeersToPrefixRandom(t *testing.T) {
	replicationFactor := 10
	nPeers := 128
	peers := make([]peer.ID, nPeers)
	peersTrie := trie.New[bit256.Key, peer.ID]()
	for i := range peers {
		p := genRandPeerID(t)
		peers[i] = p
		peersTrie.Add(helpers.PeerIDToBit256(p), p)
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
			subtrie, ok := helpers.FindSubtrie(peersTrie, currPrefix)
			require.True(t, ok)
			subtrieSize = subtrie.Size()
			if subtrieSize > replicationFactor {
				break
			}
			currPrefix = currPrefix[:len(currPrefix)-1]
		}
		require.Len(t, closestPeers, subtrieSize, "prefix: %s", prefix)
	}
}

func TestKeysAllocationsToPeers(t *testing.T) {
	nKeys := 1024
	nPeers := 128
	replicationFactor := 10

	mhs := genMultihashes(nKeys)
	keysTrie := trie.New[bit256.Key, mh.Multihash]()
	for _, c := range mhs {
		keysTrie.Add(helpers.MhToBit256(c), c)
	}
	peers := make([]peer.ID, nPeers)
	peersTrie := trie.New[bit256.Key, peer.ID]()
	for i := range peers {
		peers[i] = genRandPeerID(t)
		peersTrie.Add(helpers.PeerIDToBit256(peers[i]), peers[i])
	}
	keysAllocations := helpers.AllocateToKClosest(keysTrie, peersTrie, replicationFactor)

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

func TestProvideKeysToPeer(t *testing.T) {
	msgCount := 0
	msgSender := &mockMsgSender{
		sendMessageFunc: func(ctx context.Context, p peer.ID, m *pb.Message) error {
			msgCount++
			return errors.New("error")
		},
	}
	reprovider := SweepingProvider{
		msgSender: msgSender,
	}

	nKeys := 16
	pid, err := peer.Decode("12BoooooPEER")
	require.NoError(t, err)
	mhs := genMultihashes(nKeys)
	pmes := &pb.Message{}

	// All ADD_PROVIDER RPCs fail, return an error after reprovideInitialFailuresAllowed+1 attempts
	err = reprovider.provideKeysToPeer(pid, mhs, pmes)
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
	err = reprovider.provideKeysToPeer(pid, mhs, pmes)
	require.NoError(t, err)
	require.Equal(t, nKeys, msgCount)
}

func TestIndividualProvideForPrefixSingle(t *testing.T) {
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
	}

	assertAdvertisementCount := func(n int) {
		msgSenderLk.Lock()
		defer msgSenderLk.Unlock()
		for _, count := range advertisements {
			require.Equal(t, n, count)
		}
	}

	// Providing no keys returns no error
	err := r.individualProvideForPrefix(prefix, nil, false, false)
	require.NoError(t, err)
	assertAdvertisementCount(0)

	// Providing a single key - success
	err = r.individualProvideForPrefix(prefix, mhs, false, false)
	require.NoError(t, err)
	assertAdvertisementCount(1)

	// Providing a single key - failure
	router.getClosestPeersFunc = func(ctx context.Context, k string) ([]peer.ID, error) {
		return nil, errors.New("GetClosestPeers error")
	}
	err = r.individualProvideForPrefix(prefix, mhs, false, false)
	require.Error(t, err)
	assertAdvertisementCount(1)
	// Verify failed key ends up in the provide queue.
	_, keys, ok := r.provideQueue.Dequeue()
	require.True(t, ok)
	require.Equal(t, mhs, keys)

	// Reproviding a single key - failure
	err = r.individualProvideForPrefix(prefix, mhs, true, true)
	require.Error(t, err)
	assertAdvertisementCount(1)
	// Verify failed prefix ends up in the reprovide queue.
	dequeued, ok := r.reprovideQueue.Dequeue()
	require.True(t, ok)
	require.Equal(t, prefix, dequeued)
}

func TestIndividualProvideForPrefixMultiple(t *testing.T) {
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
	err := r.individualProvideForPrefix(prefix, ks, false, false)
	require.NoError(t, err)
	assertAdvertisementCount(1)

	// Providing two keys - failure
	router.getClosestPeersFunc = func(ctx context.Context, k string) ([]peer.ID, error) {
		return nil, errors.New("GetClosestPeers error")
	}
	err = r.individualProvideForPrefix(prefix, ks, false, false)
	require.Error(t, err)
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
	err = r.individualProvideForPrefix(prefix, ks, true, true)
	require.Error(t, err)
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

	err = r.individualProvideForPrefix(prefix, ks, false, false)
	require.NoError(t, err)
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

	err = r.individualProvideForPrefix(prefix, ks, true, true)
	require.NoError(t, err)
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

func TestProvideOnce(t *testing.T) {
	pid, err := peer.Decode("12BoooooPEER")
	require.NoError(t, err)

	online := atomic.Bool{}
	online.Store(true) // start online
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
	clk := clock.NewMock()

	checkInterval := time.Minute

	opts := []Option{
		WithPeerID(pid),
		WithRouter(router),
		WithMessageSender(msgSender),
		WithSelfAddrs(func() []ma.Multiaddr {
			addr, err := ma.NewMultiaddr("/ip4/127.0.0.1/tcp/4001")
			require.NoError(t, err)
			return []ma.Multiaddr{addr}
		}),
		WithClock(clk),
		WithConnectivityCheckOnlineInterval(checkInterval),
		WithConnectivityCheckOfflineInterval(checkInterval),
	}
	reprovider, err := NewProvider(opts...)
	require.NoError(t, err)
	defer reprovider.Close()
	reprovider.cachedAvgPrefixLen = 4

	h := genMultihashes(1)[0]

	// Set the reprovider as offline
	online.Store(false)
	reprovider.connectivity.TriggerCheck()
	time.Sleep(5 * time.Millisecond) // wait for connectivity check to finish
	reprovider.ProvideOnce(h)
	time.Sleep(5 * time.Millisecond) // wait for ProvideOnce to finish
	require.Equal(t, int32(0), provideCount.Load(), "should not have provided when offline")
	// Ensure the key is in the provide queue
	_, keys, ok := reprovider.provideQueue.Dequeue()
	require.True(t, ok)
	require.Equal(t, 1, len(keys))
	require.Equal(t, h, keys[0])

	// Set the reprovider as online
	online.Store(true)
	clk.Add(checkInterval)           // trigger connectivity check
	time.Sleep(5 * time.Millisecond) // wait for connectivity check to finish
	reprovider.ProvideOnce(h)
	waitUntil(t, func() bool { return provideCount.Load() == 1 }, 50*time.Millisecond, "waiting for ProvideOnce to finish")
}

func TestStartProvidingSingle(t *testing.T) {
	pid, err := peer.Decode("12BoooooPEER")
	require.NoError(t, err)
	replicationFactor := 4
	h := genMultihashes(1)[0]

	mockClock := clock.NewMock()
	reprovideInterval := time.Hour

	prefixLen := 4
	peers := make([]peer.ID, replicationFactor)
	peers[0], err = peer.Decode("12BooooPEER1")
	require.NoError(t, err)
	kbKey := helpers.KeyToBytes(helpers.PeerIDToBit256(peers[0]))
	for i := range peers[1:] {
		peers[i+1], err = kb.GenRandPeerIDWithCPL(kbKey, uint(prefixLen))
		require.NoError(t, err)
	}

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
	opts := []Option{
		WithReplicationFactor(replicationFactor),
		WithReprovideInterval(reprovideInterval),
		WithPeerID(pid),
		WithRouter(router),
		WithMessageSender(msgSender),
		WithSelfAddrs(func() []ma.Multiaddr {
			addr, err := ma.NewMultiaddr("/ip4/127.0.0.1/tcp/4001")
			require.NoError(t, err)
			return []ma.Multiaddr{addr}
		}),
		WithClock(mockClock),
	}
	reprovider, err := NewProvider(opts...)
	require.NoError(t, err)
	defer reprovider.Close()

	// Blocks until key is provided
	reprovider.StartProviding(true, h)
	waitUntil(t, func() bool { return provideCount.Load() == int32(len(peers)) }, 50*time.Millisecond, "waiting for ProvideOnce to finish")
	require.Equal(t, 1+initialGetClosestPeers, int(getClosestPeersCount.Load()))

	// Verify reprovide is scheduled.
	prefix := bitstr.Key(key.BitString(helpers.MhToBit256(h))[:prefixLen])
	reprovider.scheduleLk.Lock()
	require.Equal(t, 1, reprovider.schedule.Size())
	found, reprovideTime := trie.Find(reprovider.schedule, prefix)
	if !found {
		t.Log(prefix)
		t.Log(helpers.AllEntries(reprovider.schedule, reprovider.order)[0].Key)
		t.Fatal("prefix not inserted in schedule")
	}
	require.Equal(t, reprovider.reprovideTimeForPrefix(prefix), reprovideTime)
	reprovider.scheduleLk.Unlock()

	// Try to provide the same key again -> noop
	reprovider.StartProviding(false, h)
	time.Sleep(5 * time.Millisecond)
	require.Equal(t, int32(len(peers)), provideCount.Load())
	require.Equal(t, 1+initialGetClosestPeers, int(getClosestPeersCount.Load()))

	// Verify reprovide happens as scheduled.
	mockClock.Add(reprovideTime - 1)
	require.Equal(t, 1+initialGetClosestPeers, int(getClosestPeersCount.Load()))
	require.Equal(t, int32(len(peers)), provideCount.Load())
	mockClock.Add(1)
	require.Equal(t, 2+initialGetClosestPeers, int(getClosestPeersCount.Load()))
	require.Equal(t, 2*int32(len(peers)), provideCount.Load())
	mockClock.Add(reprovideInterval - 1)
	require.Equal(t, 2+initialGetClosestPeers, int(getClosestPeersCount.Load()))
	require.Equal(t, 2*int32(len(peers)), provideCount.Load())
	mockClock.Add(reprovideInterval) // 1
	require.Equal(t, 3+initialGetClosestPeers, int(getClosestPeersCount.Load()))
	require.Equal(t, 3*int32(len(peers)), provideCount.Load())
}

func TestStartProvidingMany(t *testing.T) {
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

	mockClock := clock.NewMock()
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
		WithSelfAddrs(func() []ma.Multiaddr {
			addr, err := ma.NewMultiaddr("/ip4/127.0.0.1/tcp/4001")
			require.NoError(t, err)
			return []ma.Multiaddr{addr}
		}),
		WithClock(mockClock),
	}
	reprovider, err := NewProvider(opts...)
	require.NoError(t, err)
	defer reprovider.Close()

	reprovider.StartProviding(true, mhs...)
	waitUntil(t, func() bool { return provideCount.Load() == int32(len(mhs)*replicationFactor) }, 100*time.Millisecond, "waiting for ProvideMany to finish")

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
	for range (reprovideInterval - 1) / step {
		mockClock.Add(step)
	}
	waitUntil(t, func() bool { return provideCount.Load() == 2*int32(len(mhs)*replicationFactor) }, 100*time.Millisecond, "waiting for reprovide to finish")

	msgSenderLk.Lock()
	require.Equal(t, nKeys, len(addProviderRpcs))
	for k, holders := range addProviderRpcs {
		// Verify that all keys have been provided to exactly replicationFactor
		// distinct peers.
		require.Len(t, holders, replicationFactor, key.BitString(helpers.MhToBit256([]byte(k))))
		for _, count := range holders {
			require.Equal(t, 1, count)
		}
		// Verify provider records are assigned to the closest peers
		closestPeers := kb.SortClosestPeers(peers, kb.ConvertKey(k))[:replicationFactor]
		for _, p := range closestPeers {
			require.Contains(t, holders, p)
		}
	}

	step = time.Minute // speed up test since prefixes have been consolidated in schedule
	// Test reprovides again, clear addProviderRpcs
	clear(addProviderRpcs)
	msgSenderLk.Unlock()
	for range (reprovideInterval - 1) / step {
		mockClock.Add(step)
	}
	waitUntil(t, func() bool { return provideCount.Load() == 3*int32(len(mhs)*replicationFactor) }, 100*time.Millisecond, "waiting for reprovide to finish")

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
}

func TestStartProvidingUnstableNetwork(t *testing.T) {
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

	mockClock := clock.NewMock()
	reprovideInterval := time.Hour
	connectivityCheckInterval := time.Second

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
		WithSelfAddrs(func() []ma.Multiaddr {
			addr, err := ma.NewMultiaddr("/ip4/127.0.0.1/tcp/4001")
			require.NoError(t, err)
			return []ma.Multiaddr{addr}
		}),
		WithClock(mockClock),
		WithConnectivityCheckOnlineInterval(connectivityCheckInterval),
		WithConnectivityCheckOfflineInterval(connectivityCheckInterval),
	}
	reprovider, err := NewProvider(opts...)
	require.NoError(t, err)
	defer reprovider.Close()
	time.Sleep(10 * time.Millisecond)
	routerOffline.Store(true)

	reprovider.StartProviding(true, mhs...)
	time.Sleep(10 * time.Millisecond) // wait for StartProviding to finish
	require.Equal(t, int32(0), provideCount.Load(), "should not have provided when offline")

	nodeOffline := func() bool {
		return !reprovider.connectivity.IsOnline()
	}
	waitUntil(t, nodeOffline, 10*time.Millisecond, "waiting for node to be offline")
	mockClock.Add(connectivityCheckInterval)

	routerOffline.Store(false)
	mockClock.Add(connectivityCheckInterval)
	waitUntil(t, reprovider.connectivity.IsOnline, 10*time.Millisecond, "waiting for node to come back online")

	providedAllKeys := func() bool {
		msgSenderLk.Lock()
		defer msgSenderLk.Unlock()
		if len(addProviderRpcs) != nKeys {
			return false
		}
		for _, peers := range addProviderRpcs {
			// Verify that all keys have been provided to exactly replicationFactor
			// distinct peers.
			if len(peers) != replicationFactor {
				return false
			}
		}
		return true
	}
	waitUntil(t, providedAllKeys, 200*time.Millisecond, "waiting for all keys to be reprovided")
}

// TODO: test shrinking/expanding network
