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
	"github.com/ipfs/go-cid"
	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
	"github.com/libp2p/go-libp2p-kad-dht/provider/internal/connectivity"
	"github.com/libp2p/go-libp2p-kad-dht/provider/internal/helpers"
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

func TestKeyToBytes(t *testing.T) {
	require.Equal(t, []byte{0b00000000}, helpers.KeyToBytes(bitstr.Key("0")))
	require.Equal(t, []byte{0b00000000}, helpers.KeyToBytes(bitstr.Key("00000000")))
	require.Equal(t, []byte{0b00000000, 0b00000000}, helpers.KeyToBytes(bitstr.Key("000000000")))
	require.Equal(t, []byte{0b00110000}, helpers.KeyToBytes(bitstr.Key("0011")))
	require.Equal(t, []byte{0b11111110}, helpers.KeyToBytes(bitstr.Key("1111111")))
}

func genCids(n int) []cid.Cid {
	cids := make([]cid.Cid, n)
	for i := range n {
		h, err := mh.Sum([]byte(strconv.Itoa(i)), mh.SHA2_256, -1)
		if err != nil {
			panic(err)
		}
		c := cid.NewCidV1(cid.Raw, h)
		cids[i] = c
	}
	return cids
}

func genBalancedCids(exponent int) []cid.Cid {
	cids := make(map[bitstr.Key]cid.Cid)
	for i := 0; len(cids) < (1 << exponent); i++ {
		h, err := mh.Sum([]byte(strconv.Itoa(i)), mh.SHA2_256, -1)
		if err != nil {
			panic(err)
		}
		prefix := bitstr.Key(key.BitString(helpers.MhToBit256(h))[:exponent])
		if _, ok := cids[prefix]; !ok {
			cids[prefix] = cid.NewCidV1(cid.Raw, h)
		}
	}
	out := make([]cid.Cid, 0, len(cids))
	for _, c := range cids {
		out = append(out, c)
	}
	return out
}

func cidsToMhs(cids []cid.Cid) []mh.Multihash {
	mhs := make([]mh.Multihash, len(cids))
	for i, c := range cids {
		mhs[i] = c.Hash()
	}
	return mhs
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

func TestIndividualProvideForPrefixSingle(t *testing.T) {
	ctx := context.Background()
	cids := genCids(1)
	k := cids[0].Hash()
	prefix := bitstr.Key("1011101111")
	router := &mockRouter{
		getClosestPeersFunc: func(ctx context.Context, k string) ([]peer.ID, error) {
			return nil, nil
		},
	}
	mockClock := clock.NewMock()
	r := SweepingProvider{
		router:            router,
		clock:             mockClock,
		reprovideInterval: time.Hour,
		pendingKeysChan:   make(chan []mh.Multihash, 1),
		failedRegionsChan: make(chan bitstr.Key, 1),
		schedule:          trie.New[bitstr.Key, time.Duration](),
		scheduleTimer:     mockClock.Timer(time.Hour),
		getSelfAddrs:      func() []ma.Multiaddr { return nil },
		addLocalRecord:    func(mh mh.Multihash) error { return nil },
	}

	// Providing no cids returns no error
	err := r.individualProvideForPrefix(ctx, prefix, nil, false, false)
	require.NoError(t, err)

	// Providing a single cid - success
	err = r.individualProvideForPrefix(ctx, prefix, []mh.Multihash{k}, false, false)
	require.NoError(t, err)

	// Providing a single cid - failure
	router.getClosestPeersFunc = func(ctx context.Context, k string) ([]peer.ID, error) {
		return nil, errors.New("GetClosestPeers error")
	}
	err = r.individualProvideForPrefix(ctx, prefix, []mh.Multihash{k}, false, false)
	require.Error(t, err)
	require.Equal(t, []mh.Multihash{k}, <-r.pendingKeysChan)

	err = r.individualProvideForPrefix(ctx, prefix, []mh.Multihash{k}, true, true)
	require.Error(t, err)
	require.Equal(t, prefix, <-r.failedRegionsChan)
}

func TestIndividualProvideForPrefixMultiple(t *testing.T) {
	ctx := context.Background()
	cids := genCids(2)
	ks := make([]mh.Multihash, len(cids))
	for i := range ks {
		ks[i] = cids[i].Hash()
	}
	prefix := bitstr.Key("10111011")
	router := &mockRouter{
		getClosestPeersFunc: func(ctx context.Context, k string) ([]peer.ID, error) {
			return nil, nil
		},
	}
	mockClock := clock.NewMock()
	r := SweepingProvider{
		router:            router,
		clock:             mockClock,
		reprovideInterval: time.Hour,
		pendingKeysChan:   make(chan []mh.Multihash, len(ks)),
		failedRegionsChan: make(chan bitstr.Key, 1),
		schedule:          trie.New[bitstr.Key, time.Duration](),
		scheduleTimer:     mockClock.Timer(time.Hour),
		getSelfAddrs:      func() []ma.Multiaddr { return nil },
		addLocalRecord:    func(mh mh.Multihash) error { return nil },
	}

	// Providing two cids - 2 successes
	err := r.individualProvideForPrefix(ctx, prefix, ks, false, false)
	require.NoError(t, err)

	// Providing two cids - 2 failures
	router.getClosestPeersFunc = func(ctx context.Context, k string) ([]peer.ID, error) {
		return nil, errors.New("GetClosestPeers error")
	}
	err = r.individualProvideForPrefix(ctx, prefix, ks, false, false)
	require.Error(t, err)
	pendingCids := append(<-r.pendingKeysChan, <-r.pendingKeysChan...)
	require.Len(t, pendingCids, len(ks))
	require.Contains(t, pendingCids, ks[0])
	require.Contains(t, pendingCids, ks[1])

	err = r.individualProvideForPrefix(ctx, prefix, ks, true, true)
	require.Error(t, err)
	require.Equal(t, prefix, <-r.failedRegionsChan)

	// Providing two cids - 1 success, 1 failure
	lk := sync.Mutex{}
	counter := 0
	router.getClosestPeersFunc = func(ctx context.Context, k string) (peers []peer.ID, err error) {
		lk.Lock()
		defer lk.Unlock()
		if counter%2 == 0 {
			err = errors.New("GetClosestPeers error")
		}
		counter++
		return
	}

	err = r.individualProvideForPrefix(ctx, prefix, ks, false, false)
	require.NoError(t, err)
	require.Len(t, r.pendingKeysChan, 1)
	pendingCids = <-r.pendingKeysChan
	require.Len(t, pendingCids, 1)
	require.Contains(t, ks, pendingCids[0])

	err = r.individualProvideForPrefix(ctx, prefix, ks, true, true)
	require.NoError(t, err)
	require.Len(t, r.failedRegionsChan, 0)
	require.Len(t, r.pendingKeysChan, 0)
}

func genRandPeerID(t *testing.T) peer.ID {
	_, pub, err := crypto.GenerateKeyPair(crypto.Ed25519, -1)
	require.NoError(t, err)
	pid, err := peer.IDFromPublicKey(pub)
	require.NoError(t, err)
	return pid
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

	connChecker, err := connectivity.New(func(ctx context.Context) bool { return true }, func() {})
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
			subtrie, ok := helpers.SubtrieMatchingPrefix(peersTrie, currPrefix)
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

func TestCidsAllocationsToPeers(t *testing.T) {
	nCids := 1024
	nPeers := 128
	replicationFactor := 10

	cids := cidsToMhs(genCids(nCids))
	cidsTrie := trie.New[bit256.Key, mh.Multihash]()
	for _, c := range cids {
		cidsTrie.Add(helpers.MhToBit256(c), c)
	}
	peers := make([]peer.ID, nPeers)
	peersTrie := trie.New[bit256.Key, peer.ID]()
	for i := range peers {
		peers[i] = genRandPeerID(t)
		peersTrie.Add(helpers.PeerIDToBit256(peers[i]), peers[i])
	}
	cidsAllocations := helpers.AllocateToKClosest(cidsTrie, peersTrie, replicationFactor)

	for _, c := range cids {
		k := sha256.Sum256(c)
		closestPeers := kb.SortClosestPeers(peers, k[:])[:replicationFactor]
		for _, p := range closestPeers[:replicationFactor] {
			require.Contains(t, cidsAllocations[p], c)
		}
		for _, p := range closestPeers[replicationFactor:] {
			require.NotContains(t, cidsAllocations[p], c)
		}
	}
}

func TestProvideCidsToPeer(t *testing.T) {
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

	nCids := 16
	pid, err := peer.Decode("12BoooooPEER")
	require.NoError(t, err)
	mhs := cidsToMhs(genCids(nCids))
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
	require.Equal(t, nCids, msgCount)
}

func TestProvideNoBootstrap(t *testing.T) {
	ctx := context.Background()
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
	msgSender := &mockMsgSender{}
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
	reprovider, err := NewProvider(ctx, opts...)
	require.NoError(t, err)
	defer reprovider.Close()
	reprovider.cachedAvgPrefixLen = 4

	c := genCids(1)[0]

	// Set the reprovider as offline
	online.Store(false)
	reprovider.connectivity.TriggerCheck(ctx)
	time.Sleep(5 * time.Millisecond) // wait for connectivity check to finish
	err = reprovider.ProvideOnce(ctx, c.Hash())
	require.ErrorIs(t, ErrNodeOffline, err)

	// Set the reprovider as online, but don't bootstrap it
	online.Store(true)
	clk.Add(checkInterval)           // trigger connectivity check
	time.Sleep(5 * time.Millisecond) // wait for connectivity check to finish
	err = reprovider.ProvideOnce(ctx, c.Hash())
	require.NoError(t, err)
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

func TestProvideSingle(t *testing.T) {
	ctx := context.Background()
	pid, err := peer.Decode("12BoooooPEER")
	require.NoError(t, err)
	replicationFactor := 4
	c := genCids(1)[0]

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
	msgSender := &mockMsgSender{}
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
	reprovider, err := NewProvider(ctx, opts...)
	require.NoError(t, err)
	defer reprovider.Close()

	// Blocks until cid is provided
	err = reprovider.ForceStartProviding(ctx, c.Hash())
	require.NoError(t, err)
	require.Equal(t, 1+initialGetClosestPeers, int(getClosestPeersCount.Load()))

	// Verify reprovide is scheduled.
	prefix := bitstr.Key(key.BitString(helpers.MhToBit256(c.Hash()))[:prefixLen])
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

	// Try to provide the same cid again -> noop
	reprovider.StartProviding(c.Hash())
	time.Sleep(5 * time.Millisecond)
	require.Equal(t, 1+initialGetClosestPeers, int(getClosestPeersCount.Load()))

	// Verify reprovide happens as scheduled.
	mockClock.Add(reprovideTime - 1)
	require.Equal(t, 1+initialGetClosestPeers, int(getClosestPeersCount.Load()))
	mockClock.Add(1)
	require.Equal(t, 2+initialGetClosestPeers, int(getClosestPeersCount.Load()))
	mockClock.Add(reprovideInterval - 1)
	require.Equal(t, 2+initialGetClosestPeers, int(getClosestPeersCount.Load()))
	mockClock.Add(reprovideInterval) // 1
	require.Equal(t, 3+initialGetClosestPeers, int(getClosestPeersCount.Load()))
}

func TestProvideMany(t *testing.T) {
	ctx := context.Background()

	pid, err := peer.Decode("12BoooooPEER")
	require.NoError(t, err)

	nCidsExponent := 10
	nCids := 1 << nCidsExponent
	cids := genBalancedCids(nCidsExponent)
	mhs := cidsToMhs(cids)

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
	addProviderRpcs := make(map[string][]peer.ID) // cid -> peerid
	msgSender := &mockMsgSender{
		sendMessageFunc: func(ctx context.Context, p peer.ID, m *pb.Message) error {
			msgSenderLk.Lock()
			defer msgSenderLk.Unlock()
			_, k, err := mh.MHFromBytes(m.GetKey())
			require.NoError(t, err)
			if _, ok := addProviderRpcs[string(k)]; !ok {
				addProviderRpcs[string(k)] = []peer.ID{p}
			} else {
				addProviderRpcs[string(k)] = append(addProviderRpcs[string(k)], p)
			}
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
	reprovider, err := NewProvider(ctx, opts...)
	require.NoError(t, err)
	defer reprovider.Close()
	mockClock.Add(reprovideInterval - 1)

	err = reprovider.ForceStartProviding(ctx, mhs...)
	require.NoError(t, err)
	time.Sleep(20 * time.Millisecond) // wait for ProvideMany to finish

	// Each cid should have been provided at least once.
	msgSenderLk.Lock()
	require.Equal(t, nCids, len(addProviderRpcs))
	for k, holders := range addProviderRpcs {
		// Verify that all cids have been provided to exactly replicationFactor
		// distinct peers.
		require.Len(t, holders, replicationFactor)
		// Verify provider records are assigned to the closest peers
		closestPeers := kb.SortClosestPeers(peers, kb.ConvertKey(k))[:replicationFactor]
		for _, p := range closestPeers {
			require.Contains(t, holders, p)
		}
	}

	step := 10 * time.Second
	// Test reprovides
	clear(addProviderRpcs)
	msgSenderLk.Unlock()
	for range reprovideInterval / step {
		mockClock.Add(step)
	}
	time.Sleep(20 * time.Millisecond) // wait for reprovide to finish

	msgSenderLk.Lock()
	require.Equal(t, nCids, len(addProviderRpcs))
	for k, holders := range addProviderRpcs {
		// Verify that all cids have been provided to exactly replicationFactor
		// distinct peers.
		require.Len(t, holders, replicationFactor, key.BitString(helpers.MhToBit256([]byte(k))))
		// Verify provider records are assigned to the closest peers
		closestPeers := kb.SortClosestPeers(peers, kb.ConvertKey(k))[:replicationFactor]
		for _, p := range closestPeers {
			require.Contains(t, holders, p)
		}
	}

	step = time.Minute // speed up test since prefixes have been consolidated in schedule
	// Test reprovides again
	clear(addProviderRpcs)
	msgSenderLk.Unlock()
	for range reprovideInterval / step {
		mockClock.Add(step)
	}
	time.Sleep(20 * time.Millisecond) // wait for reprovide to finish

	msgSenderLk.Lock()
	require.Equal(t, nCids, len(addProviderRpcs))
	for k, holders := range addProviderRpcs {
		// Verify that all cids have been provided to exactly replicationFactor
		// distinct peers.
		require.Len(t, holders, replicationFactor)
		// Verify provider records are assigned to the closest peers
		closestPeers := kb.SortClosestPeers(peers, kb.ConvertKey(k))[:replicationFactor]
		for _, p := range closestPeers {
			require.Contains(t, holders, p)
		}
	}
	msgSenderLk.Unlock()
}

func TestProvideManyUnstableNetwork(t *testing.T) {
	ctx := context.Background()
	pid, err := peer.Decode("12BoooooPEER")
	require.NoError(t, err)

	nCidsExponent := 10
	nCids := 1 << nCidsExponent
	cids := genBalancedCids(nCidsExponent)
	mhs := cidsToMhs(cids)

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
	addProviderRpcs := make(map[string][]peer.ID) // cid -> peerid
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
				addProviderRpcs[string(k)] = []peer.ID{p}
			} else {
				addProviderRpcs[string(k)] = append(addProviderRpcs[string(k)], p)
			}
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
	reprovider, err := NewProvider(ctx, opts...)
	require.NoError(t, err)
	defer reprovider.Close()
	time.Sleep(10 * time.Millisecond)
	routerOffline.Store(true)

	err = reprovider.ForceStartProviding(ctx, mhs...)
	require.Error(t, err)

	nodeOffline := func() bool {
		return !reprovider.connectivity.IsOnline()
	}
	waitUntil(t, nodeOffline, 10*time.Millisecond, "waiting for node to be offline")
	mockClock.Add(connectivityCheckInterval)

	routerOffline.Store(false)
	mockClock.Add(connectivityCheckInterval)
	waitUntil(t, reprovider.connectivity.IsOnline, 10*time.Millisecond, "waiting for node to come back online")

	providedAllCids := func() bool {
		msgSenderLk.Lock()
		defer msgSenderLk.Unlock()
		if len(addProviderRpcs) != nCids {
			return false
		}
		for _, peers := range addProviderRpcs {
			// Verify that all cids have been provided to exactly replicationFactor
			// distinct peers.
			if len(peers) != replicationFactor {
				return false
			}
		}
		return true
	}
	waitUntil(t, providedAllCids, 200*time.Millisecond, "waiting for all cids to be provided")
}

// TODO: test shrinking/expanding network
