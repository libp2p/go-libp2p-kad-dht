package reprovider

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
	s := SweepingReprovider{
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
	s := SweepingReprovider{
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
	require.Equal(t, []byte{0b00000000}, keyToBytes(bitstr.Key("0")))
	require.Equal(t, []byte{0b00000000}, keyToBytes(bitstr.Key("00000000")))
	require.Equal(t, []byte{0b00000000, 0b00000000}, keyToBytes(bitstr.Key("000000000")))
	require.Equal(t, []byte{0b00110000}, keyToBytes(bitstr.Key("0011")))
	require.Equal(t, []byte{0b11111110}, keyToBytes(bitstr.Key("1111111")))
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
		prefix := bitstr.Key(key.BitString(mhToBit256(h))[:exponent])
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

// var _ KadRouter = (*mockRouter)(nil)
//
// type mockRouter struct {
// 	peers *trie.Trie[bit256.Key, peer.ID]
// }
//
// func (r *mockRouter) GetClosestPeers(ctx context.Context, k string) ([]peer.ID, error) {
// 	return allValues(r.peers, bitstr.Key(strings.Repeat("0", 256))), nil
// }
//
// func (r *mockRouter) Provide(ctx context.Context, c cid.Cid, _ bool) error {
// 	return nil
// }

var _ KadRouter = (*mockRouter)(nil)

type mockRouter struct {
	getClosestPeersFunc func(ctx context.Context, k string) ([]peer.ID, error)
	provideFunc         func(ctx context.Context, c cid.Cid, _ bool) error
}

func (r *mockRouter) GetClosestPeers(ctx context.Context, k string) ([]peer.ID, error) {
	if r.getClosestPeersFunc == nil {
		return nil, nil
	}
	return r.getClosestPeersFunc(ctx, k)
}

func (r *mockRouter) Provide(ctx context.Context, c cid.Cid, broadcast bool) error {
	if r.provideFunc == nil {
		return nil
	}
	return r.provideFunc(ctx, c, broadcast)
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

func TestLocalNearstPeersCPL(t *testing.T) {
	selfKey := [32]byte{}
	nPeers := 15
	localPeers := make([]peer.ID, nPeers)
	var err error
	for i := range nPeers {
		// localPeers[i] share a common prefix with selfKey of nPeers-i
		localPeers[i], err = kb.GenRandPeerIDWithCPL(selfKey[:], uint(nPeers-i))
		require.NoError(t, err)
	}

	reprovider := &SweepingReprovider{
		replicationFactor: 0,
		order:             bit256.NewKey(selfKey[:]),
		localNearestPeersToSelf: func(n int) []peer.ID {
			return localPeers[:n]
		},
	}

	// localNearestPeersCPL should return keyLen if replication factor is 0
	require.Equal(t, keyLen, reprovider.localNearestPeersCPL())

	for i := range nPeers {
		reprovider.replicationFactor = i + 1
		cpl := reprovider.localNearestPeersCPL()
		require.Equal(t, nPeers-i, cpl)
	}
}

func TestGetAvgPrefixLenEmptySchedule(t *testing.T) {
	selfKey := [32]byte{}
	targetCpl := 10
	nPeers := 16
	localPeers := make([]peer.ID, nPeers)
	var err error
	for i := range nPeers {
		// localPeers[:nPeers/2] all have cpl of targetCpl
		// localPeers[nPeers/2:] all have cpl of targetCpl+1
		localPeers[i], err = kb.GenRandPeerIDWithCPL(selfKey[:], uint(targetCpl+i/(nPeers/2)))
		require.NoError(t, err)
	}
	reprovider := SweepingReprovider{
		replicationFactor: 20,
		order:             bit256.NewKey(selfKey[:]),
		schedule:          trie.New[bitstr.Key, time.Duration](),
		localNearestPeersToSelf: func(n int) []peer.ID {
			return localPeers[:min(n, len(localPeers))]
		},
		clock: clock.New(),
	}

	reprovider.scheduleLk.Lock()
	require.Equal(t, targetCpl, reprovider.getAvgPrefixLenNoLock())
	reprovider.scheduleLk.Unlock()
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
		provideFunc: func(ctx context.Context, k cid.Cid, broadcast bool) error {
			return nil
		},
	}
	mockClock := clock.NewMock()
	r := SweepingReprovider{
		router:            router,
		clock:             mockClock,
		reprovideInterval: time.Hour,
		pendingCidsChan:   make(chan []mh.Multihash, 1),
		failedRegionsChan: make(chan bitstr.Key, 1),
		schedule:          trie.New[bitstr.Key, time.Duration](),
		scheduleTimer:     mockClock.Timer(time.Hour),
		getSelfAddrs:      func() []ma.Multiaddr { return nil },
	}

	// Providing no cids returns no error
	err := r.individualProvideForPrefix(ctx, prefix, nil, initialProvide)
	require.NoError(t, err)

	// Providing a single cid - success
	err = r.individualProvideForPrefix(ctx, prefix, []mh.Multihash{k}, initialProvide)
	require.NoError(t, err)

	// Providing a single cid - failure
	router.getClosestPeersFunc = func(ctx context.Context, k string) ([]peer.ID, error) {
		return nil, errors.New("GetClosestPeers error")
	}
	err = r.individualProvideForPrefix(ctx, prefix, []mh.Multihash{k}, initialProvide)
	require.Error(t, err)
	require.Equal(t, []mh.Multihash{k}, <-r.pendingCidsChan)

	err = r.individualProvideForPrefix(ctx, prefix, []mh.Multihash{k}, periodicReprovide)
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
		provideFunc: func(ctx context.Context, k cid.Cid, broadcast bool) error {
			return nil
		},
	}
	mockClock := clock.NewMock()
	r := SweepingReprovider{
		router:            router,
		clock:             mockClock,
		reprovideInterval: time.Hour,
		pendingCidsChan:   make(chan []mh.Multihash, len(ks)),
		failedRegionsChan: make(chan bitstr.Key, 1),
		schedule:          trie.New[bitstr.Key, time.Duration](),
		scheduleTimer:     mockClock.Timer(time.Hour),
		getSelfAddrs:      func() []ma.Multiaddr { return nil },
	}

	// Providing two cids - 2 successes
	err := r.individualProvideForPrefix(ctx, prefix, ks, initialProvide)
	require.NoError(t, err)

	// Providing two cids - 2 failures
	router.getClosestPeersFunc = func(ctx context.Context, k string) ([]peer.ID, error) {
		return nil, errors.New("GetClosestPeers error")
	}
	err = r.individualProvideForPrefix(ctx, prefix, ks, initialProvide)
	require.Error(t, err)
	pendingCids := append(<-r.pendingCidsChan, <-r.pendingCidsChan...)
	require.Len(t, pendingCids, len(ks))
	require.Contains(t, pendingCids, ks[0])
	require.Contains(t, pendingCids, ks[1])

	err = r.individualProvideForPrefix(ctx, prefix, ks, periodicReprovide)
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

	err = r.individualProvideForPrefix(ctx, prefix, ks, initialProvide)
	require.NoError(t, err)
	require.Len(t, r.pendingCidsChan, 1)
	pendingCids = <-r.pendingCidsChan
	require.Len(t, pendingCids, 1)
	require.Contains(t, ks, pendingCids[0])

	err = r.individualProvideForPrefix(ctx, prefix, ks, periodicReprovide)
	require.NoError(t, err)
	require.Len(t, r.failedRegionsChan, 0)
	require.Len(t, r.pendingCidsChan, 0)
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
		peersTrie.Add(peerIDToBit256(p), p)
	}

	router := &mockRouter{
		getClosestPeersFunc: func(ctx context.Context, k string) ([]peer.ID, error) {
			sortedPeers := kb.SortClosestPeers(peers, kb.ConvertKey(k))
			return sortedPeers[:min(replicationFactor, len(peers))], nil
		},
		provideFunc: func(ctx context.Context, k cid.Cid, broadcast bool) error {
			return nil
		},
	}

	r := SweepingReprovider{
		router:            router,
		replicationFactor: replicationFactor,
	}

	for _, prefix := range []bitstr.Key{"", "0", "1", "00", "01", "10", "11", "000", "001", "010", "011", "100", "101", "110", "111"} {
		closestPeers, err := r.closestPeersToPrefix(prefix)
		require.NoError(t, err)
		subtrieSize := 0
		currPrefix := prefix
		// Reduce prefix if necessary as closestPeersToPrefix always returns at
		// least replicationFactor peers if possible.
		for {
			subtrie, ok := subtrieMatchingPrefix(peersTrie, currPrefix)
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

	reprovider := SweepingReprovider{
		replicationFactor: replicationFactor,
		order:             bit256.ZeroKey(),
	}

	cids := cidsToMhs(genCids(nCids))
	cidsTrie := trie.New[bit256.Key, mh.Multihash]()
	for _, c := range cids {
		cidsTrie.Add(mhToBit256(c), c)
	}
	peers := make([]peer.ID, nPeers)
	peersTrie := trie.New[bit256.Key, peer.ID]()
	for i := range peers {
		peers[i] = genRandPeerID(t)
		peersTrie.Add(peerIDToBit256(peers[i]), peers[i])
	}
	r := region{
		prefix: bitstr.Key(""),
		peers:  peersTrie,
		cids:   cidsTrie,
	}
	cidsAllocations := reprovider.cidsAllocationsToPeers(r)
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
	reprovider := SweepingReprovider{
		msgSender: msgSender,
	}

	nCids := 16
	pid, err := peer.Decode("12BoooooPEER")
	require.NoError(t, err)
	mhs := cidsToMhs(genCids(nCids))
	pmes := &pb.Message{}

	// All ADD_PROVIDER RPCs fail, return an error after reprovideInitialFailuresAllowed+1 attempts
	err = reprovider.provideCidsToPeer(pid, mhs, pmes)
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
	err = reprovider.provideCidsToPeer(pid, mhs, pmes)
	require.NoError(t, err)
	require.Equal(t, nCids, msgCount)
}

func TestProvideNoBootstrap(t *testing.T) {
	ctx := context.Background()
	pid, err := peer.Decode("12BoooooPEER")
	require.NoError(t, err)
	router := &mockRouter{}
	msgSender := &mockMsgSender{}
	opts := []Option{
		WithPeerID(pid),
		WithRouter(router),
		WithMessageSender(msgSender),
		WithSelfAddrs(func() []ma.Multiaddr {
			return nil
		}),
		WithLocalNearestPeersToSelf(func(int) []peer.ID {
			return nil
		}),
	}
	prov, err := NewReprovider(ctx, opts...)
	reprovider := prov.(*SweepingReprovider)
	require.NoError(t, err)

	_ = reprovider
	c := genCids(1)[0]

	// Set the reprovider as offline
	reprovider.connectivity.online.Store(false)
	err = prov.Provide(ctx, c, true)
	require.ErrorIs(t, ErrNodeOffline, err)

	// Set the reprovider as online, but don't bootstrap it
	reprovider.connectivity.online.Store(true)
	err = prov.Provide(ctx, c, true)
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

func TestProviderOffline(t *testing.T) {
	ctx := context.Background()
	online := atomic.Bool{}
	online.Store(false)
	checkFuncCalled := atomic.Bool{}
	mockClock := clock.NewMock()
	checkInterval := time.Minute
	catchupPendingChan := make(chan struct{}, 1)
	reprovider := SweepingReprovider{
		connectivity: connectivityChecker{
			ctx:                  ctx,
			clock:                mockClock,
			onlineCheckInterval:  checkInterval,
			offlineCheckInterval: checkInterval,
			checkFunc: func() bool {
				checkFuncCalled.Store(true)
				return online.Load()
			},
			backOnlineNotify: func() {
				catchupPendingChan <- struct{}{}
			},
		},
	}

	checked := func() bool {
		return checkFuncCalled.Load()
	}
	nodeOnline := func() bool {
		return reprovider.connectivity.online.Load()
	}
	nodeOffline := func() bool {
		return !reprovider.connectivity.online.Load()
	}
	// offline -> offline
	reprovider.connectivity.triggerCheck()
	waitUntil(t, checked, 10*time.Millisecond)
	waitUntil(t, nodeOffline, 10*time.Millisecond)
	require.Len(t, reprovider.catchupPendingChan, 0)

	// Wait before modifying online status
	checkFuncCalled.Store(false)
	online.Store(true)

	// offline -> online
	mockClock.Add(checkInterval)
	waitUntil(t, checked, 10*time.Millisecond)
	waitUntil(t, nodeOnline, 10*time.Millisecond)
	require.Len(t, catchupPendingChan, 1)
	<-catchupPendingChan

	mockClock.Add(checkInterval)
	checkFuncCalled.Store(false)
	// online -> online
	reprovider.connectivity.triggerCheck()
	waitUntil(t, checked, 10*time.Millisecond)
	waitUntil(t, nodeOnline, 10*time.Millisecond)
	require.Len(t, reprovider.catchupPendingChan, 0)

	checkFuncCalled.Store(false)
	mockClock.Add(checkInterval)
	online.Store(false)

	// online -> offline
	reprovider.connectivity.triggerCheck()
	waitUntil(t, checked, 10*time.Millisecond)
	waitUntil(t, nodeOffline, 10*time.Millisecond)
}

func TestProvideSingle(t *testing.T) {
	ctx := context.Background()
	pid, err := peer.Decode("12BoooooPEER")
	c := genCids(1)[0]
	require.NoError(t, err)

	mockClock := clock.NewMock()
	reprovideInterval := time.Hour

	mutex := sync.Mutex{}
	getClosestPeersCount := 0
	provideCount := 0
	router := &mockRouter{
		getClosestPeersFunc: func(ctx context.Context, k string) ([]peer.ID, error) {
			mutex.Lock()
			defer mutex.Unlock()
			getClosestPeersCount++
			return nil, nil
		},
		provideFunc: func(ctx context.Context, k cid.Cid, broadcast bool) error {
			mutex.Lock()
			defer mutex.Unlock()
			if !bytes.Equal(c.Hash(), k.Hash()) {
				t.Error("wrong cid")
			}
			provideCount++
			return nil
		},
	}
	msgSender := &mockMsgSender{}
	nLocalPeers := 16
	prefixLen := 12
	localPeers := make([]peer.ID, nLocalPeers)
	for i := range localPeers {
		localPeers[i], err = kb.GenRandPeerIDWithCPL(kb.ConvertPeerID(pid), uint(prefixLen))
		require.NoError(t, err)
	}
	opts := []Option{
		WithReprovideInterval(reprovideInterval),
		WithPeerID(pid),
		WithRouter(router),
		WithMessageSender(msgSender),
		WithSelfAddrs(func() []ma.Multiaddr {
			return nil
		}),
		WithLocalNearestPeersToSelf(func(n int) []peer.ID {
			return localPeers[:min(n, len(localPeers))]
		}),
		WithClock(mockClock),
	}
	prov, err := NewReprovider(ctx, opts...)
	require.NoError(t, err)

	// Blocks until cid is provided
	err = prov.Provide(ctx, c, true)
	require.NoError(t, err)
	require.Equal(t, 1, getClosestPeersCount)
	require.Equal(t, 1, provideCount)

	// Verify reprovide is scheduled.
	reprovider := prov.(*SweepingReprovider)
	prefix := bitstr.Key(key.BitString(mhToBit256(c.Hash()))[:prefixLen])
	reprovider.scheduleLk.Lock()
	require.Equal(t, 1, reprovider.schedule.Size())
	found, reprovideTime := trie.Find(reprovider.schedule, prefix)
	if !found {
		t.Fatal("prefix not inserted in schedule")
	}
	require.Equal(t, reprovider.reprovideTimeForPrefix(prefix), reprovideTime)
	reprovider.scheduleLk.Unlock()

	// Try to provide the same cid again. Returns no error, but it is a noop.
	err = prov.Provide(ctx, c, true)
	require.NoError(t, err)
	require.Equal(t, 1, getClosestPeersCount)
	require.Equal(t, 1, provideCount)

	// Verify reprovide happens as scheduled.
	mockClock.Add(reprovideTime - 1)
	require.Equal(t, 1, getClosestPeersCount)
	require.Equal(t, 1, provideCount)
	mockClock.Add(1)
	require.Equal(t, 2, getClosestPeersCount)
	require.Equal(t, 2, provideCount)
	mockClock.Add(reprovideInterval - 1)
	require.Equal(t, 2, getClosestPeersCount)
	require.Equal(t, 2, provideCount)
	mockClock.Add(reprovideInterval) // 1
	require.Equal(t, 2, getClosestPeersCount)
	require.Equal(t, 2, provideCount)
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

	routerLk := sync.Mutex{}
	getClosestPeersCount := 0
	provideCount := 0
	router := &mockRouter{
		getClosestPeersFunc: func(ctx context.Context, k string) ([]peer.ID, error) {
			routerLk.Lock()
			defer routerLk.Unlock()
			getClosestPeersCount++
			sortedPeers := kb.SortClosestPeers(peers, kb.ConvertKey(k))
			return sortedPeers[:min(replicationFactor, len(peers))], nil
		},
		provideFunc: func(ctx context.Context, k cid.Cid, broadcast bool) error {
			routerLk.Lock()
			defer routerLk.Unlock()
			if broadcast {
				provideCount++
			}
			return nil
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
			return nil
		}),
		WithLocalNearestPeersToSelf(func(n int) []peer.ID {
			peers, err := router.GetClosestPeers(ctx, string(pid))
			require.NoError(t, err)
			return peers[:min(n, len(peers))]
		}),
		WithClock(mockClock),
	}
	prov, err := NewReprovider(ctx, opts...)
	require.NoError(t, err)
	mockClock.Add(reprovideInterval - 1)

	reprovider := prov.(*SweepingReprovider)
	err = reprovider.ProvideMany(ctx, mhs)
	require.NoError(t, err)

	// No individual provides.
	routerLk.Lock()
	require.Equal(t, 0, provideCount)
	routerLk.Unlock()
	// Each cid should have been provided at least once.
	msgSenderLk.Lock()
	require.Len(t, addProviderRpcs, nCids)
	for _, peers := range addProviderRpcs {
		// Verify that all cids have been provided to exactly replicationFactor
		// distinct peers.
		require.Len(t, peers, replicationFactor)
	}

	// Test reprovides
	clear(addProviderRpcs)
	msgSenderLk.Unlock()
	for range reprovideInterval / time.Minute {
		mockClock.Add(time.Minute)
	}
	routerLk.Lock()
	require.Equal(t, 0, provideCount)
	routerLk.Unlock()
	msgSenderLk.Lock()
	require.Equal(t, nCids, len(addProviderRpcs))
	require.Len(t, addProviderRpcs, nCids)
	for _, peers := range addProviderRpcs {
		// Verify that all cids have been provided to exactly replicationFactor
		// distinct peers.
		require.Len(t, peers, replicationFactor)
	}

	// Test reprovides again
	clear(addProviderRpcs)
	msgSenderLk.Unlock()
	for range reprovideInterval / time.Minute {
		mockClock.Add(time.Minute)
	}
	routerLk.Lock()
	require.Equal(t, 0, provideCount)
	routerLk.Unlock()
	msgSenderLk.Lock()
	require.Equal(t, nCids, len(addProviderRpcs))
	require.Len(t, addProviderRpcs, nCids)
	for _, peers := range addProviderRpcs {
		// Verify that all cids have been provided to exactly replicationFactor
		// distinct peers.
		require.Len(t, peers, replicationFactor)
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
	connectivityCheckInterval := time.Minute

	routerLk := sync.Mutex{}
	routerOffline := atomic.Bool{}
	routerOffline.Store(true)
	getClosestPeersCount := 0
	provideCount := 0
	router := &mockRouter{
		getClosestPeersFunc: func(ctx context.Context, k string) ([]peer.ID, error) {
			routerLk.Lock()
			defer routerLk.Unlock()
			getClosestPeersCount++
			if routerOffline.Load() {
				return nil, errors.New("offline")
			}
			sortedPeers := kb.SortClosestPeers(peers, kb.ConvertKey(k))
			return sortedPeers[:min(replicationFactor, len(peers))], nil
		},
		provideFunc: func(ctx context.Context, k cid.Cid, broadcast bool) error {
			routerLk.Lock()
			defer routerLk.Unlock()
			if routerOffline.Load() {
				return errors.New("offline")
			}
			if broadcast {
				provideCount++
			}
			return nil
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
			return nil
		}),
		WithLocalNearestPeersToSelf(func(n int) []peer.ID {
			peers := kb.SortClosestPeers(peers, kb.ConvertPeerID(pid))
			return peers[:min(n, len(peers))]
		}),
		WithClock(mockClock),
		WithConnectivityCheckOnlineInterval(connectivityCheckInterval),
		WithConnectivityCheckOfflineInterval(connectivityCheckInterval),
	}
	prov, err := NewReprovider(ctx, opts...)
	require.NoError(t, err)

	reprovider := prov.(*SweepingReprovider)
	reprovider.connectivity = connectivityChecker{
		ctx:                  ctx,
		clock:                mockClock,
		onlineCheckInterval:  connectivityCheckInterval,
		offlineCheckInterval: connectivityCheckInterval,
		checkFunc: func() bool {
			peers, err := router.GetClosestPeers(ctx, string(pid))
			return err == nil && len(peers) > 0
		},
		backOnlineNotify: reprovider.catchupPendingNotify,
	}
	reprovider.connectivity.online.Store(true)

	err = reprovider.ProvideMany(ctx, mhs)
	require.Error(t, err)

	nodeOffline := func() bool {
		return !reprovider.connectivity.isOnline()
	}
	waitUntil(t, nodeOffline, 10*time.Millisecond, "waiting for node to be offline")
	mockClock.Add(connectivityCheckInterval)

	routerOffline.Store(false)
	mockClock.Add(connectivityCheckInterval)
	waitUntil(t, reprovider.connectivity.isOnline, 10*time.Millisecond, "waiting for node to come back online")

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
	waitUntil(t, providedAllCids, 100*time.Millisecond, "waiting for all cids to be provided")
}
