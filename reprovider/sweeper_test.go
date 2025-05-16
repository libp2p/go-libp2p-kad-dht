package reprovider

import (
	"bytes"
	"context"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/filecoin-project/go-clock"
	"github.com/ipfs/go-cid"
	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
	kb "github.com/libp2p/go-libp2p-kbucket"
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
	s := reprovideSweeper{
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
	s := reprovideSweeper{
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

var _ KadRouter = (*mockRouter)(nil)

type mockRouter struct {
	peers *trie.Trie[bit256.Key, peer.ID]
}

func (r *mockRouter) GetClosestPeers(ctx context.Context, k string) ([]peer.ID, error) {
	return allValues(r.peers, bitstr.Key(strings.Repeat("0", 256))), nil
}

func (r *mockRouter) Provide(ctx context.Context, c cid.Cid, _ bool) error {
	return nil
}

var _ KadRouter = (*modularMockRouter)(nil)

type modularMockRouter struct {
	getClosestPeersFunc func(ctx context.Context, k string) ([]peer.ID, error)
	provideFunc         func(ctx context.Context, c cid.Cid, _ bool) error
}

func (r *modularMockRouter) GetClosestPeers(ctx context.Context, k string) ([]peer.ID, error) {
	return r.getClosestPeersFunc(ctx, k)
}

func (r *modularMockRouter) Provide(ctx context.Context, c cid.Cid, broadcast bool) error {
	return r.provideFunc(ctx, c, broadcast)
}

var _ pb.MessageSender = (*mockMessageSender)(nil)

type mockMessageSender struct{}

func (msg *mockMessageSender) SendRequest(ctx context.Context, p peer.ID, m *pb.Message) (*pb.Message, error) {
	return nil, nil
}

func (msg *mockMessageSender) SendMessage(ctx context.Context, p peer.ID, m *pb.Message) error {
	return nil
}

func TestProvideNoBootstrap(t *testing.T) {
	ctx := context.Background()
	pid, err := peer.Decode("12BoooooPEER")
	require.NoError(t, err)
	router := &mockRouter{
		peers: trie.New[bit256.Key, peer.ID](),
	}
	msgSender := &mockMessageSender{}
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
	reprovider := prov.(*reprovideSweeper)
	require.NoError(t, err)

	_ = reprovider
	c := genCids(1)[0]

	// Set the reprovider as offline
	reprovider.online.Store(false)
	err = prov.Provide(ctx, c, true)
	require.ErrorIs(t, ErrNodeOffline, err)

	// Set the reprovider as online, but don't bootstrap it
	reprovider.online.Store(true)
	err = prov.Provide(ctx, c, true)
	require.NoError(t, err)
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

	reprovider := &reprovideSweeper{
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
	reprovider := reprovideSweeper{
		replicationFactor: 20,
		order:             bit256.NewKey(selfKey[:]),
		schedule:          trie.New[bitstr.Key, time.Duration](),
		localNearestPeersToSelf: func(n int) []peer.ID {
			return localPeers[:min(n, len(localPeers))]
		},
	}

	reprovider.scheduleLk.Lock()
	require.Equal(t, targetCpl, reprovider.getAvgPrefixLenNoLock())
	reprovider.scheduleLk.Unlock()
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
	router := &modularMockRouter{
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
	msgSender := &mockMessageSender{}
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
	require.Equal(t, 0, getClosestPeersCount)
	require.Equal(t, 1, provideCount)

	// Verify reprovide is scheduled.
	reprovider := prov.(*reprovideSweeper)
	prefix := bitstr.Key(key.BitString(mhToBit256(c.Hash()))[:prefixLen])
	reprovider.scheduleLk.Lock()
	require.Equal(t, 1, reprovider.schedule.Size())
	found, reprovideTime := trie.Find(reprovider.schedule, prefix)
	if !found {
		t.Fatal("prefix not inserted in schedule")
	}
	require.Equal(t, reprovider.reprovideTimeForPrefix(prefix), reprovideTime)
	reprovider.scheduleLk.Unlock()

	// Try to reprovide the same cid. Returns no error, but it is a noop.
	err = prov.Provide(ctx, c, true)
	require.NoError(t, err)
	require.Equal(t, 0, getClosestPeersCount)
	require.Equal(t, 1, provideCount)

	// Verify reprovide happens as scheduled.
	mockClock.Add(reprovideTime - 1)
	require.Equal(t, 0, getClosestPeersCount)
	require.Equal(t, 1, provideCount)
	mockClock.Add(1)
	require.Equal(t, 0, getClosestPeersCount)
	require.Equal(t, 2, provideCount)
	mockClock.Add(reprovideInterval - 1)
	require.Equal(t, 0, getClosestPeersCount)
	require.Equal(t, 2, provideCount)
	mockClock.Add(1)
	require.Equal(t, 0, getClosestPeersCount)
	require.Equal(t, 3, provideCount)
}

func TestProvideMany(t *testing.T) {
	t.Skip()
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

	mutex := sync.Mutex{}
	getClosestPeersCount := 0
	provideCount := 0
	router := &modularMockRouter{
		getClosestPeersFunc: func(ctx context.Context, k string) ([]peer.ID, error) {
			mutex.Lock()
			defer mutex.Unlock()
			getClosestPeersCount++
			sortedPeers := kb.SortClosestPeers(peers, kb.ConvertKey(k))
			return sortedPeers[:min(replicationFactor, len(peers))], nil
		},
		provideFunc: func(ctx context.Context, k cid.Cid, broadcast bool) error {
			mutex.Lock()
			defer mutex.Unlock()
			provideCount++
			return nil
		},
	}
	msgSender := &mockMessageSender{}
	nLocalPeers := 16
	prefixLen := 12
	localPeers := make([]peer.ID, nLocalPeers)
	for i := range localPeers {
		localPeers[i], err = kb.GenRandPeerIDWithCPL(kb.ConvertPeerID(pid), uint(prefixLen))
		require.NoError(t, err)
	}
	opts := []Option{
		WithReprovideInterval(reprovideInterval),
		WithReplicationFactor(replicationFactor),
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

	reprovider := prov.(*reprovideSweeper)
	reprovider.ProvideMany(ctx, mhs)
	_ = nCids
	t.Fail()
}
