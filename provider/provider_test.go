package provider

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/filecoin-project/go-clock"
	"github.com/ipfs/go-test/random"
	kb "github.com/libp2p/go-libp2p-kbucket"
	"github.com/libp2p/go-libp2p/core/peer"
	mh "github.com/multiformats/go-multihash"

	"github.com/probe-lab/go-libdht/kad/key/bit256"
	"github.com/probe-lab/go-libdht/kad/key/bitstr"
	"github.com/probe-lab/go-libdht/kad/trie"

	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
	"github.com/libp2p/go-libp2p-kad-dht/provider/internal/keyspace"

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
