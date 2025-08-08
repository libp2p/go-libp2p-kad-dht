package provider

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/ipfs/go-test/random"
	kb "github.com/libp2p/go-libp2p-kbucket"
	"github.com/libp2p/go-libp2p/core/peer"
	mh "github.com/multiformats/go-multihash"

	"github.com/probe-lab/go-libdht/kad/key/bit256"
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
