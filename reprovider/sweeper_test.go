package reprovider

import (
	"bytes"
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
	kb "github.com/libp2p/go-libp2p-kbucket"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	mh "github.com/multiformats/go-multihash"
	"github.com/probe-lab/go-libdht/kad/key/bit256"
	"github.com/probe-lab/go-libdht/kad/key/bitstr"
	"github.com/probe-lab/go-libdht/kad/trie"
	"github.com/stretchr/testify/require"
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

var _ pb.MessageSender = (*mockMessageSender)(nil)

type mockMessageSender struct{}

func (msg *mockMessageSender) SendRequest(ctx context.Context, p peer.ID, m *pb.Message) (*pb.Message, error) {
	return nil, nil
}

func (msg *mockMessageSender) SendMessage(ctx context.Context, p peer.ID, m *pb.Message) error {
	return nil
}

func TestProvideSingle(t *testing.T) {
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
	reprovider, err := NewReprovider(ctx, opts...)
	require.NoError(t, err)

	c := genCids(1)[0]
	err = reprovider.Provide(ctx, c, true)
	require.ErrorIs(t, ErrClientNotBoostrapped, err)
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

	// localNearestPeersCPL should return 0 if replication factor is 0
	require.Equal(t, 0, reprovider.localNearestPeersCPL())

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

	require.Equal(t, targetCpl, reprovider.getAvgPrefixLen())
}
