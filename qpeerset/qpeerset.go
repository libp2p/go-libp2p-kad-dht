package qpeerset

import (
	"math/big"
	"sort"

	"github.com/libp2p/go-libp2p-core/peer"
	ks "github.com/whyrusleeping/go-keyspace"
)

// PeerState describes the state of a peer ID during the lifecycle of an individual lookup.
type PeerState int

const (
	// PeerHeard is applied to peers which have not been queried yet.
	PeerHeard PeerState = iota
	// PeerWaiting is applied to peers that are currently being queried.
	PeerWaiting
	// PeerQueried is applied to peers who have been queried and a response was retrieved successfully.
	PeerQueried
	// PeerUnreachable is applied to peers who have been queried and a response was not retrieved successfully.
	PeerUnreachable
)

// QueryPeerset maintains the state of a Kademlia asynchronous lookup.
// The lookup state is a set of peers, each labeled with a peer state.
type QueryPeerset struct {
	// the key being searched for
	key ks.Key

	// all known peers
	all []queryPeerState

	// sorted is true if all is currently in sorted order
	sorted bool
}

type queryPeerState struct {
	id         peer.ID
	distance   *big.Int
	state      PeerState
	referredBy peer.ID
}

type sortedQueryPeerset QueryPeerset

func (sqp *sortedQueryPeerset) Len() int {
	return len(sqp.all)
}

func (sqp *sortedQueryPeerset) Swap(i, j int) {
	sqp.all[i], sqp.all[j] = sqp.all[j], sqp.all[i]
}

func (sqp *sortedQueryPeerset) Less(i, j int) bool {
	di, dj := sqp.all[i].distance, sqp.all[j].distance
	return di.Cmp(dj) == -1
}

// NewQueryPeerset creates a new empty set of peers.
// key is the target key of the lookup that this peer set is for.
func NewQueryPeerset(key string) *QueryPeerset {
	return &QueryPeerset{
		key:    ks.XORKeySpace.Key([]byte(key)),
		all:    []queryPeerState{},
		sorted: false,
	}
}

func (qp *QueryPeerset) find(p peer.ID) int {
	for i := range qp.all {
		if qp.all[i].id == p {
			return i
		}
	}
	return -1
}

func (qp *QueryPeerset) distanceToKey(p peer.ID) *big.Int {
	return ks.XORKeySpace.Key([]byte(p)).Distance(qp.key)
}

// TryAdd adds the peer p to the peer set.
// If the peer is already present, no action is taken.
// Otherwise, the peer is added with state set to PeerHeard.
// TryAdd returns true iff the peer was not already present.
func (qp *QueryPeerset) TryAdd(p, referredBy peer.ID) bool {
	if qp.find(p) >= 0 {
		return false
	} else {
		qp.all = append(qp.all,
			queryPeerState{id: p, distance: qp.distanceToKey(p), state: PeerHeard, referredBy: referredBy})
		qp.sorted = false
		return true
	}
}

func (qp *QueryPeerset) sort() {
	if qp.sorted {
		return
	}
	sort.Sort((*sortedQueryPeerset)(qp))
	qp.sorted = true
}

// SetState sets the state of peer p to s.
// If p is not in the peerset, SetState panics.
func (qp *QueryPeerset) SetState(p peer.ID, s PeerState) {
	qp.all[qp.find(p)].state = s
}

// GetState returns the state of peer p.
// If p is not in the peerset, GetState panics.
func (qp *QueryPeerset) GetState(p peer.ID) PeerState {
	return qp.all[qp.find(p)].state
}

// GetReferrer returns the peer that referred us to the peer p.
// If p is not in the peerset, GetReferrer panics.
func (qp *QueryPeerset) GetReferrer(p peer.ID) peer.ID {
	return qp.all[qp.find(p)].referredBy
}

// NumWaiting returns the number of peers in state PeerWaiting.
func (qp *QueryPeerset) NumWaiting() int {
	return len(qp.GetWaitingPeers())
}

// GetWaitingPeers returns a slice of all peers in state PeerWaiting, in an undefined order.
func (qp *QueryPeerset) GetWaitingPeers() (result []peer.ID) {
	for _, p := range qp.all {
		if p.state == PeerWaiting {
			result = append(result, p.id)
		}
	}
	return
}

// GetClosestNotUnreachable returns the closest to the key peers, which are not in state PeerUnreachable.
// If count is non-negative, GetClosestNotUnreachable returns count peers or less, if fewer peers meet the condition.
// Otherwise, GetClosestNotUnreachable returns all peers meeting the condition.
// In all cases, returned peers are sorted by ascending distance to target.
func (qp *QueryPeerset) GetClosestNotUnreachable(count int) (result []peer.ID) {
	qp.sort()
	for _, p := range qp.all {
		if p.state != PeerUnreachable {
			result = append(result, p.id)
		}
	}
	if count >= 0 && len(result) >= count {
		return result[:count]
	}
	return result
}

// NumHeard returns the number of peers in state PeerHeard.
func (qp *QueryPeerset) NumHeard() int {
	return len(qp.GetHeardPeers())
}

// GetHeardPeers returns a slice of all peers in state PeerHeard, in an undefined order.
func (qp *QueryPeerset) GetHeardPeers() (result []peer.ID) {
	for _, p := range qp.all {
		if p.state == PeerHeard {
			result = append(result, p.id)
		}
	}
	return
}

// GetSortedHeard returns a slice of all peers in state PeerHeard, ordered by ascending distance to the target key.
func (qp *QueryPeerset) GetSortedHeard() (result []peer.ID) {
	qp.sort()
	return qp.GetHeardPeers()
}
