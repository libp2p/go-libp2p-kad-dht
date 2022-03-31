package qpeerset

import (
	"math/big"
	"sort"

	"github.com/libp2p/go-libp2p-core/peer"
	ks "github.com/whyrusleeping/go-keyspace"
)

// SingleQueryPeerset maintains the state of a Kademlia asynchronous lookup.
// The lookup state is a set of peers, each labeled with a peer state.
type SingleQueryPeerset struct {
	// the key being searched for
	key ks.Key

	// all known peers
	all []queryPeerState

	// sorted is true if all is currently in sorted order
	sorted bool
}

// NewQueryPeerset creates a new empty set of peers.
// key is the target key of the lookup that this peer set is for.
func NewQueryPeerset(key string) *SingleQueryPeerset {
	return &SingleQueryPeerset{
		key:    ks.XORKeySpace.Key([]byte(key)),
		all:    []queryPeerState{},
		sorted: false,
	}
}

func (qp *SingleQueryPeerset) find(p peer.ID) int {
	for i := range qp.all {
		if qp.all[i].id == p {
			return i
		}
	}
	return -1
}

func (qp *SingleQueryPeerset) distanceToKey(p peer.ID) *big.Int {
	return ks.XORKeySpace.Key([]byte(p)).Distance(qp.key)
}

// TryAdd adds the peer p to the peer set.
// If the peer is already present, no action is taken.
// Otherwise, the peer is added with state set to PeerHeard.
// TryAdd returns true iff the peer was not already present.
func (qp *SingleQueryPeerset) TryAdd(p, referredBy peer.ID) bool {
	if qp.find(p) >= 0 {
		return false
	} else {
		qp.all = append(qp.all,
			queryPeerState{id: p, distance: qp.distanceToKey(p), state: PeerHeard, referredBy: referredBy})
		qp.sorted = false
		return true
	}
}

func (qp *SingleQueryPeerset) sort() {
	if qp.sorted {
		return
	}
	sort.Sort((*sortedQueryPeerset)(qp))
	qp.sorted = true
}

// SetState sets the state of peer p to s.
// If p is not in the peerset, SetState panics.
func (qp *SingleQueryPeerset) SetState(p peer.ID, s PeerState) {
	qp.all[qp.find(p)].state = s
}

// GetState returns the state of peer p.
// If p is not in the peerset, GetState panics.
func (qp *SingleQueryPeerset) GetState(p peer.ID) PeerState {
	return qp.all[qp.find(p)].state
}

// GetReferrer returns the peer that referred us to the peer p.
// If p is not in the peerset, GetReferrer panics.
func (qp *SingleQueryPeerset) GetReferrer(p peer.ID) peer.ID {
	return qp.all[qp.find(p)].referredBy
}

// GetClosestNInStates returns the closest to the key peers, which are in one of the given states.
// It returns n peers or less, if fewer peers meet the condition.
// The returned peers are sorted in ascending order by their distance to the key.
func (qp *SingleQueryPeerset) GetClosestNInStates(n int, states ...PeerState) (result []peer.ID) {
	qp.sort()
	m := make(map[PeerState]struct{}, len(states))
	for i := range states {
		m[states[i]] = struct{}{}
	}

	for _, p := range qp.all {
		if _, ok := m[p.state]; ok {
			result = append(result, p.id)
		}
	}
	if len(result) >= n {
		return result[:n]
	}
	return result
}

// GetClosestInStates returns the peers, which are in one of the given states.
// The returned peers are sorted in ascending order by their distance to the key.
func (qp *SingleQueryPeerset) GetClosestInStates(states ...PeerState) (result []peer.ID) {
	return qp.GetClosestNInStates(len(qp.all), states...)
}

// NumHeard returns the number of peers in state PeerHeard.
func (qp *SingleQueryPeerset) NumHeard() int {
	return len(qp.GetClosestInStates(PeerHeard))
}

// NumWaiting returns the number of peers in state PeerWaiting.
func (qp *SingleQueryPeerset) NumWaiting() int {
	return len(qp.GetClosestInStates(PeerWaiting))
}
