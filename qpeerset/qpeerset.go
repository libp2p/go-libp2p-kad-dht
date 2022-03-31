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

	// all known peers and their query states
	states map[peer.ID]*queryPeerState
}

type queryPeerState struct {
	distance   *big.Int
	state      PeerState
	referredBy peer.ID
}

// NewQueryPeerset creates a new empty set of peers.
// key is the target key of the lookup that this peer set is for.
func NewQueryPeerset(key string) *QueryPeerset {
	return &QueryPeerset{
		key:    ks.XORKeySpace.Key([]byte(key)),
		states: map[peer.ID]*queryPeerState{},
	}
}

func (qp *QueryPeerset) distanceToKey(p peer.ID) *big.Int {
	return ks.XORKeySpace.Key([]byte(p)).Distance(qp.key)
}

// TryAdd adds the peer p to the peer set.
// If the peer is already present, no action is taken.
// Otherwise, the peer is added with state set to PeerHeard.
// TryAdd returns true iff the peer was not already present.
func (qp *QueryPeerset) TryAdd(p, referredBy peer.ID) bool {
	if _, found := qp.states[p]; found {
		return false
	}

	qp.states[p] = &queryPeerState{
		distance:   qp.distanceToKey(p),
		state:      PeerHeard,
		referredBy: referredBy,
	}

	return true
}

// SetState sets the state of peer p to s.
// If p is not in the peerset, SetState panics.
func (qp *QueryPeerset) SetState(p peer.ID, s PeerState) {
	qp.states[p].state = s
}

// GetState returns the state of peer p.
// If p is not in the peerset, GetState panics.
func (qp *QueryPeerset) GetState(p peer.ID) PeerState {
	return qp.states[p].state
}

// GetReferrer returns the peer that referred us to the peer p.
// If p is not in the peerset, GetReferrer panics.
func (qp *QueryPeerset) GetReferrer(p peer.ID) peer.ID {
	return qp.states[p].referredBy
}

// GetClosestNInStates returns the closest to the key peers, which are in one of the given states.
// It returns n peers or less, if fewer peers meet the condition.
// The returned peers are sorted in ascending order by their distance to the key.
func (qp *QueryPeerset) GetClosestNInStates(n int, states ...PeerState) []peer.ID {
	m := make(map[PeerState]struct{}, len(states))
	for i := range states {
		m[states[i]] = struct{}{}
	}

	results := []peer.ID{}
	for p, state := range qp.states {
		if _, ok := m[state.state]; ok {
			results = append(results, p)
		}
	}

	sort.Slice(results, func(i, j int) bool {
		di, dj := qp.states[results[i]].distance, qp.states[results[j]].distance
		return di.Cmp(dj) == -1
	})

	if len(results) >= n {
		return results[:n]
	}

	return results
}

// GetClosestInStates returns the peers, which are in one of the given states.
// The returned peers are sorted in ascending order by their distance to the key.
func (qp *QueryPeerset) GetClosestInStates(states ...PeerState) []peer.ID {
	return qp.GetClosestNInStates(len(qp.states), states...)
}

// NumHeard returns the number of peers in state PeerHeard.
func (qp *QueryPeerset) NumHeard() int {
	return len(qp.GetClosestInStates(PeerHeard))
}

// NumWaiting returns the number of peers in state PeerWaiting.
func (qp *QueryPeerset) NumWaiting() int {
	return len(qp.GetClosestInStates(PeerWaiting))
}

func (qp *QueryPeerset) GetIntersection(other *QueryPeerset) []peer.ID {
	if !qp.key.Equal(other.key) {
		return []peer.ID{}
	}

	// loop over smaller set
	smaller := qp.states
	larger := other.states
	if len(smaller) > len(larger) {
		smaller = other.states
		larger = qp.states
	}

	results := []peer.ID{}

	for p := range smaller {
		if _, found := larger[p]; found {
			results = append(results, p)
		}
	}

	return results
}
