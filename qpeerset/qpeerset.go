package qpeerset

import (
	"math/big"
	"sort"
	"sync"

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
	statesLk sync.RWMutex
	states   map[peer.ID]*queryPeerState
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
	qp.statesLk.Lock()
	defer qp.statesLk.Unlock()

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
	qp.statesLk.Lock()
	qp.states[p].state = s
	qp.statesLk.Unlock()
}

// GetState returns the state of peer p.
// If p is not in the peerset, GetState panics.
func (qp *QueryPeerset) GetState(p peer.ID) PeerState {
	qp.statesLk.RLock()
	defer qp.statesLk.RUnlock()
	return qp.states[p].state
}

// GetReferrer returns the peer that referred us to the peer p.
// If p is not in the peerset, GetReferrer panics.
func (qp *QueryPeerset) GetReferrer(p peer.ID) peer.ID {
	qp.statesLk.RLock()
	defer qp.statesLk.RUnlock()
	return qp.states[p].referredBy
}

// GetClosestNInStates returns the closest to the key peers, which are in one of the given states.
// It returns n peers or less, if fewer peers meet the condition.
// The returned peers are sorted in ascending order by their distance to the key.
func (qp *QueryPeerset) GetClosestNInStates(n int, states ...PeerState) []peer.ID {
	qp.statesLk.RLock()
	defer qp.statesLk.RUnlock()

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

// GetIntersection returns all peers that are in qp and all other given query peer sets
// regardless of their state.
// TODO: Remove duplication with GetIntersectionNotInState. However, this function isn't used anyways...
func (qp *QueryPeerset) GetIntersection(others ...*QueryPeerset) []peer.ID {
	var smallest map[peer.ID]*queryPeerState
	var smallestIdx int

	all := append(others, qp)
	for i, qps := range all {
		qps.statesLk.RLock()
		defer qps.statesLk.RUnlock()

		if !qp.key.Equal(qps.key) {
			return []peer.ID{}
		}

		if len(qps.states) < len(smallest) || smallest == nil {
			smallest = qps.states
			smallestIdx = i
		}
	}

	// Remove smallest peer set from the list
	all[smallestIdx] = all[len(all)-1]
	all = all[:len(all)-1]

	results := []peer.ID{}
OUTER:
	for p := range smallest {
		for _, other := range all {
			if _, found := other.states[p]; !found {
				continue OUTER
			}
		}
		results = append(results, p)
	}

	return results
}

// GetIntersectionNotInState returns all peers that are in qp and all other given query peer sets
// and none of the peer sets tracks a peer as PeerUnreachable. This means all peers in the resulting list
// can either be in the states PeerHeard, PeerWaiting, or PeerQueried.
// The resulting list is not ordered in any way.
func (qp *QueryPeerset) GetIntersectionNotInState(state PeerState, others ...*QueryPeerset) []peer.ID {
	var smallest map[peer.ID]*queryPeerState
	var smallestIdx int

	all := append(others, qp)
	for i, qps := range all {
		qps.statesLk.RLock()
		defer qps.statesLk.RUnlock()

		if !qp.key.Equal(qps.key) {
			return []peer.ID{}
		}

		if len(qps.states) < len(smallest) || smallest == nil {
			smallest = qps.states
			smallestIdx = i
		}
	}

	// Remove smallest peer set from the list
	all[smallestIdx] = all[len(all)-1]
	all = all[:len(all)-1]

	results := []peer.ID{}
OUTER:
	for p, ps := range smallest {
		if ps.state == state {
			continue
		}
		for _, other := range all {
			if qpstate, found := other.states[p]; !found || qpstate.state == state {
				continue OUTER
			}
		}
		results = append(results, p)
	}

	return results
}
