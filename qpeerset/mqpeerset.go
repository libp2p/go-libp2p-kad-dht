package qpeerset

import (
	"fmt"
	"sort"

	"github.com/google/uuid"
	"github.com/libp2p/go-libp2p-core/peer"
	ks "github.com/whyrusleeping/go-keyspace"
)

// MultiQueryPeerset maintains the state of multiple Kademlia asynchronous lookups.
// The lookup state is a set queries with each a set of peers where each is labeled
// with a peer state.
type MultiQueryPeerset struct {
	// the key being searched for
	key ks.Key

	// all known peers of all running queries
	states map[uuid.UUID]map[peer.ID]*queryPeerState

	// all peers that appeared in all queries
	intersections map[peer.ID]*intersPeerState
}

// NewMultiQueryPeerset creates a new empty set of lookup queries.
// key is the target key of the lookup that this query set is for.
func NewMultiQueryPeerset(key string, queryIDs ...uuid.UUID) *MultiQueryPeerset {
	mqps := &MultiQueryPeerset{
		key:           ks.XORKeySpace.Key([]byte(key)),
		states:        map[uuid.UUID]map[peer.ID]*queryPeerState{},
		intersections: map[peer.ID]*intersPeerState{},
	}
	for _, queryID := range queryIDs {
		mqps.states[queryID] = map[peer.ID]*queryPeerState{}
	}
	return mqps
}

// TryAdd adds the peer p to the peer set.
// If the peer is already present, no action is taken.
// Otherwise, the peer is added with state set to PeerHeard.
// TryAdd returns true if the peer was not already present.
func (mqp *MultiQueryPeerset) TryAdd(qid uuid.UUID, referrer peer.ID, p peer.ID) bool {
	// panic if the query ID is unknown
	query, found := mqp.states[qid]
	if !found {
		panic(fmt.Sprintf("tried to add peer for unknown query %s", qid))
	}

	// don't do anything if we already know the peer in that query
	if _, found = query[p]; found {
		return false
	}

	mqp.states[qid][p] = &queryPeerState{
		id:         p,
		distance:   ks.XORKeySpace.Key([]byte(p)).Distance(mqp.key),
		state:      PeerHeard,
		referredBy: referrer,
	}

	count := 0
	state := PeerHeard
	for _, peersMap := range mqp.states {
		if _, found := peersMap[p]; found {
			count += 1
			if peersMap[p].state > state {
				state = peersMap[p].state
			}
		}
	}

	if count == len(mqp.states) {
		mqp.intersections[p] = &intersPeerState{
			ID:       p,
			Distance: ks.XORKeySpace.Key([]byte(p)).Distance(mqp.key),
			State:    state,
		}
	}

	return true
}

func (mqp *MultiQueryPeerset) TryAddMany(qid uuid.UUID, referrer peer.ID, peerIDs ...peer.ID) {
	for _, p := range peerIDs {
		mqp.TryAdd(qid, referrer, p)
	}
}

// GetState returns the state of peer p in the query qid.
// If p is not in the peerset, GetState panics.
func (mqp *MultiQueryPeerset) GetState(qid uuid.UUID, p peer.ID) PeerState {
	return mqp.states[qid][p].state
}

// SetState sets the state of peer p in query qid to s.
// If p or qid is not in the peerset, SetState panics.
func (mqp *MultiQueryPeerset) SetState(qid uuid.UUID, p peer.ID, s PeerState) {
	mqp.states[qid][p].state = s
	if inters, found := mqp.intersections[p]; found && s > inters.State {
		mqp.intersections[p].State = s
	}
}

// GetIntersections returns an ascending list of peers that appeared in all queries.
func (mqp *MultiQueryPeerset) GetIntersections() []*intersPeerState {
	var states []*intersPeerState
	for _, state := range mqp.intersections {
		states = append(states, state)
	}
	sort.Slice(states, func(i, j int) bool {
		return states[i].Distance.Cmp(states[j].Distance) <= 0
	})
	return states
}
