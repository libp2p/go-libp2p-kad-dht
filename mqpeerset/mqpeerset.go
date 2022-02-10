package mqpeerset

import (
	"fmt"
	"math/big"
	"sort"
	"strconv"

	"github.com/google/uuid"
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

func (ps PeerState) String() string {
	switch ps {
	case PeerHeard:
		return "HEARD"
	case PeerWaiting:
		return "WAITING"
	case PeerQueried:
		return "QUERIED"
	case PeerUnreachable:
		return "UNREACHABLE"
	}
	panic("unknown PeerState " + strconv.Itoa(int(ps)))
}

// MultiQueryPeerset .
type MultiQueryPeerset struct {
	// the key being searched for
	key ks.Key

	// all known peers of all running queries
	states map[uuid.UUID]map[peer.ID]*multiQueryPeerState

	// all peers that appeared in all queries
	intersections map[peer.ID]*IntersectionPeerState
}

type multiQueryPeerState struct {
	id         peer.ID
	distance   *big.Int
	state      PeerState
	referredBy peer.ID
}

type IntersectionPeerState struct {
	id       peer.ID
	distance *big.Int
	state    PeerState
}

func (ips *IntersectionPeerState) String() string {
	return fmt.Sprintf("%s (%s)", ips.id.String()[:16], ips.state)
}

// NewMultiQueryPeerset creates a new empty set of peers.
// key is the target key of the lookup that this peer set is for.
func NewMultiQueryPeerset(key string, queryIDs ...uuid.UUID) *MultiQueryPeerset {
	mqps := &MultiQueryPeerset{
		key:           ks.XORKeySpace.Key([]byte(key)),
		states:        map[uuid.UUID]map[peer.ID]*multiQueryPeerState{},
		intersections: map[peer.ID]*IntersectionPeerState{},
	}
	for _, queryID := range queryIDs {
		mqps.states[queryID] = map[peer.ID]*multiQueryPeerState{}
	}
	return mqps
}

// TryAdd adds the peer p to the peer set.
// If the peer is already present, no action is taken.
// Otherwise, the peer is added with state set to PeerHeard.
// TryAdd returns true if the peer was not already present.
func (mqp *MultiQueryPeerset) TryAdd(qid uuid.UUID, referrer peer.ID, p peer.ID) bool {
	// panic if the query ID is unknown
	if _, found := mqp.states[qid]; !found {
		panic("tried to add peer for unknown query")
	}

	// don't do anything if we already know the peer in that query
	if _, found := mqp.states[qid][p]; found {
		return false
	}

	mqp.states[qid][p] = &multiQueryPeerState{
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
		mqp.intersections[p] = &IntersectionPeerState{
			id:       p,
			distance: ks.XORKeySpace.Key([]byte(p)).Distance(mqp.key),
			state:    state,
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
	if _, found := mqp.intersections[p]; found && s > mqp.intersections[p].state {
		mqp.intersections[p].state = s
	}
}

// GetIntersections returns an ascending list of peers that appeared in all queries.
func (mqp *MultiQueryPeerset) GetIntersections() []*IntersectionPeerState {
	var states []*IntersectionPeerState
	for _, state := range mqp.intersections {
		states = append(states, state)
	}
	sort.Slice(states, func(i, j int) bool {
		return states[i].distance.Cmp(states[j].distance) <= 0
	})
	return states
}
