package qpeerset

import (
	"math/big"
	"sort"

	"github.com/libp2p/go-libp2p-core/peer"
	ks "github.com/whyrusleeping/go-keyspace"
)

type PeerState int

const (
	PeerSeen PeerState = iota
	PeerWaiting
	PeerQueried
	PeerUnreachable
)

type QueryPeerset struct {
	// the key being searched for
	key ks.Key

	// all known peers
	all []queryPeerState

	// sorted is true if all is currently in sorted order
	sorted bool
}

type queryPeerState struct {
	id    peer.ID
	state PeerState
}

type sortedQueryPeerset QueryPeerset

func (sqp *sortedQueryPeerset) Len() int {
	return len(sqp.all)
}

func (sqp *sortedQueryPeerset) Swap(i, j int) {
	sqp.all[i], sqp.all[j] = sqp.all[j], sqp.all[i]
}

func (sqp *sortedQueryPeerset) Less(i, j int) bool {
	di, dj := sqp.distanceToKey(i), sqp.distanceToKey(j)
	return di.Cmp(dj) == -1
}

func (sqp *sortedQueryPeerset) distanceToKey(i int) *big.Int {
	return ks.XORKeySpace.Key([]byte(sqp.all[i].id)).Distance(sqp.key)
}

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

func (qp *QueryPeerset) TryAdd(p peer.ID) bool {
	if qp.find(p) >= 0 {
		return false
	} else {
		qp.all = append(qp.all, queryPeerState{id: p, state: PeerSeen})
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

func (qp *QueryPeerset) MarkSeen(p peer.ID) {
	qp.all[qp.find(p)].state = PeerSeen
}

func (qp *QueryPeerset) MarkWaiting(p peer.ID) {
	qp.all[qp.find(p)].state = PeerWaiting
}

func (qp *QueryPeerset) MarkQueried(p peer.ID) {
	qp.all[qp.find(p)].state = PeerQueried
}

func (qp *QueryPeerset) MarkUnreachable(p peer.ID) {
	qp.all[qp.find(p)].state = PeerUnreachable
}

func (qp *QueryPeerset) IsSeen(p peer.ID) bool {
	return qp.all[qp.find(p)].state == PeerSeen
}

func (qp *QueryPeerset) IsWaiting(p peer.ID) bool {
	return qp.all[qp.find(p)].state == PeerWaiting
}

func (qp *QueryPeerset) IsQueried(p peer.ID) bool {
	return qp.all[qp.find(p)].state == PeerQueried
}

func (qp *QueryPeerset) IsUnreachable(p peer.ID) bool {
	return qp.all[qp.find(p)].state == PeerUnreachable
}

func (qp *QueryPeerset) NumWaiting() int {
	return len(qp.GetWaitingPeers())
}

func (qp *QueryPeerset) GetWaitingPeers() (result []peer.ID) {
	for _, p := range qp.all {
		if p.state == PeerWaiting {
			result = append(result, p.id)
		}
	}
	return
}

func (qp *QueryPeerset) GetClosestNotUnreachable(k int) (result []peer.ID) {
	qp.sort()
	for _, p := range qp.all {
		if p.state != PeerUnreachable {
			result = append(result, p.id)
		}
	}
	if len(result) > k {
		return result[:k]
	}
	return result
}

func (qp *QueryPeerset) NumSeen() int {
	return len(qp.GetSeenPeers())
}

func (qp *QueryPeerset) GetSeenPeers() (result []peer.ID) {
	for _, p := range qp.all {
		if p.state == PeerSeen {
			result = append(result, p.id)
		}
	}
	return
}

func (qp *QueryPeerset) GetSortedSeen() (result []peer.ID) {
	qp.sort()
	return qp.GetSeenPeers()
}
