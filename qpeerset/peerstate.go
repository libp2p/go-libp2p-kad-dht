package qpeerset

import (
	"fmt"
	"math/big"

	"github.com/libp2p/go-libp2p-core/peer"
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
	default:
		panic(fmt.Sprintf("unknown PeerState %d", ps))
	}
}

type queryPeerState struct {
	id         peer.ID
	distance   *big.Int
	state      PeerState
	referredBy peer.ID
}

type sortedQueryPeerset SingleQueryPeerset

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

// intersPeerState is the state of a peer within the intersection of multiple queries.
type intersPeerState struct {
	ID       peer.ID
	Distance *big.Int
	State    PeerState
}

func (ips *intersPeerState) String() string {
	return fmt.Sprintf("%s (%s)", ips.ID.ShortString(), ips.State)
}
