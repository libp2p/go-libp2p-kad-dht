package coord

import (
	"github.com/libp2p/go-libp2p-kad-dht/v2/internal/coord/brdcst"
	"github.com/libp2p/go-libp2p-kad-dht/v2/internal/coord/query"
	"github.com/libp2p/go-libp2p-kad-dht/v2/kadt"
	"github.com/libp2p/go-libp2p-kad-dht/v2/pb"
)

// EventStartBroadcast starts a new
type EventStartBroadcast struct {
	QueryID           query.QueryID
	Target            kadt.Key
	Message           *pb.Message
	KnownClosestNodes []kadt.PeerID
	Strategy          brdcst.Strategy
	Notify            NotifyCloser[BehaviourEvent]
}

func (*EventStartBroadcast) behaviourEvent() {}

// EventBroadcastFinished is emitted by the coordinator when a broadcasting
// a record to the network has finished, either through running to completion or
// by being canceled.
type EventBroadcastFinished struct {
	QueryID   query.QueryID
	Contacted []kadt.PeerID
	Errors    map[string]struct {
		Node kadt.PeerID
		Err  error
	}
}

func (*EventBroadcastFinished) behaviourEvent() {}
