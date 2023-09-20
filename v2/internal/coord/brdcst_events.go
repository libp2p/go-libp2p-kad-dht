package coord

import (
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
	Notify            NotifyCloser[BehaviourEvent]
}

func (*EventStartBroadcast) behaviourEvent() {}
