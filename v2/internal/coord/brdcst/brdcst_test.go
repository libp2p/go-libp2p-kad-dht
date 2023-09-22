package brdcst

import (
	"testing"

	"github.com/libp2p/go-libp2p-kad-dht/v2/internal/coord/internal/tiny"
)

func TestBroadcastState_interface_conformance(t *testing.T) {
	states := []BroadcastState{
		&StateBroadcastIdle{},
		&StateBroadcastWaiting{},
		&StateBroadcastStoreRecord[tiny.Key, tiny.Node, tiny.Message]{},
		&StateBroadcastFindCloser[tiny.Key, tiny.Node]{},
		&StateBroadcastFinished[tiny.Key, tiny.Node]{},
	}
	for _, st := range states {
		st.broadcastState() // drives test coverage
	}
}

func TestBroadcastEvent_interface_conformance(t *testing.T) {
	events := []BroadcastEvent{
		&EventBroadcastStop{},
		&EventBroadcastPoll{},
		&EventBroadcastStart[tiny.Key, tiny.Node]{},
		&EventBroadcastNodeResponse[tiny.Key, tiny.Node]{},
		&EventBroadcastNodeFailure[tiny.Key, tiny.Node]{},
		&EventBroadcastStoreRecordSuccess[tiny.Key, tiny.Node, tiny.Message]{},
		&EventBroadcastStoreRecordFailure[tiny.Key, tiny.Node, tiny.Message]{},
	}
	for _, ev := range events {
		ev.broadcastEvent() // drives test coverage
	}
}
