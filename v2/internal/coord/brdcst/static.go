package brdcst

import (
	"context"
	"fmt"

	"github.com/plprobelab/go-kademlia/kad"
	"go.opentelemetry.io/otel/trace"

	"github.com/libp2p/go-libp2p-kad-dht/v2/internal/coord/coordt"
	"github.com/libp2p/go-libp2p-kad-dht/v2/tele"
)

// Static is a [Broadcast] state machine and encapsulates the logic around
// doing a put operation to a static set of nodes. That static set of nodes
// is given by the list of seed nodes in the [EventBroadcastStart] event.
type Static[K kad.Key[K], N kad.NodeID[K], M coordt.Message] struct {
	// the unique ID for this broadcast operation
	queryID coordt.QueryID

	// a struct holding configuration options
	cfg *ConfigStatic

	// the message that we will send to the closest nodes in the follow-up phase
	msg M

	// nodes we still need to store records with. This map will be filled with
	// all the closest nodes after the query has finished.
	todo map[string]N

	// nodes we have contacted to store the record but haven't heard a response yet
	waiting map[string]N

	// nodes that successfully hold the record for us
	success map[string]N

	// nodes that failed to hold the record for us
	failed map[string]struct {
		Node N
		Err  error
	}
}

// NewStatic initializes a new [Static] struct.
func NewStatic[K kad.Key[K], N kad.NodeID[K], M coordt.Message](qid coordt.QueryID, msg M, cfg *ConfigStatic) *Static[K, N, M] {
	return &Static[K, N, M]{
		queryID: qid,
		cfg:     cfg,
		msg:     msg,
		todo:    map[string]N{},
		waiting: map[string]N{},
		success: map[string]N{},
		failed: map[string]struct {
			Node N
			Err  error
		}{},
	}
}

// Advance advances the state of the [Static] [Broadcast] state machine.
func (f *Static[K, N, M]) Advance(ctx context.Context, ev BroadcastEvent) (out BroadcastState) {
	ctx, span := tele.StartSpan(ctx, "Static.Advance", trace.WithAttributes(tele.AttrInEvent(ev)))
	defer func() {
		span.SetAttributes(tele.AttrOutEvent(out))
		span.End()
	}()

	switch ev := ev.(type) {
	case *EventBroadcastStart[K, N]:
		for _, seed := range ev.Seed {
			f.todo[seed.String()] = seed
		}
	case *EventBroadcastStop:
		for _, n := range f.todo {
			delete(f.todo, n.String())
			f.failed[n.String()] = struct {
				Node N
				Err  error
			}{Node: n, Err: fmt.Errorf("cancelled")}
		}

		for _, n := range f.waiting {
			delete(f.waiting, n.String())
			f.failed[n.String()] = struct {
				Node N
				Err  error
			}{Node: n, Err: fmt.Errorf("cancelled")}
		}
	case *EventBroadcastStoreRecordSuccess[K, N, M]:
		delete(f.waiting, ev.NodeID.String())
		f.success[ev.NodeID.String()] = ev.NodeID
	case *EventBroadcastStoreRecordFailure[K, N, M]:
		delete(f.waiting, ev.NodeID.String())
		f.failed[ev.NodeID.String()] = struct {
			Node N
			Err  error
		}{Node: ev.NodeID, Err: ev.Error}
	case *EventBroadcastPoll:
		// ignore, nothing to do
	default:
		panic(fmt.Sprintf("unexpected event: %T", ev))
	}

	for k, n := range f.todo {
		delete(f.todo, k)
		f.waiting[k] = n
		return &StateBroadcastStoreRecord[K, N, M]{
			QueryID: f.queryID,
			NodeID:  n,
			Message: f.msg,
		}
	}

	if len(f.waiting) > 0 {
		return &StateBroadcastWaiting{}
	}

	if len(f.todo) == 0 {
		contacted := make([]N, 0, len(f.success)+len(f.failed))
		for _, n := range f.success {
			contacted = append(contacted, n)
		}
		for _, n := range f.failed {
			contacted = append(contacted, n.Node)
		}

		return &StateBroadcastFinished[K, N]{
			QueryID:   f.queryID,
			Contacted: contacted,
			Errors:    f.failed,
		}
	}

	return &StateBroadcastIdle{}
}
