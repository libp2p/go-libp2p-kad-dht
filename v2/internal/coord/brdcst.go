package coord

import (
	"context"
	"sync"

	"github.com/libp2p/go-libp2p-kad-dht/v2/internal/coord/brdcst"

	"github.com/libp2p/go-libp2p-kad-dht/v2/internal/coord/query"
	"github.com/libp2p/go-libp2p-kad-dht/v2/tele"

	"github.com/libp2p/go-libp2p-kad-dht/v2/kadt"

	"go.opentelemetry.io/otel/trace"
	"golang.org/x/exp/slog"
)

type PooledBroadcastBehaviour struct {
	brdcst  *brdcst.Broadcast[kadt.Key, kadt.PeerID]
	waiters map[query.QueryID]NotifyCloser[BehaviourEvent]

	pendingMu sync.Mutex
	pending   []BehaviourEvent
	ready     chan struct{}

	logger *slog.Logger
	tracer trace.Tracer
}

var _ Behaviour[BehaviourEvent, BehaviourEvent] = (*PooledBroadcastBehaviour)(nil)

func NewPooledBroadcastBehaviour(brdcst *brdcst.Broadcast[kadt.Key, kadt.PeerID], logger *slog.Logger, tracer trace.Tracer) *PooledBroadcastBehaviour {
	b := &PooledBroadcastBehaviour{
		brdcst:  brdcst,
		waiters: make(map[query.QueryID]NotifyCloser[BehaviourEvent]),
		ready:   make(chan struct{}, 1),
		logger:  logger.With("behaviour", "brdcst"),
		tracer:  tracer,
	}
	return b
}

func (b *PooledBroadcastBehaviour) Ready() <-chan struct{} {
	return b.ready
}

func (b *PooledBroadcastBehaviour) Notify(ctx context.Context, ev BehaviourEvent) {
	ctx, span := b.tracer.Start(ctx, "PooledBroadcastBehaviour.Notify")
	defer span.End()

	b.pendingMu.Lock()
	defer b.pendingMu.Unlock()

	var cmd brdcst.BroadcastEvent
	switch ev := ev.(type) {
	case *EventStartBroadcast:
		cmd = &brdcst.EventBroadcastStart[kadt.Key, kadt.PeerID]{
			QueryID:           ev.QueryID,
			Target:            ev.Target,
			KnownClosestNodes: ev.KnownClosestNodes,
		}
		if ev.Notify != nil {
			b.waiters[ev.QueryID] = ev.Notify
		}

	case *EventStopQuery:
		cmd = &brdcst.EventBroadcastStopQuery{
			QueryID: ev.QueryID,
		}

	case *EventGetCloserNodesSuccess:
		for _, info := range ev.CloserNodes {
			// TODO: do this after advancing pool
			b.pending = append(b.pending, &EventAddNode{
				NodeID: info,
			})
		}
		waiter, ok := b.waiters[ev.QueryID]
		if ok {
			waiter.Notify(ctx, &EventQueryProgressed{
				NodeID:  ev.To,
				QueryID: ev.QueryID,
			})
		}

		cmd = &brdcst.EventBroadcastNodeResponse[kadt.Key, kadt.PeerID]{
			NodeID:      ev.To,
			QueryID:     ev.QueryID,
			CloserNodes: ev.CloserNodes,
		}

	case *EventGetCloserNodesFailure:
		// queue an event that will notify the routing behaviour of a failed node
		b.pending = append(b.pending, &EventNotifyNonConnectivity{
			ev.To,
		})

		cmd = &brdcst.EventBroadcastNodeFailure[kadt.Key, kadt.PeerID]{
			NodeID:  ev.To,
			QueryID: ev.QueryID,
			Error:   ev.Err,
		}
	}

	// attempt to advance the ...
	ev, ok := b.advanceBrdcst(ctx, cmd)
	if ok {
		b.pending = append(b.pending, ev)
	}
	if len(b.pending) > 0 {
		select {
		case b.ready <- struct{}{}:
		default:
		}
	}
}

func (b *PooledBroadcastBehaviour) Perform(ctx context.Context) (BehaviourEvent, bool) {
	ctx, span := b.tracer.Start(ctx, "RoutingBehaviour.Perform")
	defer span.End()

	// No inbound work can be done until Perform is complete
	b.pendingMu.Lock()
	defer b.pendingMu.Unlock()

	for {
		// drain queued events first.
		if len(b.pending) > 0 {
			var ev BehaviourEvent
			ev, b.pending = b.pending[0], b.pending[1:]

			if len(b.pending) > 0 {
				select {
				case b.ready <- struct{}{}:
				default:
				}
			}
			return ev, true
		}

		// poll the child state machines in priority order to give each an opportunity to perform work

		ev, ok := b.advanceBrdcst(ctx, &brdcst.EventBroadcastPoll{})
		if ok {
			return ev, true
		}

		// finally check if any pending events were accumulated in the meantime
		if len(b.pending) == 0 {
			return nil, false
		}
	}
}

func (b *PooledBroadcastBehaviour) advanceBrdcst(ctx context.Context, ev brdcst.BroadcastEvent) (out BehaviourEvent, term bool) {
	ctx, span := b.tracer.Start(ctx, "PooledBroadcastBehaviour.advancePool", trace.WithAttributes(tele.AttrInEvent(ev)))
	defer func() {
		span.SetAttributes(tele.AttrOutEvent(out))
		span.End()
	}()

	pstate := b.brdcst.Advance(ctx, ev)
	switch st := pstate.(type) {
	case *brdcst.StateBroadcastFindCloser:
		return &EventOutboundGetCloserNodes{
			QueryID: st.QueryID,
			To:      st.NodeID,
			Target:  st.Target,
			Notify:  b,
		}, true
	case *brdcst.StateBroadcastFinished:
		waiter, ok := b.waiters[st.QueryID]
		if ok {
			waiter.Notify(ctx, &EventBroadcastFinished{
				QueryID: st.QueryID,
				Stats:   st.Stats,
			})
			waiter.Close()
		}
	}

	return nil, false
}
