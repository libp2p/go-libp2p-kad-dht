package dht

import (
	"context"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p-core/peer"
)

func NewRoutingTableEvent(
	peerUpdate *RoutingTablePeerUpdatedEvent,
	bucketEvt *BucketRefreshLaunchedEvent,
) *RoutingTableEvent {
	return &RoutingTableEvent{
		PeerUpdated:           peerUpdate,
		BucketRefreshLaunched: bucketEvt,
	}
}

// RoutingTableEvent is emitted for every notable event that happens to the routing table.
// RoutingTableEvent supports JSON marshalling because all of its fields do, recursively.
type RoutingTableEvent struct {
	// PeerUpdated, if not nil, describes a peer's state in the routing table being modified
	PeerUpdated *RoutingTablePeerUpdatedEvent
	// BucketRefreshLaunched, if not nil, describes a bucket refresh being attempted
	BucketRefreshLaunched *BucketRefreshLaunchedEvent
}

func NewRoutingTablePeerUpdatedEvent(
	added []peer.ID,
	removed []peer.ID,
	updated []*RTPeerScoreUpdate,
) *RoutingTablePeerUpdatedEvent {
	return &RoutingTablePeerUpdatedEvent{
		Added:   NewPeerKadIDSlice(added),
		Removed: NewPeerKadIDSlice(removed),
		Updated: updated,
	}
}

// RoutingTablePeerUpdatedEvent describes a peer being added to the routing table
type RoutingTablePeerUpdatedEvent struct {
	// Added is a set of peers whose are being added to the routing table
	Added []*PeerKadID
	// Removed is a set of peers whose are being removed from the routing table
	Removed []*PeerKadID
	// Updated is a set of peer whose routing table scores are being updated
	Updated []*RTPeerScoreUpdate
}

type RTPeerScoreUpdate struct {
	Peer  *PeerKadID
	Score time.Time
}

func NewRTPeerScoreUpdate(p peer.ID, score time.Time) *RTPeerScoreUpdate {
	return &RTPeerScoreUpdate{Peer: NewPeerKadID(p), Score: score}
}

// BucketRefreshLaunchedEvent describes a bucket refresh event.
type BucketRefreshLaunchedEvent struct {
	// CPL is the common prefix length of the bucket being refreshed
	CPL int
	// Skipped is whether the refresh is being skipped
	Skipped bool
}

func NewBucketRefreshLaunchedEvent(cpl int, skipped bool) *BucketRefreshLaunchedEvent {
	return &BucketRefreshLaunchedEvent{CPL: cpl, Skipped: skipped}
}

type rtEventKey struct{}

// TODO: rtEventChannel copies the implementation of eventChanel.
// The two should be refactored to use a common event channel implementation.
// A common implementation needs to rethink the signature of RegisterForEvents,
// because returning a typed channel cannot be made polymorphic without creating
// additional "adapter" channels. This will be easier to handle when Go
// introduces generics.
type rtEventChannel struct {
	mu  sync.Mutex
	ctx context.Context
	ch  chan<- *RoutingTableEvent
}

// waitThenClose is spawned in a goroutine when the channel is registered. This
// safely cleans up the channel when the context has been canceled.
func (e *rtEventChannel) waitThenClose() {
	<-e.ctx.Done()
	e.mu.Lock()
	close(e.ch)
	// 1. Signals that we're done.
	// 2. Frees memory (in case we end up hanging on to this for a while).
	e.ch = nil
	e.mu.Unlock()
}

// send sends an event on the event channel, aborting if either the passed or
// the internal context expire.
func (e *rtEventChannel) send(ctx context.Context, ev *RoutingTableEvent) {
	e.mu.Lock()
	// Closed.
	if e.ch == nil {
		e.mu.Unlock()
		return
	}
	// in case the passed context is unrelated, wait on both.
	select {
	case e.ch <- ev:
	case <-e.ctx.Done():
	case <-ctx.Done():
	}
	e.mu.Unlock()
}

// RegisterForRoutingTableEvents registers a routing table event channel with the given context.
// The returned context can be passed in when creating a DHT to receive routing table events on
// the returned channels.
//
// The passed context MUST be canceled when the caller is no longer interested
// in query events.
func RegisterForRoutingTableEvents(ctx context.Context) (context.Context, <-chan *RoutingTableEvent) {
	ch := make(chan *RoutingTableEvent, RoutingTableEventBufferSize)
	ech := &rtEventChannel{ch: ch, ctx: ctx}
	go ech.waitThenClose()
	return context.WithValue(ctx, rtEventKey{}, ech), ch
}

// Number of events to buffer.
var RoutingTableEventBufferSize = 16

// PublishRoutingTableEvent publishes a query event to the query event channel
// associated with the given context, if any.
func PublishRoutingTableEvent(ctx context.Context, ev *RoutingTableEvent) {
	ich := ctx.Value(rtEventKey{})
	if ich == nil {
		return
	}

	// We *want* to panic here.
	ech := ich.(*rtEventChannel)
	ech.send(ctx, ev)
}
