package dht

import (
	"context"
	"sync"

	"github.com/google/uuid"

	"github.com/libp2p/go-libp2p-core/peer"
	kbucket "github.com/libp2p/go-libp2p-kbucket"
)

// LookupEvent is emitted for every notable event that happens during a DHT lookup.
// LookupEvent supports JSON marshalling because all of its fields do, recursively.
type LookupEvent struct {
	// ID is a unique identifier for the lookup instance
	ID uuid.UUID
	// Key is the Kademlia key used as a lookup target.
	Key kbucket.ID
	// Request, if not nil, describes a state update event, associated with an outgoing query request.
	Request *LookupUpdateEvent
	// Response, if not nil, describes a state update event, associated with an outgoing query response.
	Response *LookupUpdateEvent
	// Terminate, if not nil, describe a termination event.
	Terminate *LookupTerminateEvent
}

// LookupUpdateEvent describes a lookup state update event.
type LookupUpdateEvent struct {
	// Cause is the peer whose response (or lack of response) caused the update event.
	// If Cause is nil, this is the first update event in the lookup, caused by the seeding.
	Cause peer.ID
	// Source is the peer who informed us about the peer IDs in this update (below).
	Source peer.ID
	// Heard is a set of peers whose state in the lookup's peerset is being set to "heard".
	Heard []peer.ID
	// Waiting is a set of peers whose state in the lookup's peerset is being set to "waiting".
	Waiting []peer.ID
	// Queried is a set of peers whose state in the lookup's peerset is being set to "queried".
	Queried []peer.ID
	// Unreachable is a set of peers whose state in the lookup's peerset is being set to "unreachable".
	Unreachable []peer.ID
}

// LookupTerminateEvent describes a lookup termination event.
type LookupTerminateEvent struct {
	// Reason is the reason for lookup termination.
	Reason LookupTerminationReason
}

// LookupTerminationReason captures reasons for terminating a lookup.
type LookupTerminationReason int

const (
	// LookupStopped indicates that the lookup was aborted by the user's stopFn.
	LookupStopped LookupTerminationReason = iota
	// LookupCancelled indicates that the lookup was aborted by the context.
	LookupCancelled
	// LookupStarvation indicates that the lookup terminated due to lack of unqueried peers.
	LookupStarvation
	// LookupCompleted indicates that the lookup terminated successfully, reaching the Kademlia end condition.
	LookupCompleted
)

type routingLookupKey struct{}

// TODO: lookupEventChannel copies the implementation of eventChanel.
// The two should be refactored to use a common event channel implementation.
// A common implementation needs to rethink the signature of RegisterForEvents,
// because returning a typed channel cannot be made polymorphic without creating
// additional "adapter" channels. This will be easier to handle when Go
// introduces generics.
type lookupEventChannel struct {
	mu  sync.Mutex
	ctx context.Context
	ch  chan<- *LookupEvent
}

// waitThenClose is spawned in a goroutine when the channel is registered. This
// safely cleans up the channel when the context has been canceled.
func (e *lookupEventChannel) waitThenClose() {
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
func (e *lookupEventChannel) send(ctx context.Context, ev *LookupEvent) {
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

// RegisterForLookupEvents registers a lookup event channel with the given context.
// The returned context can be passed to DHT queries to receive lookup events on
// the returned channels.
//
// The passed context MUST be canceled when the caller is no longer interested
// in query events.
func RegisterForLookupEvents(ctx context.Context) (context.Context, <-chan *LookupEvent) {
	ch := make(chan *LookupEvent, LookupEventBufferSize)
	ech := &lookupEventChannel{ch: ch, ctx: ctx}
	go ech.waitThenClose()
	return context.WithValue(ctx, routingLookupKey{}, ech), ch
}

// Number of events to buffer.
var LookupEventBufferSize = 16

// PublishLookupEvent publishes a query event to the query event channel
// associated with the given context, if any.
func PublishLookupEvent(ctx context.Context, ev *LookupEvent) {
	ich := ctx.Value(routingLookupKey{})
	if ich == nil {
		return
	}

	// We *want* to panic here.
	ech := ich.(*lookupEventChannel)
	ech.send(ctx, ev)
}
