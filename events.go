package dht

import (
	"context"
	"sync"

	"github.com/google/uuid"

	"github.com/libp2p/go-libp2p-core/peer"
	kbucket "github.com/libp2p/go-libp2p-kbucket"
)

// AsyncEvent is emitted for every notable event that happens during a DHT async lookup.
// AsyncEvent supports JSON marshalling because all of its fields do, recursively.
type AsyncEvent struct {
	// ID is a unique identifier for the lookup instance
	ID uuid.UUID
	// Key is the Kademlia key used as a lookup target.
	Key kbucket.ID
	// Update, if not nil, describes a state update event.
	Update *AsyncUpdateEvent
	// Terminate, if not nil, describe a termination event.
	Terminate *AsyncTerminateEvent
}

// AsyncUpdateEvent describes a lookup state update event.
type AsyncUpdateEvent struct {
	// Cause is the peer whose response (or lack of response) caused the update event.
	// If Cause is nil, this is the first update event in the lookup, caused by the seeding.
	Cause peer.ID
	// Heard is a set of peers whose state in the lookup's peerset is being set to "heard".
	Heard []peer.ID
	// Waiting is a set of peers whose state in the lookup's peerset is being set to "waiting".
	Waiting []peer.ID
	// Queried is a set of peers whose state in the lookup's peerset is being set to "queried".
	Queried []peer.ID
	// Unreachable is a set of peers whose state in the lookup's peerset is being set to "unreachable".
	Unreachable []peer.ID
}

// AsyncTerminateEvent describes a lookup termination event.
type AsyncTerminateEvent struct {
	// Reason is the reason for lookup termination.
	Reason AsyncTerminationReason
}

// AsyncTerminationReason captures reasons for terminating a lookup.
type AsyncTerminationReason int

const (
	// AsyncStopped indicates that the lookup was aborted by the user's stopFn.
	AsyncStopped AsyncTerminationReason = iota
	// AsyncCancelled indicates that the lookup was aborted by the context.
	AsyncCancelled
	// AsyncStarvation indicates that the lookup terminated due to lack of unqueried peers.
	AsyncStarvation
	// AsyncCompleted indicates that the lookup terminated successfully, reaching the Kademlia end condition.
	AsyncCompleted
)

type routingAsyncKey struct{}

// TODO: asyncEventChannel copies the implementation of eventChanel.
// The two should be refactored to use a common event channel implementation.
// A common implementation needs to rethink the signature of RegisterForEvents,
// because returning a typed channel cannot be made polymorphic without creating
// additional "adapter" channels. This will be easier to handle when Go
// introduces generics.
type asyncEventChannel struct {
	mu  sync.Mutex
	ctx context.Context
	ch  chan<- *AsyncEvent
}

// waitThenClose is spawned in a goroutine when the channel is registered. This
// safely cleans up the channel when the context has been canceled.
func (e *asyncEventChannel) waitThenClose() {
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
func (e *asyncEventChannel) send(ctx context.Context, ev *AsyncEvent) {
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

// RegisterForAsyncEvents registers an async lookup event channel with the given
// context. The returned context can be passed to DHT queries to receive async lookup
// events on the returned channels.
//
// The passed context MUST be canceled when the caller is no longer interested
// in query events.
func RegisterForAsyncEvents(ctx context.Context) (context.Context, <-chan *AsyncEvent) {
	ch := make(chan *AsyncEvent, AsyncEventBufferSize)
	ech := &asyncEventChannel{ch: ch, ctx: ctx}
	go ech.waitThenClose()
	return context.WithValue(ctx, routingAsyncKey{}, ech), ch
}

// Number of events to buffer.
var AsyncEventBufferSize = 16

// PublishAsyncEvent publishes a query event to the query event channel
// associated with the given context, if any.
func PublishAsyncEvent(ctx context.Context, ev *AsyncEvent) {
	ich := ctx.Value(routingAsyncKey{})
	if ich == nil {
		return
	}

	// We *want* to panic here.
	ech := ich.(*asyncEventChannel)
	ech.send(ctx, ev)
}
