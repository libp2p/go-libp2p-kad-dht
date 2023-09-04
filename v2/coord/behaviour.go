package coord

import (
	"context"
	"sync"
	"sync/atomic"
)

type Notify[C DhtEvent] interface {
	Notify(ctx context.Context, ev C)
}

type NotifyCloser[C DhtEvent] interface {
	Notify[C]
	Close()
}

type NotifyFunc[C DhtEvent] func(ctx context.Context, ev C)

func (f NotifyFunc[C]) Notify(ctx context.Context, ev C) {
	f(ctx, ev)
}

type Behaviour[I DhtEvent, O DhtEvent] interface {
	// Ready returns a channel that signals when the behaviour is ready to perform work.
	Ready() <-chan struct{}

	// Notify informs the behaviour of an event. The behaviour may perform the event
	// immediately and queue the result, causing the behaviour to become ready.
	// It is safe to call Notify from the Perform method.
	Notify(ctx context.Context, ev I)

	// Perform gives the behaviour the opportunity to perform work or to return a queued
	// result as an event.
	Perform(ctx context.Context) (O, bool)
}

type SM[E any, S any] interface {
	Advance(context.Context, E) S
}

type WorkQueueFunc[E DhtEvent] func(context.Context, E) bool

// WorkQueue is buffered queue of work to be performed.
// The queue automatically drains the queue sequentially by calling a
// WorkQueueFunc for each work item, passing the original context
// and event.
type WorkQueue[E DhtEvent] struct {
	pending chan pendingEvent[E]
	fn      WorkQueueFunc[E]
	done    atomic.Bool
	once    sync.Once
}

func NewWorkQueue[E DhtEvent](fn WorkQueueFunc[E]) *WorkQueue[E] {
	w := &WorkQueue[E]{
		pending: make(chan pendingEvent[E], 16),
		fn:      fn,
	}
	return w
}

type pendingEvent[E any] struct {
	Ctx   context.Context
	Event E
}

// Enqueue queues work to be perfomed. It will block if the
// queue has reached its maximum capacity for pending work. While
// blocking it will return a context cancellation error if the work
// item's context is cancelled.
func (w *WorkQueue[E]) Enqueue(ctx context.Context, cmd E) error {
	if w.done.Load() {
		return nil
	}
	w.once.Do(func() {
		go func() {
			defer w.done.Store(true)
			for cc := range w.pending {
				if cc.Ctx.Err() != nil {
					return
				}
				if done := w.fn(cc.Ctx, cc.Event); done {
					w.done.Store(true)
					return
				}
			}
		}()
	})

	select {
	case <-ctx.Done(): // this is the context for the work item
		return ctx.Err()
	case w.pending <- pendingEvent[E]{
		Ctx:   ctx,
		Event: cmd,
	}:
		return nil

	}
}

// A Waiter is a Notifiee whose Notify method forwards the
// notified event to a channel which a client can wait on.
type Waiter[E DhtEvent] struct {
	pending chan WaiterEvent[E]
	done    atomic.Bool
}

var _ Notify[DhtEvent] = (*Waiter[DhtEvent])(nil)

func NewWaiter[E DhtEvent]() *Waiter[E] {
	w := &Waiter[E]{
		pending: make(chan WaiterEvent[E], 16),
	}
	return w
}

type WaiterEvent[E DhtEvent] struct {
	Ctx   context.Context
	Event E
}

func (w *Waiter[E]) Notify(ctx context.Context, ev E) {
	if w.done.Load() {
		return
	}
	select {
	case <-ctx.Done(): // this is the context for the work item
		return
	case w.pending <- WaiterEvent[E]{
		Ctx:   ctx,
		Event: ev,
	}:
		return

	}
}

// Close signals that the waiter should not forward and further calls to Notify.
// It closes the waiter channel so a client selecting on it will receive the close
// operation.
func (w *Waiter[E]) Close() {
	w.done.Store(true)
	close(w.pending)
}

func (w *Waiter[E]) Chan() <-chan WaiterEvent[E] {
	return w.pending
}
