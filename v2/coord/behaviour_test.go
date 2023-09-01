package kademlia

import (
	"context"
	"fmt"
	"reflect"
	"testing"
)

type NullSM[E any, S any] struct{}

func (NullSM[E, S]) Advance(context.Context, E) S {
	var v S
	return v
}

type RecordingSM[E any, S any] struct {
	State    S
	Received E
}

func NewRecordingSM[E any, S any](response S) *RecordingSM[E, S] {
	return &RecordingSM[E, S]{
		State: response,
	}
}

func (r *RecordingSM[E, S]) Advance(ctx context.Context, e E) S {
	r.Received = e
	return r.State
}

// expectBehaviourEvent selects on a behaviour's ready channel until it becomes ready and then checks the perform
// mehtod for the expected event type. Unexpected events are ignored and selecting resumes.
// The function returns when an event matching the type of expected is received or when the context is cancelled.
func expectBehaviourEvent[I DhtEvent, O DhtEvent](t *testing.T, ctx context.Context, b Behaviour[I, O], expected O) (O, error) {
	t.Helper()
	for {
		select {
		case <-b.Ready():
			ev, ok := b.Perform(ctx)
			if !ok {
				continue
			}
			t.Logf("saw event: %T\n", ev)
			if reflect.TypeOf(ev) == reflect.TypeOf(expected) {
				return ev, nil
			}
		case <-ctx.Done():
			var v O
			return v, fmt.Errorf("test deadline exceeded")
		}
	}
}
