package coord

import (
	"context"
)

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
