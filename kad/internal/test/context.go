package test

import (
	"context"
	"testing"
	"time"
)

// Ctx returns a Context and a CancelFunc. The context will be
// cancelled just before the test binary deadline (as
// specified by the -timeout flag when running the test). The
// CancelFunc may be called to cancel the context earlier than
// the deadline.
func Ctx(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()

	deadline, ok := t.Deadline()
	if !ok {
		deadline = time.Now().Add(time.Minute)
	} else {
		deadline = deadline.Add(-time.Second)
	}
	return context.WithDeadline(context.Background(), deadline)
}
