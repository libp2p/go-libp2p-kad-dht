package kadtest

import (
	"context"
	"testing"
	"time"
)

// CtxShort returns a Context and a CancelFunc. The context will be
// cancelled after 10 seconds or just before the test binary deadline (as
// specified by the -timeout flag when running the test), whichever is
// sooner. The CancelFunc may be called to cancel the context earlier than
// the deadline.
func CtxShort(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()

	timeout := 10 * time.Second
	goal := time.Now().Add(timeout)

	deadline, ok := t.Deadline()
	if !ok {
		deadline = goal
	} else {
		deadline = deadline.Add(-time.Second)
		if deadline.After(goal) {
			deadline = goal
		}
	}
	return context.WithDeadline(context.Background(), deadline)
}
