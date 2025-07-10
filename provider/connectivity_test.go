package provider

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	clk "github.com/filecoin-project/go-clock"
)

// tiny helper: spin-wait (≤ real-time 100 ms) until fn() is true or t.Fatal.
func eventually(t *testing.T, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(100 * time.Millisecond)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition not reached in time")
}

//  1. If the last check was too recent, triggerCheck must return early and never
//     call checkFunc.
func TestTriggerCheck_SkipsWhenRecent(t *testing.T) {
	mockClk := clk.NewMock()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var called int32
	c := &connectivityChecker{
		ctx:                  ctx,
		clock:                mockClk,
		onlineCheckInterval:  time.Minute,
		offlineCheckInterval: time.Minute,
		checkFunc:            func(context.Context) bool { atomic.AddInt32(&called, 1); return true },
		backOnlineNotify:     func() {},
	}
	c.online.Store(true)

	c.triggerCheck() // should perform the check
	time.Sleep(5 * time.Millisecond)
	c.triggerCheck() // should return early

	// Give the goroutine a chance to run.
	time.Sleep(5 * time.Millisecond)
	if got := atomic.LoadInt32(&called); got != 1 {
		t.Fatalf("checkFunc called %d times; expected 1", got)
	}
}

//  2. While online and past onlineCheckInterval, triggerCheck should:
//     – run checkFunc exactly once
//     – keep the node marked online
//     – NOT fire backOnlineNotify.
func TestTriggerCheck_OnlineFastPath(t *testing.T) {
	mockClk := clk.NewMock()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var calls, notified int32
	c := &connectivityChecker{
		ctx:                  ctx,
		clock:                mockClk,
		onlineCheckInterval:  time.Minute,
		offlineCheckInterval: time.Minute,
		checkFunc:            func(context.Context) bool { atomic.AddInt32(&calls, 1); return true },
		backOnlineNotify:     func() { atomic.AddInt32(&notified, 1) },
	}
	c.online.Store(true)
	c.lastCheck = mockClk.Now().Add(-2 * time.Minute) // new check can be triggered

	c.triggerCheck()
	eventually(t, func() bool { return atomic.LoadInt32(&calls) == 1 })

	if !c.isOnline() {
		t.Fatal("node should still be online")
	}
	if atomic.LoadInt32(&notified) != 0 {
		t.Fatal("backOnlineNotify must NOT fire on an already-online node")
	}
}

//  3. If the first check finds the node offline, connectivityChecker must:
//     – mark the node offline
//     – poll every offlineCheckInterval
//     – once checkFunc returns true, mark online again
//     – invoke backOnlineNotify exactly once and then stop.
func TestTriggerCheck_OfflineRecovery(t *testing.T) {
	mockClk := clk.NewMock()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var online atomic.Bool // represents real connectivity
	var notified int32
	done := make(chan struct{})

	online.Store(false) // start offline
	c := &connectivityChecker{
		ctx:                  ctx,
		clock:                mockClk,
		onlineCheckInterval:  time.Minute,
		offlineCheckInterval: time.Minute,
		checkFunc:            func(context.Context) bool { return online.Load() },
		backOnlineNotify: func() {
			atomic.AddInt32(&notified, 1)
			close(done)
		},
	}
	c.online.Store(true)                              // previously online
	c.lastCheck = mockClk.Now().Add(-2 * time.Minute) // check can be triggered
	c.triggerCheck()                                  // launches goroutine

	// First offline tick (still disconnected).
	time.Sleep(1 * time.Millisecond)
	if c.isOnline() {
		t.Fatal("node should be marked offline after failing first check")
	}

	// Bring the node back online, next tick should succeed.
	online.Store(true)
	mockClk.Add(time.Minute)

	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("backOnlineNotify never fired")
	}

	if !c.isOnline() {
		t.Fatal("node should be back online")
	}
	if n := atomic.LoadInt32(&notified); n != 1 {
		t.Fatalf("backOnlineNotify fired %d times; expected exactly once", n)
	}

	// Advance more time; the loop must have exited, so no further notifications.
	mockClk.Add(5 * time.Minute)
	time.Sleep(1 * time.Millisecond)
	if n := atomic.LoadInt32(&notified); n != 1 {
		t.Fatalf("backOnlineNotify fired again (%d times)", n)
	}
}
