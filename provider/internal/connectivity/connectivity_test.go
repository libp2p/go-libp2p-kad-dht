package connectivity

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/filecoin-project/go-clock"
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
	var called int32
	c, err := New(
		func() bool { atomic.AddInt32(&called, 1); return true },
		func() {},
		WithClock(clock.NewMock()),
		WithOnlineCheckInterval(time.Minute),
		WithOfflineCheckInterval(time.Minute),
	)
	if err != nil {
		t.Fatalf("failed to create ConnectivityChecker: %v", err)
	}
	defer c.Close()

	c.online.Store(true)

	c.TriggerCheck() // should perform the check
	time.Sleep(5 * time.Millisecond)
	c.TriggerCheck() // should return early

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
	mockClk := clock.NewMock()

	var calls, notified int32
	c, err := New(
		func() bool { atomic.AddInt32(&calls, 1); return true },
		func() { atomic.AddInt32(&notified, 1) },
		WithClock(mockClk),
		WithOnlineCheckInterval(time.Minute),
		WithOfflineCheckInterval(time.Minute),
	)
	if err != nil {
		t.Fatalf("failed to create ConnectivityChecker: %v", err)
	}
	defer c.Close()                                   // ensure cleanup
	c.lastCheck = mockClk.Now().Add(-2 * time.Minute) // new check can be triggered

	c.TriggerCheck()
	eventually(t, func() bool { return atomic.LoadInt32(&calls) == 1 })

	if !c.IsOnline() {
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
	mockClk := clock.NewMock()
	var online atomic.Bool // represents real connectivity
	var notified int32
	done := make(chan struct{})

	online.Store(false) // start offline
	c, err := New(
		func() bool { return online.Load() },
		func() { atomic.AddInt32(&notified, 1); close(done) },
		WithClock(mockClk),
		WithOnlineCheckInterval(time.Minute),
		WithOfflineCheckInterval(time.Minute),
	)
	if err != nil {
		t.Fatalf("failed to create ConnectivityChecker: %v", err)
	}
	defer c.Close()                                   // ensure cleanup
	c.lastCheck = mockClk.Now().Add(-2 * time.Minute) // new check can be triggered
	c.TriggerCheck()                                  // launches goroutine

	// First offline tick (still disconnected).
	time.Sleep(1 * time.Millisecond)
	if c.IsOnline() {
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

	if !c.IsOnline() {
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

//  4. Check whether the ConnectivityChecker adapts when the node goes offline
//     and online again
func TestOnlineOffline(t *testing.T) {
	online := atomic.Bool{}
	checkFuncCalled := atomic.Bool{}
	mockClock := clock.NewMock()
	checkInterval := time.Minute
	notified := make(chan struct{}, 1)
	c, err := New(
		func() bool { checkFuncCalled.Store(true); return online.Load() },
		func() { notified <- struct{}{} },
		WithClock(mockClock),
		WithOnlineCheckInterval(checkInterval),
		WithOfflineCheckInterval(checkInterval),
	)
	if err != nil {
		t.Fatalf("failed to create ConnectivityChecker: %v", err)
	}

	checked := func() bool {
		return checkFuncCalled.Load()
	}
	nodeOnline := func() bool {
		return c.IsOnline()
	}
	nodeOffline := func() bool {
		return !c.IsOnline()
	}

	// online -> online
	online.Store(true) // node starts online
	c.TriggerCheck()
	eventually(t, checked)
	eventually(t, nodeOnline)
	if len(notified) != 0 {
		t.Fatal("notified should be empty")
	}

	// online -> offline
	checkFuncCalled.Store(false)
	mockClock.Add(checkInterval)
	online.Store(false) // simulate going offline

	c.TriggerCheck()
	eventually(t, checked)
	eventually(t, nodeOffline)
	if len(notified) != 0 {
		t.Fatal("notified should be empty")
	}

	// TriggerCheck is noop while offline
	checkFuncCalled.Store(false)
	c.TriggerCheck()
	time.Sleep(5 * time.Millisecond)
	if checkFuncCalled.Load() {
		t.Fatal("TriggerCheck should be no-op while offline")
	}

	// offline -> offline
	mockClock.Add(checkInterval)
	eventually(t, checked)
	eventually(t, nodeOffline)

	// offline -> online
	checkFuncCalled.Store(false)
	online.Store(true) // simulate coming back online
	mockClock.Add(checkInterval)

	eventually(t, checked)
	eventually(t, nodeOnline)
	if len(notified) != 1 {
		t.Fatal("notified should have one element")
	}
}

// 5. Check ConnectivityChecker behaviour after closing it.
func TestClose(t *testing.T) {
	online := atomic.Bool{}
	checkFuncCalled := atomic.Bool{}
	mockClock := clock.NewMock()
	checkInterval := time.Minute
	notified := make(chan struct{}, 1)
	c, err := New(
		func() bool { checkFuncCalled.Store(true); return online.Load() },
		func() { notified <- struct{}{} },
		WithClock(mockClock),
		WithOnlineCheckInterval(checkInterval),
		WithOfflineCheckInterval(checkInterval),
	)
	if err != nil {
		t.Fatalf("failed to create ConnectivityChecker: %v", err)
	}

	checked := func() bool {
		return checkFuncCalled.Load()
	}
	nodeOffline := func() bool {
		return !c.IsOnline()
	}

	// Node is offline
	c.TriggerCheck()
	eventually(t, checked)
	eventually(t, nodeOffline)

	// Close connectivity checker
	checkFuncCalled.Store(false)
	c.Close()
	mockClock.Add(2 * checkInterval)
	time.Sleep(5 * time.Millisecond)
	if checkFuncCalled.Load() {
		t.Fatal("checkFunc should not be called after Close")
	}

	// TriggerCheck is noop after Close
	c.TriggerCheck()
	mockClock.Add(2 * checkInterval)
	time.Sleep(5 * time.Millisecond)
	if checkFuncCalled.Load() {
		t.Fatal("checkFunc should not be called after Close")
	}
}

// 6. Try to build a ConnectivityChecker with invalid options
func TestInvalidOptions(t *testing.T) {
	// Negative OnlineCheckInterval
	_, err := New(
		func() bool { return true },
		func() {},
		WithOnlineCheckInterval(-time.Minute),
	)
	if err == nil {
		t.Fatal("expected error for negative OnlineCheckInterval")
	}
	_, err = New(
		func() bool { return true },
		func() {},
		WithOfflineCheckInterval(0*time.Second),
	)
	if err == nil {
		t.Fatal("expected error for zero OfflineCheckInterval")
	}
}
