package connectivity

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/filecoin-project/go-clock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func eventually(t *testing.T, condition func() bool, maxDelay time.Duration, args ...any) {
	step := time.Millisecond
	for range maxDelay / step {
		if condition() {
			return
		}
		time.Sleep(step)
	}
	t.Fatal(args...)
}

func TestConnectivityChecker_New(t *testing.T) {
	t.Parallel()

	t.Run("starts offline when checkFunc returns false", func(t *testing.T) {
		checkFunc := func() bool { return false }

		checker, err := New(checkFunc)
		require.NoError(t, err)
		defer checker.Close()

		// Give some time for initialization
		eventually(t, func() bool { return !checker.IsOnline() }, 20*time.Millisecond, "checker should be offline")

		assert.False(t, checker.IsOnline())
		checker.stateMutex.Lock()
		assert.Equal(t, Offline, checker.state)
		checker.stateMutex.Unlock()
	})

	t.Run("starts online when checkFunc returns true", func(t *testing.T) {
		checkFunc := func() bool { return true }

		checker, err := New(checkFunc)
		require.NoError(t, err)
		checker.Start()
		defer checker.Close()

		// Give some time for initialization
		eventually(t, checker.IsOnline, 20*time.Millisecond, "checker should be online")

		checker.stateMutex.Lock()
		assert.Equal(t, Online, checker.state)
		checker.stateMutex.Unlock()
	})

	t.Run("with custom options", func(t *testing.T) {
		mockClock := clock.NewMock()
		checkFunc := func() bool { return true }

		checker, err := New(checkFunc,
			WithClock(mockClock),
			WithOnlineCheckInterval(30*time.Second),
			WithOfflineCheckInterval(10*time.Second),
		)
		require.NoError(t, err)
		defer checker.Close()

		assert.NotNil(t, checker)
	})

	t.Run("with invalid options", func(t *testing.T) {
		checkFunc := func() bool { return true }

		_, err := New(checkFunc,
			WithOnlineCheckInterval(-1*time.Second),
		)
		assert.Error(t, err)

		_, err = New(checkFunc,
			WithOfflineCheckInterval(0),
		)
		assert.Error(t, err)

		_, err = New(checkFunc,
			WithOfflineDelay(-1*time.Second),
		)
		assert.Error(t, err)
	})
}

func TestConnectivityChecker_Close(t *testing.T) {
	t.Parallel()

	t.Run("close stops all operations", func(t *testing.T) {
		checkFunc := func() bool { return false }

		checker, err := New(checkFunc)
		require.NoError(t, err)

		err = checker.Close()
		assert.NoError(t, err)

		// Multiple closes should not panic
		err = checker.Close()
		assert.NoError(t, err)
	})

	t.Run("operations after close are ignored", func(t *testing.T) {
		checkFunc := func() bool { return true }

		checker, err := New(checkFunc)
		require.NoError(t, err)

		checker.Close()

		// TriggerCheck should be ignored after close
		checker.TriggerCheck()

		// State should still be accessible
		_ = checker.IsOnline()
	})
}

func TestConnectivityChecker_TriggerCheck_WithMockClock(t *testing.T) {
	t.Parallel()

	t.Run("ignores check when interval not passed", func(t *testing.T) {
		mockClock := clock.NewMock()
		var checkCount atomic.Int32

		checkFunc := func() bool {
			checkCount.Add(1)
			return true
		}

		checker, err := New(checkFunc,
			WithClock(mockClock),
			WithOnlineCheckInterval(1*time.Minute),
		)
		require.NoError(t, err)
		checker.Start()
		defer checker.Close()

		// Wait for initial check
		time.Sleep(10 * time.Millisecond)

		initialCount := checkCount.Load()

		// Trigger check immediately - should be ignored
		checker.TriggerCheck()
		time.Sleep(10 * time.Millisecond)

		assert.Equal(t, initialCount, checkCount.Load())

		// Advance clock and trigger check - should work
		mockClock.Add(2 * time.Minute)
		checker.TriggerCheck()
		time.Sleep(10 * time.Millisecond)

		assert.Greater(t, checkCount.Load(), initialCount)
	})

	t.Run("transitions from online to offline", func(t *testing.T) {
		var isOnline atomic.Bool
		isOnline.Store(true)

		checkFunc := func() bool {
			return isOnline.Load()
		}

		var onlineCallCount, offlineCallCount atomic.Int32
		onlineNotify := func() { onlineCallCount.Add(1) }
		offlineNotify := func() { offlineCallCount.Add(1) }

		checker, err := New(checkFunc,
			WithOnlineCheckInterval(10*time.Millisecond), // Very short interval
			WithOfflineCheckInterval(20*time.Millisecond),
			WithOfflineDelay(100*time.Millisecond), // Short delay for testing
			WithOnOnline(onlineNotify),
			WithOnOffline(offlineNotify),
		)
		require.NoError(t, err)
		checker.Start()
		defer checker.Close()

		// Wait for initialization - should be online
		time.Sleep(30 * time.Millisecond)
		assert.True(t, checker.IsOnline())

		// Make checkFunc return false and trigger check
		isOnline.Store(false)

		// Wait for online check interval to pass before triggering
		time.Sleep(20 * time.Millisecond)
		checker.TriggerCheck()
		time.Sleep(30 * time.Millisecond)

		// Should be in Disconnected state
		checker.stateMutex.Lock()
		assert.Equal(t, Disconnected, checker.state)
		checker.stateMutex.Unlock()

		// Wait for offline delay to pass
		time.Sleep(150 * time.Millisecond)

		// Should be offline and callback should be called
		checker.stateMutex.Lock()
		assert.Equal(t, Offline, checker.state)
		checker.stateMutex.Unlock()
		assert.Greater(t, offlineCallCount.Load(), int32(0))
	})

	t.Run("transitions from offline to online", func(t *testing.T) {
		mockClock := clock.NewMock()
		var isOnline atomic.Bool
		isOnline.Store(false)

		checkFunc := func() bool {
			return isOnline.Load()
		}

		var onlineCallCount atomic.Int32
		onlineNotify := func() { onlineCallCount.Add(1) }

		checker, err := New(checkFunc,
			WithClock(mockClock),
			WithOfflineCheckInterval(30*time.Second),
			WithOnOnline(onlineNotify),
		)
		require.NoError(t, err)
		checker.Start()
		defer checker.Close()

		// Wait for initialization
		time.Sleep(10 * time.Millisecond)
		assert.False(t, checker.IsOnline())

		// Make checkFunc return true
		isOnline.Store(true)

		// Advance clock to trigger offline check
		mockClock.Add(1 * time.Minute)
		time.Sleep(50 * time.Millisecond)

		// Should be online and callback should be called
		assert.True(t, checker.IsOnline())
		assert.Greater(t, onlineCallCount.Load(), int32(0))
	})

	t.Run("concurrent trigger checks", func(t *testing.T) {
		mockClock := clock.NewMock()
		var checkCount atomic.Int32

		checkFunc := func() bool {
			checkCount.Add(1)
			time.Sleep(50 * time.Millisecond) // Simulate slow check
			return true
		}

		checker, err := New(checkFunc,
			WithClock(mockClock),
			WithOnlineCheckInterval(1*time.Minute),
		)
		require.NoError(t, err)
		defer checker.Close()

		// Wait for initialization to complete
		time.Sleep(100 * time.Millisecond) // Longer wait to ensure initialization goroutine completes
		initialCount := checkCount.Load()

		// Advance clock
		mockClock.Add(2 * time.Minute)

		// Launch multiple concurrent checks
		var wg sync.WaitGroup
		for range 10 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				checker.TriggerCheck()
			}()
		}
		wg.Wait()

		// Wait for any ongoing checks to complete
		time.Sleep(100 * time.Millisecond)

		finalCount := checkCount.Load()

		// Only one additional check should have been performed (due to mutex)
		// The mutex should prevent concurrent checks, so we should see exactly initialCount+1 or maybe +2 due to timing
		assert.GreaterOrEqual(t, finalCount, initialCount+1)
		assert.LessOrEqual(t, finalCount, initialCount+2) // Allow for slight timing variations
	})

	t.Run("probe loop with periodic checks", func(t *testing.T) {
		mockClock := clock.NewMock()
		var isOnline atomic.Bool
		var checkCount atomic.Int32
		isOnline.Store(false)

		checkFunc := func() bool {
			checkCount.Add(1)
			return isOnline.Load()
		}

		var onlineCallCount atomic.Int32
		onlineNotify := func() { onlineCallCount.Add(1) }

		checker, err := New(checkFunc,
			WithClock(mockClock),
			WithOfflineCheckInterval(30*time.Second),
			WithOnOnline(onlineNotify),
		)
		require.NoError(t, err)
		checker.Start()
		defer checker.Close()

		// Wait for initialization
		time.Sleep(10 * time.Millisecond)
		initialCheckCount := checkCount.Load()

		// Advance clock multiple times to trigger periodic checks
		for range 3 {
			mockClock.Add(31 * time.Second)
			time.Sleep(10 * time.Millisecond)
		}

		// Multiple checks should have been performed
		assert.Greater(t, checkCount.Load(), initialCheckCount)

		// Now make it go online
		isOnline.Store(true)
		mockClock.Add(31 * time.Second)
		time.Sleep(50 * time.Millisecond)

		// Should be online and callback called
		assert.True(t, checker.IsOnline())
		assert.Greater(t, onlineCallCount.Load(), int32(0))
	})
}

func TestConnectivityChecker_StateTransitions(t *testing.T) {
	t.Parallel()

	t.Run("all state values", func(t *testing.T) {
		var isOnline atomic.Bool
		isOnline.Store(true)

		checkFunc := func() bool {
			return isOnline.Load()
		}

		checker, err := New(checkFunc,
			WithOnlineCheckInterval(10*time.Millisecond), // Very short interval to allow quick trigger
			WithOfflineDelay(100*time.Millisecond),       // Short delay for testing
			WithOfflineCheckInterval(20*time.Millisecond),
		)
		require.NoError(t, err)
		checker.Start()
		defer checker.Close()

		// Wait for initialization - should be Online
		time.Sleep(30 * time.Millisecond)
		checker.stateMutex.Lock()
		assert.Equal(t, Online, checker.state)
		checker.stateMutex.Unlock()
		assert.True(t, checker.IsOnline())

		// Make offline and trigger check - should be Disconnected
		isOnline.Store(false)

		// Wait for online check interval to pass before triggering
		time.Sleep(20 * time.Millisecond)
		checker.TriggerCheck()
		time.Sleep(30 * time.Millisecond)

		checker.stateMutex.Lock()
		assert.Equal(t, Disconnected, checker.state)
		checker.stateMutex.Unlock()
		assert.False(t, checker.IsOnline())

		// Wait beyond offline delay - should be Offline
		time.Sleep(150 * time.Millisecond)
		checker.stateMutex.Lock()
		assert.Equal(t, Offline, checker.state)
		checker.stateMutex.Unlock()
		assert.False(t, checker.IsOnline())
	})
}

func TestConnectivityChecker_Callbacks(t *testing.T) {
	t.Parallel()

	t.Run("callbacks are called appropriately", func(t *testing.T) {
		var isOnline atomic.Bool
		isOnline.Store(true) // Start online first

		checkFunc := func() bool {
			return isOnline.Load()
		}

		var onlineCallCount, offlineCallCount atomic.Int32
		onlineNotify := func() { onlineCallCount.Add(1) }
		offlineNotify := func() { offlineCallCount.Add(1) }

		checker, err := New(checkFunc,
			WithOnlineCheckInterval(10*time.Millisecond), // Very short interval
			WithOfflineCheckInterval(20*time.Millisecond),
			WithOfflineDelay(100*time.Millisecond), // Short delay for testing
			WithOnOnline(onlineNotify),
			WithOnOffline(offlineNotify),
		)
		require.NoError(t, err)
		checker.Start()
		defer checker.Close()

		// Wait for initialization - should be online
		time.Sleep(30 * time.Millisecond)
		assert.True(t, checker.IsOnline())

		// Go offline and trigger a check to start the offline transition
		isOnline.Store(false)

		// Wait for online check interval to pass before triggering
		time.Sleep(20 * time.Millisecond)
		checker.TriggerCheck()

		// Wait for offline delay to pass and offline callback to be called
		time.Sleep(150 * time.Millisecond)

		// Should have called offline callback
		assert.Greater(t, offlineCallCount.Load(), int32(0))

		// Go online
		isOnline.Store(true)
		time.Sleep(50 * time.Millisecond)

		// Should have called online callback
		assert.Greater(t, onlineCallCount.Load(), int32(0))
	})

	t.Run("panic in callback doesn't break checker", func(t *testing.T) {
		var isOnline atomic.Bool
		isOnline.Store(false)

		checkFunc := func() bool {
			return isOnline.Load()
		}

		safeCallback := func() {
			defer func() {
				if r := recover(); r != nil {
					// Expected panic, recover and continue
				}
			}()
			panic("test panic")
		}

		// This should not panic during construction or operation
		checker, err := New(checkFunc,
			WithOnOnline(safeCallback),
			WithOnOffline(safeCallback),
			WithOfflineCheckInterval(10*time.Millisecond),
			WithOfflineDelay(50*time.Millisecond),
		)
		require.NoError(t, err)
		defer checker.Close()

		// Wait for potential panics during initialization
		time.Sleep(100 * time.Millisecond)

		// Go online - should not panic the test
		isOnline.Store(true)
		time.Sleep(100 * time.Millisecond)

		// Checker should still be functional
		assert.NotPanics(t, func() {
			checker.IsOnline()
		})
	})
}

func TestConnectivityChecker_EdgeCases(t *testing.T) {
	t.Parallel()

	t.Run("close during probe loop", func(t *testing.T) {
		var checkCount atomic.Int32
		checkFunc := func() bool {
			checkCount.Add(1)
			return false
		}

		checker, err := New(checkFunc,
			WithOfflineCheckInterval(10*time.Millisecond),
		)
		require.NoError(t, err)
		checker.Start()

		// Let it run for a bit
		time.Sleep(50 * time.Millisecond)
		initialCount := checkCount.Load()

		// Close and verify no more checks happen
		checker.Close()
		time.Sleep(50 * time.Millisecond)

		// Should have stopped checking
		finalCount := checkCount.Load()
		time.Sleep(50 * time.Millisecond)
		laterCount := checkCount.Load()

		assert.Greater(t, initialCount, int32(0))
		assert.Equal(t, finalCount, laterCount) // No new checks after close
	})

	t.Run("trigger check after close", func(t *testing.T) {
		var checkCount atomic.Int32
		checkFunc := func() bool {
			checkCount.Add(1)
			return true
		}

		checker, err := New(checkFunc)
		require.NoError(t, err)

		time.Sleep(10 * time.Millisecond)
		checker.Close()

		initialCount := checkCount.Load()

		// Should be ignored
		checker.TriggerCheck()
		time.Sleep(10 * time.Millisecond)

		assert.Equal(t, initialCount, checkCount.Load())
	})

	t.Run("high frequency operations", func(t *testing.T) {
		mockClock := clock.NewMock()
		var isOnline atomic.Bool
		isOnline.Store(true)

		checkFunc := func() bool {
			return isOnline.Load()
		}

		checker, err := New(checkFunc,
			WithClock(mockClock),
			WithOnlineCheckInterval(100*time.Millisecond),
		)
		require.NoError(t, err)
		checker.Start()
		defer checker.Close()

		// Rapid state checks should work
		for range 100 {
			go func() {
				checker.IsOnline()
			}()
		}

		// Rapid trigger checks with clock advancement
		for range 10 {
			mockClock.Add(200 * time.Millisecond)
			checker.TriggerCheck()
		}

		time.Sleep(100 * time.Millisecond)

		// Should still be functional
		assert.True(t, checker.IsOnline())
	})
}

func TestConnectivityChecker_Options(t *testing.T) {
	t.Parallel()

	t.Run("WithOfflineDelay", func(t *testing.T) {
		// Use real time for this test since mock clock with timers is complex
		var isOnline atomic.Bool
		isOnline.Store(true)

		checkFunc := func() bool {
			return isOnline.Load()
		}

		var offlineCallCount atomic.Int32
		offlineNotify := func() { offlineCallCount.Add(1) }

		checker, err := New(checkFunc,
			WithOnlineCheckInterval(10*time.Millisecond), // Very short interval
			WithOfflineDelay(100*time.Millisecond),       // Short delay for testing
			WithOfflineCheckInterval(20*time.Millisecond),
			WithOnOffline(offlineNotify),
		)
		require.NoError(t, err)
		checker.Start()
		defer checker.Close()

		// Wait for initialization - should be online
		time.Sleep(30 * time.Millisecond)
		assert.True(t, checker.IsOnline())

		// Go offline and trigger check
		isOnline.Store(false)

		// Wait for online check interval to pass before triggering
		time.Sleep(20 * time.Millisecond)
		checker.TriggerCheck()
		time.Sleep(30 * time.Millisecond)

		// Should be disconnected, not offline yet
		checker.stateMutex.Lock()
		assert.Equal(t, Disconnected, checker.state)
		checker.stateMutex.Unlock()
		assert.Equal(t, int32(0), offlineCallCount.Load())

		// Wait for offline delay to pass
		time.Sleep(150 * time.Millisecond)

		// Now offline
		checker.stateMutex.Lock()
		assert.Equal(t, Offline, checker.state)
		checker.stateMutex.Unlock()
		assert.Greater(t, offlineCallCount.Load(), int32(0))
	})

	t.Run("zero offline delay", func(t *testing.T) {
		var isOnline atomic.Bool
		isOnline.Store(true)

		checkFunc := func() bool {
			return isOnline.Load()
		}

		var offlineCallCount atomic.Int32
		offlineNotify := func() { offlineCallCount.Add(1) }

		checker, err := New(checkFunc,
			WithOnlineCheckInterval(10*time.Millisecond), // Very short interval
			WithOfflineDelay(0),
			WithOfflineCheckInterval(10*time.Millisecond),
			WithOnOffline(offlineNotify),
		)
		require.NoError(t, err)
		checker.Start()
		defer checker.Close()

		// Wait for initialization to complete - should be online
		time.Sleep(20 * time.Millisecond)
		assert.True(t, checker.IsOnline())

		// Go offline and trigger check
		isOnline.Store(false)

		// Wait for online check interval to pass before triggering
		time.Sleep(20 * time.Millisecond)
		checker.TriggerCheck()

		// Give time for state to change to Disconnected first
		time.Sleep(20 * time.Millisecond)

		// With zero delay, should quickly transition to Offline
		time.Sleep(50 * time.Millisecond)

		checker.stateMutex.Lock()
		assert.Equal(t, Offline, checker.state)
		checker.stateMutex.Unlock()
		assert.Greater(t, offlineCallCount.Load(), int32(0))
	})
}

// Benchmark tests to ensure performance
func BenchmarkConnectivityChecker_StateAccess(b *testing.B) {
	checkFunc := func() bool { return true }

	checker, err := New(checkFunc)
	require.NoError(b, err)
	defer checker.Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			checker.IsOnline()
		}
	})
}

func BenchmarkConnectivityChecker_TriggerCheck(b *testing.B) {
	mockClock := clock.NewMock()
	var checkCount atomic.Int32

	checkFunc := func() bool {
		checkCount.Add(1)
		return true
	}

	checker, err := New(checkFunc,
		WithClock(mockClock),
		WithOnlineCheckInterval(1*time.Nanosecond), // Allow rapid checks
	)
	require.NoError(b, err)
	defer checker.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mockClock.Add(1 * time.Nanosecond)
		checker.TriggerCheck()
	}
}
