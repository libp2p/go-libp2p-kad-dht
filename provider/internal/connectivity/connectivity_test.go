//go:build go1.25

package connectivity

import (
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
)

var (
	onlineCheckFunc  = func() bool { return true }
	offlineCheckFunc = func() bool { return false }
)

func TestNewConnectiviyChecker(t *testing.T) {
	t.Run("initial state is offline", func(t *testing.T) {
		connChecker, err := New(onlineCheckFunc)
		require.NoError(t, err)
		defer connChecker.Close()

		require.False(t, connChecker.IsOnline())
	})

	t.Run("start online", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			onlineChan := make(chan struct{})
			onOnline := func() { close(onlineChan) }

			connChecker, err := New(onlineCheckFunc,
				WithOnOnline(onOnline),
			)
			require.NoError(t, err)
			defer connChecker.Close()

			require.False(t, connChecker.IsOnline())

			connChecker.Start()

			<-onlineChan // wait for onOnline to be run
			now := time.Now()
			synctest.Wait()

			require.True(t, connChecker.IsOnline())
			require.Equal(t, now, connChecker.LastStateChange())
		})
	})

	t.Run("start offline", func(t *testing.T) {
		onlineCount, offlineCount := atomic.Int32{}, atomic.Int32{}
		onOnline := func() { onlineCount.Add(1) }
		onOffline := func() { offlineCount.Add(1) }

		connChecker, err := New(offlineCheckFunc,
			WithOnOnline(onOnline),
			WithOnOffline(onOffline),
		)
		require.NoError(t, err)
		defer connChecker.Close()

		require.False(t, connChecker.IsOnline())

		connChecker.Start()

		require.False(t, connChecker.mutex.TryLock()) // node probing until it comes online

		require.False(t, connChecker.IsOnline())
		require.Equal(t, int32(0), onlineCount.Load())
		require.Equal(t, int32(0), offlineCount.Load())
	})
}

func TestStateTransitions(t *testing.T) {
	t.Run("offline to online", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			checkInterval := time.Second
			offlineDelay := time.Minute

			online := atomic.Bool{} // start offline
			checkFunc := func() bool { return online.Load() }

			onlineChan, offlineChan := make(chan struct{}), make(chan struct{})
			onOnline := func() { close(onlineChan) }
			onOffline := func() { close(offlineChan) }

			connChecker, err := New(checkFunc,
				WithOfflineDelay(offlineDelay),
				WithOnlineCheckInterval(checkInterval),
				WithOnOnline(onOnline),
				WithOnOffline(onOffline),
			)
			require.NoError(t, err)
			defer connChecker.Close()

			require.False(t, connChecker.IsOnline())
			connChecker.Start()

			time.Sleep(initialBackoffDelay)

			online.Store(true)

			<-onlineChan // wait for onOnline to be run
			synctest.Wait()
			require.True(t, connChecker.IsOnline())
			select {
			case <-offlineChan:
				require.FailNow(t, "onOffline shouldn't have been called")
			default:
			}
		})
	})

	t.Run("online to disconnected to offline", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			checkInterval := time.Second
			offlineDelay := time.Minute

			online := atomic.Bool{}
			online.Store(true)
			checkFunc := func() bool { return online.Load() }

			onlineChan, disconnectedChan, offlineChan := make(chan struct{}), make(chan struct{}), make(chan struct{})
			onOnline := func() { close(onlineChan) }
			onDisconnected := func() { close(disconnectedChan) }
			onOffline := func() { close(offlineChan) }

			connChecker, err := New(checkFunc,
				WithOfflineDelay(offlineDelay),
				WithOnlineCheckInterval(checkInterval),
				WithOnOnline(onOnline),
				WithOnDisconnected(onDisconnected),
				WithOnOffline(onOffline),
			)
			require.NoError(t, err)
			defer connChecker.Close()

			require.False(t, connChecker.IsOnline())
			connChecker.Start()

			<-onlineChan // wait for onOnline to be run
			synctest.Wait()
			require.True(t, connChecker.IsOnline())
			require.Equal(t, time.Now(), connChecker.lastCheck)
			require.Equal(t, time.Now(), connChecker.LastStateChange())

			online.Store(false)
			// Cannot trigger check yet
			connChecker.TriggerCheck()
			require.True(t, connChecker.mutex.TryLock()) // node still online
			connChecker.mutex.Unlock()

			time.Sleep(checkInterval - time.Millisecond)
			connChecker.TriggerCheck()
			require.True(t, connChecker.mutex.TryLock()) // node still online
			connChecker.mutex.Unlock()
			require.NotEqual(t, time.Now(), connChecker.LastStateChange())

			time.Sleep(time.Millisecond)
			connChecker.TriggerCheck()
			require.False(t, connChecker.mutex.TryLock())

			<-disconnectedChan // wait for onDisconnected to be run
			synctest.Wait()

			require.False(t, connChecker.IsOnline())
			select {
			case <-offlineChan:
				require.FailNow(t, "onOffline shouldn't have been called")
			default: // Disconnected but not Offline
			}

			connChecker.TriggerCheck() // noop since Disconnected
			require.False(t, connChecker.mutex.TryLock())

			time.Sleep(offlineDelay)

			require.False(t, connChecker.IsOnline())
			<-offlineChan // wait for callback to be run
			synctest.Wait()
			require.Equal(t, time.Now(), connChecker.LastStateChange())

			connChecker.TriggerCheck() // noop since Offline
			require.False(t, connChecker.mutex.TryLock())
		})
	})

	t.Run("online to offline immediately (offlineDelay=0)", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			checkInterval := time.Second

			online := atomic.Bool{}
			online.Store(true)
			checkFunc := func() bool { return online.Load() }

			onlineChan, disconnectedChan, offlineChan := make(chan struct{}), make(chan struct{}), make(chan struct{})
			onOnline := func() { close(onlineChan) }
			onDisconnected := func() { close(disconnectedChan) }
			onOffline := func() { close(offlineChan) }

			connChecker, err := New(checkFunc,
				WithOfflineDelay(0), // immediate offline transition
				WithOnlineCheckInterval(checkInterval),
				WithOnOnline(onOnline),
				WithOnDisconnected(onDisconnected),
				WithOnOffline(onOffline),
			)
			require.NoError(t, err)
			defer connChecker.Close()

			require.False(t, connChecker.IsOnline())
			connChecker.Start()

			<-onlineChan // wait for onOnline to be run
			synctest.Wait()
			require.True(t, connChecker.IsOnline())

			// Wait until we can perform a new check
			time.Sleep(checkInterval)

			// Go offline
			online.Store(false)
			connChecker.TriggerCheck()

			<-disconnectedChan // wait for onDisconnected to be called
			<-offlineChan      // wait for onOffline to be called immediately after
			synctest.Wait()

			require.False(t, connChecker.IsOnline())
		})
	})

	t.Run("remain online", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			checkInterval := time.Second
			offlineDelay := time.Minute

			online := atomic.Bool{}
			online.Store(true)
			checkCount := atomic.Int32{}
			checkFunc := func() bool { checkCount.Add(1); return online.Load() }

			onlineChan, offlineChan := make(chan struct{}), make(chan struct{})
			onOnline := func() { close(onlineChan) }
			onOffline := func() { close(offlineChan) }

			connChecker, err := New(checkFunc,
				WithOfflineDelay(offlineDelay),
				WithOnlineCheckInterval(checkInterval),
				WithOnOnline(onOnline),
				WithOnOffline(onOffline),
			)
			require.NoError(t, err)
			defer connChecker.Close()

			require.False(t, connChecker.IsOnline())
			connChecker.Start()

			<-onlineChan
			synctest.Wait()

			onlineSince := time.Now()
			require.True(t, connChecker.IsOnline())
			require.Equal(t, int32(1), checkCount.Load())
			require.Equal(t, onlineSince, connChecker.lastCheck)
			require.Equal(t, onlineSince, connChecker.LastStateChange())

			connChecker.TriggerCheck() // recent check, should be no-op
			synctest.Wait()
			require.Equal(t, int32(1), checkCount.Load())

			time.Sleep(checkInterval - 1)
			connChecker.TriggerCheck() // recent check, should be no-op
			synctest.Wait()
			require.Equal(t, int32(1), checkCount.Load())

			time.Sleep(time.Nanosecond)
			connChecker.TriggerCheck() // checkInterval has passed, new check is run
			synctest.Wait()
			require.Equal(t, int32(2), checkCount.Load())
			require.Equal(t, time.Now(), connChecker.lastCheck)

			time.Sleep(checkInterval)
			connChecker.TriggerCheck() // checkInterval has passed, new check is run
			synctest.Wait()
			require.Equal(t, int32(3), checkCount.Load())
			require.Equal(t, time.Now(), connChecker.lastCheck)
			require.Equal(t, onlineSince, connChecker.LastStateChange())
		})
	})
}

func TestSetCallbacks(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// Callbacks MUST be set before calling Start()
		oldOnlineCount, oldOfflineCount, newOnlineCount, newDisconnectedCount, newOfflineCount := atomic.Int32{}, atomic.Int32{}, atomic.Int32{}, atomic.Int32{}, atomic.Int32{}
		onlineChan, disconnectedChan, offlineChan := make(chan struct{}), make(chan struct{}), make(chan struct{})
		oldOnOnline := func() { oldOnlineCount.Add(1); close(onlineChan) }
		oldOnOffline := func() { oldOfflineCount.Add(1); close(offlineChan) }
		newOnOnline := func() { newOnlineCount.Add(1); close(onlineChan) }
		newOnDisconnected := func() { newDisconnectedCount.Add(1); close(disconnectedChan) }
		newOnOffline := func() { newOfflineCount.Add(1); close(offlineChan) }

		checkInterval := time.Second
		offlineDelay := time.Minute
		online := atomic.Bool{}
		online.Store(true)
		checkFunc := func() bool { return online.Load() }

		connChecker, err := New(checkFunc,
			WithOnOnline(oldOnOnline),
			WithOnOffline(oldOnOffline),
			WithOfflineDelay(offlineDelay),
			WithOnlineCheckInterval(checkInterval),
		)
		require.NoError(t, err)
		defer connChecker.Close()

		connChecker.SetCallbacks(newOnOnline, newOnDisconnected, newOnOffline)

		connChecker.Start()

		<-onlineChan // wait for newOnOnline to be called
		synctest.Wait()
		require.True(t, connChecker.IsOnline())
		require.Equal(t, int32(0), oldOnlineCount.Load())
		require.Equal(t, int32(1), newOnlineCount.Load())
		onlineSince := connChecker.LastStateChange()

		// Wait until we can perform a new check
		time.Sleep(checkInterval)

		// Go disconnected
		online.Store(false)
		connChecker.TriggerCheck()

		<-disconnectedChan // wait for newOnDisconnected to be called
		synctest.Wait()
		require.False(t, connChecker.IsOnline())
		require.Equal(t, int32(1), newDisconnectedCount.Load())
		disconnectedSince := connChecker.LastStateChange()
		require.True(t, disconnectedSince.After(onlineSince))

		// Verify we're in DISCONNECTED state (not yet OFFLINE)
		select {
		case <-offlineChan:
			require.FailNow(t, "onOffline shouldn't have been called yet")
		default:
		}

		// After offlineDelay, should transition to OFFLINE
		time.Sleep(offlineDelay)

		<-offlineChan // wait for newOnOffline to be called
		synctest.Wait()
		require.False(t, connChecker.IsOnline())
		require.Equal(t, int32(0), oldOfflineCount.Load())
		require.Equal(t, int32(1), newOfflineCount.Load())
		offlineSince := connChecker.LastStateChange()
		require.True(t, offlineSince.After(disconnectedSince))
	})
}

func TestExponentialBackoff(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		checkCount := atomic.Int32{}
		checkFunc := func() bool { checkCount.Add(1); return false }
		connChecker, err := New(checkFunc)
		require.NoError(t, err)
		defer connChecker.Close()

		connChecker.Start()
		require.False(t, connChecker.mutex.TryLock()) // node probing until it comes online
		require.False(t, connChecker.IsOnline())

		// Exponential backoff increase
		expectedWait := initialBackoffDelay
		expectedChecks := int32(1) // initial check
		for expectedWait < maxBackoffDelay {
			synctest.Wait()
			require.Equal(t, expectedChecks, checkCount.Load())
			time.Sleep(expectedWait)
			expectedChecks++
			expectedWait *= 2
		}

		// Reached max backoff delay
		synctest.Wait()
		require.Equal(t, expectedChecks, checkCount.Load())

		time.Sleep(maxBackoffDelay)
		expectedChecks++
		synctest.Wait()
		require.Equal(t, expectedChecks, checkCount.Load())

		time.Sleep(3 * maxBackoffDelay)
		expectedChecks += 3
		synctest.Wait()
		require.Equal(t, expectedChecks, checkCount.Load())
	})
}

func TestStartDisconnected(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		offlineDelay := time.Minute

		offlineChan := make(chan struct{})
		onOffline := func() { close(offlineChan) }

		connChecker, err := New(offlineCheckFunc,
			WithOfflineDelay(offlineDelay),
			WithOnOffline(onOffline),
			WithStartDisconnected(),
		)
		require.NoError(t, err)
		defer connChecker.Close()

		require.False(t, connChecker.IsOnline())

		connChecker.Start()

		// Should still be probing and not yet offline
		require.False(t, connChecker.mutex.TryLock())
		require.False(t, connChecker.IsOnline())

		// onOffline should not have been called yet
		select {
		case <-offlineChan:
			require.FailNow(t, "onOffline shouldn't have been called yet")
		default:
		}

		// After offlineDelay, onOffline callback should be called
		time.Sleep(offlineDelay)

		<-offlineChan // wait for onOffline to be called
		require.False(t, connChecker.IsOnline())
		require.Equal(t, time.Now(), connChecker.LastStateChange())
	})
}

func TestInvalidOptions(t *testing.T) {
	t.Run("negative online check interval", func(t *testing.T) {
		_, err := New(onlineCheckFunc, WithOnlineCheckInterval(-1))
		require.Error(t, err)
	})

	t.Run("negative offline delay", func(t *testing.T) {
		_, err := New(onlineCheckFunc, WithOfflineDelay(-1*time.Hour))
		require.Error(t, err)
	})
}

func TestClose(t *testing.T) {
	t.Run("close while offline", func(t *testing.T) {
		connChecker, err := New(offlineCheckFunc)
		require.NoError(t, err)
		defer connChecker.Close()

		connChecker.Start()
		require.False(t, connChecker.mutex.TryLock()) // node probing until it comes online
		require.False(t, connChecker.IsOnline())

		err = connChecker.Close()
		require.NoError(t, err)

		require.True(t, connChecker.mutex.TryLock())
		connChecker.mutex.Unlock()
	})

	t.Run("close while online", func(t *testing.T) {
		onlineChan := make(chan struct{})
		onOnline := func() { close(onlineChan) }
		connChecker, err := New(onlineCheckFunc,
			WithOnOnline(onOnline),
		)
		require.NoError(t, err)
		defer connChecker.Close()

		connChecker.Start()
		<-onlineChan
		require.True(t, connChecker.IsOnline())

		connChecker.Close()
	})

	t.Run("SetCallbacks after Close", func(t *testing.T) {
		onlineChan, disconnectedChan, offlineChan := make(chan struct{}), make(chan struct{}), make(chan struct{})
		onOnline := func() { close(onlineChan) }
		onDisconnected := func() { close(disconnectedChan) }
		onOffline := func() { close(offlineChan) }

		connChecker, err := New(offlineCheckFunc)
		require.NoError(t, err)
		defer connChecker.Close()

		require.Nil(t, connChecker.onOffline)
		require.Nil(t, connChecker.onOnline)
		require.Nil(t, connChecker.onDisconnected)

		connChecker.Close()
		connChecker.SetCallbacks(onOnline, onDisconnected, onOffline)

		// Assert that callbacks were NOT set
		require.Nil(t, connChecker.onOffline)
		require.Nil(t, connChecker.onOnline)
		require.Nil(t, connChecker.onDisconnected)
	})

	t.Run("TriggerCheck after Close", func(t *testing.T) {
		connChecker, err := New(offlineCheckFunc)
		require.NoError(t, err)
		defer connChecker.Close()

		connChecker.Start()
		// Node is already online
		require.False(t, connChecker.mutex.TryLock()) // node probing until it comes online
		require.False(t, connChecker.IsOnline())

		err = connChecker.Close()
		require.NoError(t, err)

		require.True(t, connChecker.mutex.TryLock())
		connChecker.mutex.Unlock()

		connChecker.TriggerCheck() // noop since closed

		require.True(t, connChecker.mutex.TryLock())
		connChecker.mutex.Unlock()
		require.False(t, connChecker.IsOnline())
	})
}
