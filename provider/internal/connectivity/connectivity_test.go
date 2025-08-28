package connectivity

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/quartz"
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

		require.False(t, connChecker.IsOnline())
	})

	t.Run("start online", func(t *testing.T) {
		onlineChan := make(chan struct{})
		onOnline := func() { close(onlineChan) }

		clk := quartz.NewMock(t)
		connChecker, err := New(onlineCheckFunc,
			WithMockClock(clk),
			WithOnOnline(onOnline),
		)
		require.NoError(t, err)

		require.False(t, connChecker.IsOnline())

		connChecker.Start()

		<-onlineChan // wait for onOnline to be run
		_, ok := clk.Peek()
		require.False(t, ok)

		require.True(t, connChecker.IsOnline())
	})

	t.Run("start offline", func(t *testing.T) {
		onlineCount, offlineCount := atomic.Int32{}, atomic.Int32{}
		onOnline := func() { onlineCount.Add(1) }
		onOffline := func() { offlineCount.Add(1) }

		clk := quartz.NewMock(t)
		connChecker, err := New(offlineCheckFunc,
			WithMockClock(clk),
			WithOnOnline(onOnline),
			WithOnOffline(onOffline),
		)
		require.NoError(t, err)

		require.False(t, connChecker.IsOnline())

		connChecker.Start()

		require.False(t, connChecker.mutex.TryLock()) // node probing until it comes online

		require.False(t, connChecker.IsOnline())
		require.Equal(t, int32(0), onlineCount.Load())
		require.Equal(t, int32(0), offlineCount.Load())
	})
}

func TestStateTransition(t *testing.T) {
	t.Run("offline to online", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		checkInterval := time.Second
		offlineDelay := time.Minute

		clk := quartz.NewMock(t)

		online := atomic.Bool{} // start offline
		checkFunc := func() bool { return online.Load() }

		onlineChan, offlineChan := make(chan struct{}), make(chan struct{})
		onOnline := func() { close(onlineChan) }
		onOffline := func() { close(offlineChan) }

		trap := clk.Trap().NewTimer("periodicProbe")
		defer trap.Close()

		connChecker, err := New(checkFunc,
			WithMockClock(clk),
			WithOfflineDelay(offlineDelay),
			WithOnlineCheckInterval(checkInterval),
			WithOnOnline(onOnline),
			WithOnOffline(onOffline),
		)
		require.NoError(t, err)

		require.False(t, connChecker.IsOnline())
		connChecker.Start()

		trap.MustWait(context.Background()).MustRelease(context.Background())

		online.Store(true)
		_, w := clk.AdvanceNext()
		w.MustWait(ctx)

		<-onlineChan // wait for onOnline to be run
		require.True(t, connChecker.IsOnline())
		select {
		case <-offlineChan:
			require.FailNow(t, "onOffline shouldn't have been called")
		default:
		}
	})

	t.Run("online to disconnected to offline", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		checkInterval := time.Second
		offlineDelay := time.Minute

		clk := quartz.NewMock(t)

		online := atomic.Bool{}
		online.Store(true)
		checkFunc := func() bool { return online.Load() }

		onlineChan, offlineChan := make(chan struct{}), make(chan struct{})
		onOnline := func() { close(onlineChan) }
		onOffline := func() { close(offlineChan) }

		trap := clk.Trap().NewTimer("periodicProbe")
		defer trap.Close()

		connChecker, err := New(checkFunc,
			WithMockClock(clk),
			WithOfflineDelay(offlineDelay),
			WithOnlineCheckInterval(checkInterval),
			WithOnOnline(onOnline),
			WithOnOffline(onOffline),
		)
		require.NoError(t, err)

		require.False(t, connChecker.IsOnline())
		connChecker.Start()

		<-onlineChan // wait for onOnline to be run
		require.True(t, connChecker.IsOnline())
		require.Equal(t, clk.Now(), connChecker.lastCheck)

		online.Store(false)
		// Cannot trigger check yet
		connChecker.TriggerCheck()
		require.True(t, connChecker.mutex.TryLock()) // node still online
		connChecker.mutex.Unlock()

		clk.Advance(checkInterval - 1)
		connChecker.TriggerCheck()
		require.True(t, connChecker.mutex.TryLock()) // node still online
		connChecker.mutex.Unlock()

		clk.Advance(1)
		connChecker.TriggerCheck()
		require.False(t, connChecker.mutex.TryLock())

		trap.MustWait(context.Background()).MustRelease(context.Background())
		require.False(t, connChecker.IsOnline())
		select {
		case <-offlineChan:
			require.FailNow(t, "onOffline shouldn't have been called")
		default: // Disconnected but not Offline
		}

		connChecker.TriggerCheck() // noop since Disconnected
		require.False(t, connChecker.mutex.TryLock())

		// Wait for offlineDelay to elapse
		advanced := checkInterval
		for advanced < offlineDelay {
			d, w := clk.AdvanceNext()
			w.MustWait(ctx)
			advanced += d
		}

		// Now Offline
		require.False(t, connChecker.IsOnline())
		<-offlineChan // wait for callback to be run

		connChecker.TriggerCheck() // noop since Offline
		require.False(t, connChecker.mutex.TryLock())
	})

	t.Run("remain online", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		checkInterval := time.Second
		offlineDelay := time.Minute

		clk := quartz.NewMock(t)

		online := atomic.Bool{}
		online.Store(true)
		checkCount := atomic.Int32{}
		checkFunc := func() bool { checkCount.Add(1); return online.Load() }

		onlineChan, offlineChan := make(chan struct{}), make(chan struct{})
		onOnline := func() { close(onlineChan) }
		onOffline := func() { close(offlineChan) }

		startTime := clk.Now()

		trap := clk.Trap().Now()
		defer trap.Close()

		connChecker, err := New(checkFunc,
			WithMockClock(clk),
			WithOfflineDelay(offlineDelay),
			WithOnlineCheckInterval(checkInterval),
			WithOnOnline(onOnline),
			WithOnOffline(onOffline),
		)
		require.NoError(t, err)

		require.False(t, connChecker.IsOnline())
		connChecker.Start()

		trap.MustWait(ctx).MustRelease(ctx)
		<-onlineChan

		require.True(t, connChecker.IsOnline())
		require.Equal(t, int32(1), checkCount.Load())
		require.Equal(t, startTime, connChecker.lastCheck)

		connChecker.TriggerCheck() // recent check, should be no-op
		require.Equal(t, int32(1), checkCount.Load())

		clk.Advance(checkInterval - 1).MustWait(ctx)
		connChecker.TriggerCheck() // recent check, should be no-op
		require.Equal(t, int32(1), checkCount.Load())

		clk.Advance(1).MustWait(ctx)
		connChecker.TriggerCheck() // checkInterval has passed, new check is run
		trap.MustWait(ctx).MustRelease(ctx)
		require.Equal(t, int32(2), checkCount.Load())

		clk.Advance(checkInterval).MustWait(ctx)
		connChecker.TriggerCheck() // checkInterval has passed, new check is run
		trap.MustWait(ctx).MustRelease(ctx)
		require.Equal(t, int32(3), checkCount.Load())
	})
}

func TestSetCallbacks(t *testing.T) {
	// Callbacks MUST be set before calling Start()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	oldOnlineCount, oldOfflineCount, newOnlineCount, newOfflineCount := atomic.Int32{}, atomic.Int32{}, atomic.Int32{}, atomic.Int32{}
	onlineChan, offlineChan := make(chan struct{}), make(chan struct{})
	oldOnOnline := func() { oldOnlineCount.Add(1); close(onlineChan) }
	oldOnOffline := func() { oldOfflineCount.Add(1); close(offlineChan) }
	newOnOnline := func() { newOnlineCount.Add(1); close(onlineChan) }
	newOnOffline := func() { newOfflineCount.Add(1); close(offlineChan) }

	checkInterval := time.Second
	online := atomic.Bool{}
	online.Store(true)
	checkFunc := func() bool { return online.Load() }
	clk := quartz.NewMock(t)

	connChecker, err := New(checkFunc,
		WithMockClock(clk),
		WithOnOnline(oldOnOnline),
		WithOnOffline(oldOnOffline),
		WithOfflineDelay(0),
		WithOnlineCheckInterval(checkInterval),
	)
	require.NoError(t, err)

	connChecker.SetCallbacks(newOnOnline, newOnOffline)

	connChecker.Start()

	<-onlineChan // wait for newOnOnline to be called
	require.True(t, connChecker.IsOnline())
	require.Equal(t, int32(0), oldOnlineCount.Load())
	require.Equal(t, int32(1), newOnlineCount.Load())

	// Wait until we can perform a new check
	clk.Advance(checkInterval).MustWait(ctx)

	// Go offline
	online.Store(false)
	connChecker.TriggerCheck()
	require.False(t, connChecker.mutex.TryLock()) // node probing until it comes online

	<-offlineChan // wait for newOnOffline to be called
	require.False(t, connChecker.IsOnline())
	require.Equal(t, int32(0), oldOfflineCount.Load())
	require.Equal(t, int32(1), newOfflineCount.Load())
}

func TestExponentialBackoff(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	clk := quartz.NewMock(t)
	initTrap := clk.Trap().NewTimer("periodicProbe")
	resetTrap := clk.Trap().TimerReset()
	defer func() {
		initTrap.Close()
		resetTrap.Close()
	}()

	connChecker, err := New(offlineCheckFunc,
		WithMockClock(clk),
	)
	require.NoError(t, err)

	connChecker.Start()
	require.False(t, connChecker.mutex.TryLock()) // node probing until it comes online
	require.False(t, connChecker.IsOnline())

	initTrap.MustWait(ctx).MustRelease(ctx)

	expectedWait := initialBackoffDelay
	for expectedWait < maxBackoffDelay {
		d, w := clk.AdvanceNext()
		w.MustWait(ctx)
		require.Equal(t, expectedWait, d)
		resetTrap.MustWait(ctx).MustRelease(ctx)
		expectedWait *= 2
	}
	d, w := clk.AdvanceNext()
	w.MustWait(ctx)
	require.Equal(t, maxBackoffDelay, d)
	resetTrap.MustWait(ctx).MustRelease(ctx)

	d, w = clk.AdvanceNext()
	w.MustWait(ctx)
	require.Equal(t, maxBackoffDelay, d)
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

		connChecker.Start()
		<-onlineChan
		require.True(t, connChecker.IsOnline())

		connChecker.Close()
	})

	t.Run("SetCallbacks after Close", func(t *testing.T) {
		onlineChan, offlineChan := make(chan struct{}), make(chan struct{})
		onOnline := func() { close(onlineChan) }
		onOffline := func() { close(offlineChan) }

		connChecker, err := New(offlineCheckFunc)
		require.NoError(t, err)

		require.Nil(t, connChecker.onOffline)
		require.Nil(t, connChecker.onOnline)

		connChecker.Close()
		connChecker.SetCallbacks(onOnline, onOffline)

		// Assert that callbacks were NOT set
		require.Nil(t, connChecker.onOffline)
		require.Nil(t, connChecker.onOnline)
	})

	t.Run("TriggerCheck after Close", func(t *testing.T) {
		connChecker, err := New(offlineCheckFunc)
		require.NoError(t, err)

		connChecker.Start()
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
