package connectivity

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/filecoin-project/go-clock"
)

// ConnectivityChecker provides a thread-safe way to verify the connectivity of
// a node, and triggers a wake-up callback when the node comes back online
// after a period offline.
//
// Key behaviors:
//   - Connectivity check: external function `checkFunc` supplied by caller.
//   - Online handling: only run connectivity check upon call of triggerCheck()
//     if at least `onlineCheckInterval` has passed since the last check.
//   - Offline handling: while offline, triggerCheck() is ignored.
//     – A background loop runs `checkFunc` every `offlineCheckInterval` until
//     connectivity is restored.
//     – Once back online, the node’s status is updated and `backOnlineNotify`
//     is invoked exactly once.
type ConnectivityChecker struct {
	done      chan struct{}
	closeOnce sync.Once

	online atomic.Bool
	mutex  sync.Mutex

	clock                clock.Clock
	lastCheck            time.Time
	onlineCheckInterval  time.Duration // minimum check interval when online
	offlineCheckInterval time.Duration // periodic check frequency when offline

	checkFunc        func() bool // function to check whether node is online
	backOnlineNotify func()      // callback when node comes back online
}

// New creates a new ConnectivityChecker instance.
func New(checkFunc func() bool, backOnlineNotify func(), opts ...Option) (*ConnectivityChecker, error) {
	var cfg config
	err := cfg.apply(append([]Option{DefaultConfig}, opts...)...)
	if err != nil {
		return nil, err
	}
	c := &ConnectivityChecker{
		done:                 make(chan struct{}),
		clock:                cfg.clock,
		onlineCheckInterval:  cfg.onlineCheckInterval,
		offlineCheckInterval: cfg.offlineCheckInterval,
		checkFunc:            checkFunc,
		backOnlineNotify:     backOnlineNotify,
	}
	c.online.Store(true) // Start with the node considered online

	return c, nil
}

// Close stops any running connectivity checks and prevents future ones.
func (c *ConnectivityChecker) Close() error {
	c.closeOnce.Do(func() { close(c.done) })
	return nil
}

func (c *ConnectivityChecker) closed() bool {
	select {
	case <-c.done:
		return true
	default:
		return false
	}
}

// IsOnline returns true if the node is currently online, false otherwise.
func (c *ConnectivityChecker) IsOnline() bool {
	return c.online.Load()
}

// TriggerCheck triggers an asynchronous connectivity check.
//
// * If a check is already running, does nothing.
// * If a check was already performed within the last `onlineCheckInterval`, does nothing.
// * If after running the check the node is still online, update the last check timestamp.
// * If the node is found offline, enter the loop:
//   - Perform connectivity check every `offlineCheckInterval`.
//   - Exit if context is cancelled, or ConnectivityChecker is closed.
//   - When node is found back online, run the `backOnlineNotify` callback.
func (c *ConnectivityChecker) TriggerCheck() {
	if c.closed() {
		// Noop
		return
	}
	if !c.mutex.TryLock() {
		return // already checking
	}
	if c.online.Load() && c.clock.Now().Sub(c.lastCheck) < c.onlineCheckInterval {
		c.mutex.Unlock()
		return // last check was too recent
	}

	go func() {
		defer c.mutex.Unlock()

		if c.checkFunc() {
			c.lastCheck = c.clock.Now()
			return
		}

		// Node is offline, start periodic checks
		c.online.Store(false)

		ticker := c.clock.Ticker(c.offlineCheckInterval)
		defer ticker.Stop()
		for {
			select {
			case <-c.done:
				return
			case <-ticker.C:
				if c.checkFunc() {
					if !c.closed() {
						// Node is back online.
						c.online.Store(true)
						c.lastCheck = c.clock.Now()
						c.backOnlineNotify()
					}
					return
				}
			}
		}
	}()
}
