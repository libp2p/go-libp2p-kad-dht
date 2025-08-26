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
	closed    bool
	closeOnce sync.Once
	mutex     sync.Mutex

	online atomic.Bool

	clock                clock.Clock
	lastCheck            time.Time
	onlineCheckInterval  time.Duration // minimum check interval when online
	offlineCheckInterval time.Duration // periodic check frequency when offline

	checkFunc func() bool // function to check whether node is online

	onOffline    func()
	onOnline     func()
	offlineDelay time.Duration
}

// New creates a new ConnectivityChecker instance.
func New(checkFunc func() bool, opts ...Option) (*ConnectivityChecker, error) {
	var cfg config
	err := cfg.apply(append([]Option{DefaultConfig}, opts...)...)
	if err != nil {
		return nil, err
	}
	c := &ConnectivityChecker{
		done:                 make(chan struct{}),
		checkFunc:            checkFunc,
		clock:                cfg.clock,
		onlineCheckInterval:  cfg.onlineCheckInterval,
		offlineCheckInterval: cfg.offlineCheckInterval,
		onOffline:            cfg.onOffline,
		onOnline:             cfg.onOnline,
		offlineDelay:         cfg.offlineDelay,
	}
	return c, nil
}

// SetCallbacks sets the onOnline and onOffline callbacks after construction.
// This allows breaking circular dependencies during initialization.
func (c *ConnectivityChecker) SetCallbacks(onOnline, onOffline func()) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.closed {
		return
	}
	c.onOnline = onOnline
	c.onOffline = onOffline
}

func (c *ConnectivityChecker) Start() {
	c.mutex.Lock()
	// Start probing until the node comes online
	go func() {
		defer c.mutex.Unlock()

		if c.probe() {
			// Node is already online
			return
		}
		// Wait for node to come online
		c.probeLoop(true)
	}()
}

// Close stops any running connectivity checks and prevents future ones.
func (c *ConnectivityChecker) Close() error {
	c.closeOnce.Do(func() {
		close(c.done)
		c.mutex.Lock()
		c.closed = true
		c.mutex.Unlock()
	})
	return nil
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
	if !c.mutex.TryLock() {
		return // already checking
	}
	if c.closed {
		c.mutex.Unlock()
		return
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

		// Node is disconnected, start periodic checks
		c.online.Store(false)

		c.probeLoop(false)
	}()
}

func (c *ConnectivityChecker) probeLoop(init bool) {
	var offlineC <-chan time.Time
	if !init {
		offlineTimer := c.clock.Timer(c.offlineDelay)
		defer offlineTimer.Stop()
		offlineC = offlineTimer.C
	}

	ticker := c.clock.Ticker(c.offlineCheckInterval)
	// TODO: exponential backoff
	defer ticker.Stop()
	for {
		select {
		case <-c.done:
			return
		case <-ticker.C:
			if c.probe() {
				return
			}
		case <-offlineC:
			// Node is now offline
			if c.onOffline != nil {
				c.onOffline()
			}
		}
	}
}

func (c *ConnectivityChecker) probe() bool {
	if c.checkFunc() {
		select {
		case <-c.done:
		default:
			// Node is back online.
			c.online.Store(true)

			c.lastCheck = c.clock.Now()
			if c.onOnline != nil {
				c.onOnline()
			}
		}
		return true
	}
	return false
}
