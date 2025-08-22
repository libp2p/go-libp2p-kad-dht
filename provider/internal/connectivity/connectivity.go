package connectivity

import (
	"sync"
	"time"

	"github.com/filecoin-project/go-clock"
)

type ConnectivityState uint8

const (
	Online ConnectivityState = iota
	Offline
	Disconnected
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

	state      ConnectivityState
	stateMutex sync.RWMutex

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
func New(checkFunc func() bool, backOnlineNotify func(), opts ...Option) (*ConnectivityChecker, error) {
	var cfg config
	err := cfg.apply(append([]Option{DefaultConfig}, opts...)...)
	if err != nil {
		return nil, err
	}
	c := &ConnectivityChecker{
		done:                 make(chan struct{}),
		checkFunc:            checkFunc,
		state:                Offline,
		clock:                cfg.clock,
		onlineCheckInterval:  cfg.onlineCheckInterval,
		offlineCheckInterval: cfg.offlineCheckInterval,
		onOffline:            cfg.onOffline,
		onOnline:             cfg.onOnline,
	}

	// Start probing until the node comes online
	c.mutex.Lock()
	go func() {
		defer c.mutex.Unlock()

		if c.probe() {
			// Node is already online
			return
		}
		// Wait for node to come online
		c.probeLoop(true)
	}()

	return c, nil
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
	c.stateMutex.RLock()
	defer c.stateMutex.RUnlock()
	return c.state == Online
}

func (c *ConnectivityChecker) IsOffline() bool {
	c.stateMutex.RLock()
	defer c.stateMutex.RUnlock()
	return c.state == Offline
}

func (c *ConnectivityChecker) State() ConnectivityState {
	c.stateMutex.RLock()
	defer c.stateMutex.RUnlock()
	return c.state
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
	c.stateMutex.RLock()
	if c.state == Online && c.clock.Now().Sub(c.lastCheck) < c.onlineCheckInterval {
		c.stateMutex.RUnlock()
		c.mutex.Unlock()
		return // last check was too recent
	}
	c.stateMutex.RUnlock()

	go func() {
		defer c.mutex.Unlock()

		if c.checkFunc() {
			c.lastCheck = c.clock.Now()
			return
		}

		// Node is offline, start periodic checks
		c.stateMutex.Lock()
		c.state = Disconnected
		c.stateMutex.Unlock()

		c.probeLoop(false)
	}()
}

func (c *ConnectivityChecker) probeLoop(init bool) {
	offlineTimer := c.clock.Timer(c.offlineDelay)
	if init {
		offlineTimer.Stop()
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
		case <-offlineTimer.C:
			// Node is now offline
			c.stateMutex.Lock()
			c.state = Offline
			c.stateMutex.Unlock()

			c.onOffline()
		}
	}
}

func (c *ConnectivityChecker) probe() bool {
	if c.checkFunc() {
		select {
		case <-c.done:
		default:
			// Node is back online.
			c.stateMutex.Lock()
			c.state = Online
			c.stateMutex.Unlock()

			c.lastCheck = c.clock.Now()
			c.onOnline()
		}
		return true
	}
	return false
}
