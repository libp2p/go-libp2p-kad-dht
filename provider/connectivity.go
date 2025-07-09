package provider

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/filecoin-project/go-clock"
)

// connectivityChecker provides a thread-safe way to verify the connectivity of
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
type connectivityChecker struct {
	ctx context.Context

	online atomic.Bool
	mutex  sync.Mutex

	clock                clock.Clock
	lastCheck            time.Time
	onlineCheckInterval  time.Duration // minimum check interval when online
	offlineCheckInterval time.Duration // periodic check frequency when offline

	checkFunc        func() bool // function to check whether node is online
	backOnlineNotify func()      // callback when node comes back online
}

func (c *connectivityChecker) isOnline() bool {
	return c.online.Load()
}

func (c *connectivityChecker) triggerCheck() {
	go func() {
		if !c.mutex.TryLock() {
			return // already checking
		}
		defer c.mutex.Unlock()

		if c.clock.Now().Sub(c.lastCheck) < c.onlineCheckInterval {
			return // last check was too recent
		}

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
			case <-c.ctx.Done():
				return
			case <-ticker.C:
				if c.checkFunc() {
					// Node is back online.
					c.online.Store(true)
					c.lastCheck = c.clock.Now()
					c.backOnlineNotify()
					return
				}
			}
		}
	}()
}
