package reprovider

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/filecoin-project/go-clock"
)

type connectivityChecker struct {
	ctx context.Context

	online atomic.Bool
	mutex  sync.Mutex

	clock                clock.Clock
	lastCheck            time.Time
	onlineCheckInterval  time.Duration // maximum check frequency when online
	offlineCheckInterval time.Duration // periodic check frequency when offline

	checkFunc        func() bool // function to check if node is online
	backOnlineNotify func()
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
