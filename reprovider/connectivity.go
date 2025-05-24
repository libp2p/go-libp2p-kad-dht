package reprovider

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/filecoin-project/go-clock"
)

// offlineSuspected
// if node is currently offline, do nothing
// if last probe was less than checkInterval ago, do nothing
// if last probe was more than checkInterval ago, check if node is online
// * if it is, do nothing
// * if it isn't:
//   - set online to false
//   - start periodic onlineCheck, until node is online again
//   - when node is online again, signal that we can catch up on pending work
//

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
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			if c.checkFunc() {
				// Node is back online.
				c.online.Store(true)
				c.backOnlineNotify()
				return
			}
		}
	}
}

//
// import (
// 	"sync/atomic"
// 	"time"
// )
//
// type connectivityChecker struct {
// 	running      atomic.Bool
// 	online       atomic.Bool
// 	ongoingCheck atomic.Bool
//
// 	onlineCheckFunc func() bool
// 	onComplete      func(wasOnline, isOnline bool)
//
// 	checkInterval time.Duration
//
// 	pendingCheck chan struct{}
// }
//
// func (c *connectivityChecker) run() {
// 	if !c.running.CompareAndSwap(false, true) {
// 		return // already running
// 	}
//
// 	for {
// 		select {
// 		case <-c.pendingCheck:
//
// 		default:
// 			return // no pending checks, exit
// 		}
// 	}
// 	defer c.running.Store(false)
// 	defer close(c.pendingCheck)
// }
//
// func (c *connectivityChecker) isOnline() bool {
// 	return c.online.Load()
// }
//
// func (c *connectivityChecker) triggerCheck() {
// }
//
// func (c *connectivityChecker) forceCheck() {
// }
//
// func (c *connectivityChecker) check() {
// 	if !c.ongoingCheck.CompareAndSwap(false, true) {
// 		// Check already in progress.
// 		return
// 	}
// 	defer c.ongoingCheck.Store(false)
//
// 	if c.onlineCheckFunc == nil {
// 		logger.Error("connectivity check function isn't defined")
// 		return // no check function defined
// 	}
//
// 	wasOnline := c.online.Load()
// 	isOnline := c.onlineCheckFunc()
// 	c.online.Store(isOnline)
// 	if c.onComplete != nil {
// 		c.onComplete(wasOnline, isOnline)
// 	}
// }
