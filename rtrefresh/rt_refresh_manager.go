package rtrefresh

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"

	kbucket "github.com/libp2p/go-libp2p-kbucket"

	"github.com/hashicorp/go-multierror"
	logging "github.com/ipfs/go-log"
)

var logger = logging.Logger("dht/RtRefreshManager")

const (
	maxNoResultsAfterRefresh = 2
	peerPingTimeout          = 10 * time.Second
)

type triggerRefreshReq struct {
	respCh          chan error
	forceCplRefresh bool
}

type RtRefreshManager struct {
	ctx       context.Context
	cancel    context.CancelFunc
	refcount  sync.WaitGroup
	closeOnce sync.Once

	// peerId of this DHT peer i.e. self peerId.
	h         host.Host
	dhtPeerId peer.ID
	rt        *kbucket.RoutingTable

	enableAutoRefresh   bool                                        // should run periodic refreshes ?
	refreshKeyGenFnc    func(cpl uint) (string, error)              // generate the key for the query to refresh this cpl
	refreshQueryFnc     func(ctx context.Context, key string) error // query to run for a refresh.
	refreshQueryTimeout time.Duration                               // timeout for one refresh query

	// interval between two periodic refreshes.
	// also, a cpl wont be refreshed it the time since it was last refreshed
	// is below the interval..unless a "forced" refresh is done.
	refreshInterval                    time.Duration
	successfulOutboundQueryGracePeriod time.Duration

	triggerRefresh  chan *triggerRefreshReq // channel to write refresh requests to.
	noResultsForCpl map[uint]int            // tracks how many times we didn't get any peer for a cpl
}

func NewRtRefreshManager(h host.Host, rt *kbucket.RoutingTable, autoRefresh bool,
	refreshKeyGenFnc func(cpl uint) (string, error),
	refreshQueryFnc func(ctx context.Context, key string) error,
	refreshQueryTimeout time.Duration,
	refreshInterval time.Duration,
	successfulOutboundQueryGracePeriod time.Duration) (*RtRefreshManager, error) {

	ctx, cancel := context.WithCancel(context.Background())
	return &RtRefreshManager{
		ctx:       ctx,
		cancel:    cancel,
		h:         h,
		dhtPeerId: h.ID(),
		rt:        rt,

		enableAutoRefresh: autoRefresh,
		refreshKeyGenFnc:  refreshKeyGenFnc,
		refreshQueryFnc:   refreshQueryFnc,

		refreshQueryTimeout:                refreshQueryTimeout,
		refreshInterval:                    refreshInterval,
		successfulOutboundQueryGracePeriod: successfulOutboundQueryGracePeriod,

		triggerRefresh:  make(chan *triggerRefreshReq),
		noResultsForCpl: make(map[uint]int),
	}, nil
}

func (r *RtRefreshManager) Start() error {
	r.refcount.Add(1)
	go r.loop()
	return nil
}

func (r *RtRefreshManager) Close() error {
	r.closeOnce.Do(func() {
		r.cancel()
		r.refcount.Wait()
	})
	return nil
}

// RefreshRoutingTable requests the refresh manager to refresh the Routing Table.
// If the force parameter is set to true true, all buckets will be refreshed irrespective of when they were last refreshed.
//
// The returned channel will block until the refresh finishes, then yield the
// error and close. The channel is buffered and safe to ignore.
func (r *RtRefreshManager) Refresh(force bool) <-chan error {
	resp := make(chan error, 1)
	select {
	case r.triggerRefresh <- &triggerRefreshReq{respCh: resp, forceCplRefresh: force}:
	case <-r.ctx.Done():
		resp <- r.ctx.Err()
	}
	return resp
}

// RefreshNoWait requests the refresh manager to refresh the Routing Table.
// However, it moves on without blocking if it's request can't get through.
func (r *RtRefreshManager) RefreshNoWait() {
	select {
	case r.triggerRefresh <- &triggerRefreshReq{}:
	default:
	}
}

func (r *RtRefreshManager) loop() {
	defer r.refcount.Done()

	var refreshTickrCh <-chan time.Time
	if r.enableAutoRefresh {
		err := r.doRefresh(true)
		if err != nil {
			logger.Warn("failed when refreshing routing table", err)
		}
		t := time.NewTicker(r.refreshInterval)
		defer t.Stop()
		refreshTickrCh = t.C
	}

	for {
		var waiting []chan<- error
		var forced bool
		select {
		case <-refreshTickrCh:
		case triggerRefreshReq := <-r.triggerRefresh:
			if triggerRefreshReq.respCh != nil {
				waiting = append(waiting, triggerRefreshReq.respCh)
			}
			forced = forced || triggerRefreshReq.forceCplRefresh
		case <-r.ctx.Done():
			return
		}

		// Batch multiple refresh requests if they're all waiting at the same time.
	OuterLoop:
		for {
			select {
			case triggerRefreshReq := <-r.triggerRefresh:
				if triggerRefreshReq.respCh != nil {
					waiting = append(waiting, triggerRefreshReq.respCh)
				}
				forced = forced || triggerRefreshReq.forceCplRefresh
			default:
				break OuterLoop
			}
		}

		// EXECUTE the refresh

		// ping Routing Table peers that haven't been hear of/from in the interval they should have been.
		// and evict them if they don't reply.
		var wg sync.WaitGroup
		for _, ps := range r.rt.GetPeerInfos() {
			if time.Since(ps.LastSuccessfulOutboundQueryAt) > r.successfulOutboundQueryGracePeriod {
				wg.Add(1)
				go func(ps kbucket.PeerInfo) {
					defer wg.Done()
					livelinessCtx, cancel := context.WithTimeout(r.ctx, peerPingTimeout)
					if err := r.h.Connect(livelinessCtx, peer.AddrInfo{ID: ps.Id}); err != nil {
						logger.Debugw("evicting peer after failed ping", "peer", ps.Id, "error", err)
						r.rt.RemovePeer(ps.Id)
					}
					cancel()
				}(ps)
			}
		}
		wg.Wait()

		// Query for self and refresh the required buckets
		err := r.doRefresh(forced)
		for _, w := range waiting {
			w <- err
			close(w)
		}
		if err != nil {
			logger.Warnw("failed when refreshing routing table", "error", err)
		}
	}
}

func (r *RtRefreshManager) doRefresh(forceRefresh bool) error {
	var merr error

	if err := r.queryForSelf(); err != nil {
		merr = multierror.Append(merr, err)
	}

	for c, lastRefreshedAt := range r.rt.GetTrackedCplsForRefresh() {
		cpl := uint(c)

		// skip cpls that we've stopped discovering new peers for
		if r.noResultsForCpl[cpl] >= maxNoResultsAfterRefresh {
			continue
		}

		isCplFull := r.rt.IsBucketFull(cpl)
		peersBeforeRefresh := r.rt.GetPeersForCpl(cpl)

		var err error
		if forceRefresh {
			err = r.refreshCpl(cpl)
		} else {
			err = r.refreshCplIfEligible(cpl, lastRefreshedAt)
		}

		if err != nil {
			merr = multierror.Append(merr, err)
		} else {
			if !refreshDiscoverNewPeers(isCplFull, peersBeforeRefresh, r.rt.GetPeersForCpl(cpl)) {
				r.noResultsForCpl[cpl] = r.noResultsForCpl[cpl] + 1
			}
		}
	}

	return merr
}

// did we discover any new peers because of the refresh for this cpl ?
// Only if they have the exact same elements
// should we consider that we didn't discover any new peers.
// This is because peers can also randomly drop on and off the routing table.
func refreshDiscoverNewPeers(wasCplFull bool, peersBeforeRefresh []peer.ID, peersAfterRefresh []peer.ID) bool {
	// we should always refresh buckets that were once full
	if wasCplFull || (len(peersBeforeRefresh) != len(peersAfterRefresh)) {
		return true
	}

	sort.Slice(peersBeforeRefresh, func(i, j int) bool {
		return peersBeforeRefresh[i] < peersBeforeRefresh[j]
	})

	sort.Slice(peersAfterRefresh, func(i, j int) bool {
		return peersAfterRefresh[i] < peersAfterRefresh[j]
	})

	for i := range peersBeforeRefresh {
		if peersBeforeRefresh[i] != peersAfterRefresh[i] {
			return true
		}
	}
	return false
}

func (r *RtRefreshManager) refreshCplIfEligible(cpl uint, lastRefreshedAt time.Time) error {
	if time.Since(lastRefreshedAt) <= r.refreshInterval {
		logger.Debugf("not running refresh for cpl %d as time since last refresh not above interval", cpl)
		return nil
	}

	return r.refreshCpl(cpl)
}

func (r *RtRefreshManager) refreshCpl(cpl uint) error {
	// gen a key for the query to refresh the cpl
	key, err := r.refreshKeyGenFnc(cpl)
	if err != nil {
		return fmt.Errorf("failed to generated query key for cpl=%d, err=%s", cpl, err)
	}

	logger.Infof("starting refreshing cpl %d with key %s (routing table size was %d)",
		cpl, key, r.rt.Size())

	if err := r.runRefreshDHTQuery(key); err != nil {
		return fmt.Errorf("failed to refresh cpl=%d, err=%s", cpl, err)
	}

	logger.Infof("finished refreshing cpl %d, routing table size is now %d", cpl, r.rt.Size())
	return nil
}

func (r *RtRefreshManager) queryForSelf() error {
	if err := r.runRefreshDHTQuery(string(r.dhtPeerId)); err != nil {
		return fmt.Errorf("failed to query for self, err=%s", err)
	}
	return nil
}

func (r *RtRefreshManager) runRefreshDHTQuery(key string) error {
	queryCtx, cancel := context.WithTimeout(r.ctx, r.refreshQueryTimeout)
	defer cancel()

	err := r.refreshQueryFnc(queryCtx, key)

	if err == nil || (err == context.DeadlineExceeded && queryCtx.Err() == context.DeadlineExceeded) {
		return nil
	}

	return fmt.Errorf("failed to run refresh DHT query for key=%s, err=%s", key, err)
}
