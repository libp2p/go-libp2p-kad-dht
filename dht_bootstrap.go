package dht

import (
	"context"
	"fmt"
	"time"

	"github.com/libp2p/go-libp2p-core/peer"

	multierror "github.com/hashicorp/go-multierror"
	process "github.com/jbenet/goprocess"
	processctx "github.com/jbenet/goprocess/context"
	kbucket "github.com/libp2p/go-libp2p-kbucket"
	"github.com/multiformats/go-multiaddr"
	_ "github.com/multiformats/go-multiaddr-dns"
)

var DefaultBootstrapPeers []multiaddr.Multiaddr

// Minimum number of peers in the routing table. If we drop below this and we
// see a new peer, we trigger a bootstrap round.
var minRTRefreshThreshold = 10

// timeout for pinging one peer
const peerPingTimeout = 10 * time.Second

func init() {
	for _, s := range []string{
		"/dnsaddr/bootstrap.libp2p.io/p2p/QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN",
		"/dnsaddr/bootstrap.libp2p.io/p2p/QmQCU2EcMqAqQPR2i9bChDtGNJchTbq5TbXJJ16u19uLTa",
		"/dnsaddr/bootstrap.libp2p.io/p2p/QmbLHAnMoJPWSCR5Zhtx6BHJX9KiKNN6tpvbUcqanj75Nb",
		"/dnsaddr/bootstrap.libp2p.io/p2p/QmcZf59bWwK5XFi76CZX8cbJ4BhTzzA3gU1ZjYZcYW3dwt",
		"/ip4/104.131.131.82/tcp/4001/p2p/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ", // mars.i.ipfs.io
	} {
		ma, err := multiaddr.NewMultiaddr(s)
		if err != nil {
			panic(err)
		}
		DefaultBootstrapPeers = append(DefaultBootstrapPeers, ma)
	}
}

// startSelfLookup starts a go-routine that listens for requests to trigger a self walk on a dedicated channel
// and then sends the error status back on the error channel sent along with the request.
// if multiple callers "simultaneously" ask for a self walk, it performs ONLY one self walk and sends the same error status to all of them.
func (dht *KadDHT) startSelfLookup() {
	dht.proc.Go(func(proc process.Process) {
		ctx := processctx.WithProcessClosing(dht.ctx, proc)
		for {
			var waiting []chan<- error
			select {
			case res := <-dht.triggerSelfLookup:
				if res != nil {
					waiting = append(waiting, res)
				}
			case <-ctx.Done():
				return
			}

			// batch multiple refresh requests if they're all waiting at the same time.
			waiting = append(waiting, collectWaitingChannels(dht.triggerSelfLookup)...)

			// Do a self walk
			queryCtx, cancel := context.WithTimeout(ctx, dht.rtRefreshQueryTimeout)
			_, err := dht.GetClosestPeers(queryCtx, string(dht.self))
			if err == kbucket.ErrLookupFailure {
				err = nil
			} else if err != nil {
				err = fmt.Errorf("failed to query self during routing table refresh: %s", err)
			}
			cancel()

			// send back the error status
			for _, w := range waiting {
				w <- err
				close(w)
			}
			if err != nil {
				logger.Warnw("self lookup failed", "error", err)
			}
		}
	})
}

// Start the refresh worker.
func (dht *KadDHT) startRefreshing() {
	// scan the RT table periodically & do a random walk for cpl's that haven't been queried since the given period
	dht.proc.Go(func(proc process.Process) {
		ctx := processctx.WithProcessClosing(dht.ctx, proc)

		refreshTicker := time.NewTicker(dht.rtRefreshInterval)
		defer refreshTicker.Stop()

		// refresh if option is set
		if dht.autoRefresh {
			err := dht.doRefresh(ctx)
			if err != nil {
				logger.Warn("failed when refreshing routing table", err)
			}
		} else {
			// disable the "auto-refresh" ticker so that no more ticks are sent to this channel
			refreshTicker.Stop()
		}

		for {
			var waiting []chan<- error
			select {
			case <-refreshTicker.C:
			case res := <-dht.triggerRtRefresh:
				if res != nil {
					waiting = append(waiting, res)
				}
			case <-ctx.Done():
				return
			}

			// Batch multiple refresh requests if they're all waiting at the same time.
			waiting = append(waiting, collectWaitingChannels(dht.triggerRtRefresh)...)

			err := dht.doRefresh(ctx)
			for _, w := range waiting {
				w <- err
				close(w)
			}
			if err != nil {
				logger.Warnw("failed when refreshing routing table", "error", err)
			}

			// ping Routing Table peers that haven't been hear of/from in the interval they should have been.
			for _, ps := range dht.routingTable.GetPeerInfos() {
				// ping the peer if it's due for a ping and evict it if the ping fails
				if time.Since(ps.LastSuccessfulOutboundQuery) > dht.maxLastSuccessfulOutboundThreshold {
					livelinessCtx, cancel := context.WithTimeout(ctx, peerPingTimeout)
					if err := dht.host.Connect(livelinessCtx, peer.AddrInfo{ID: ps.Id}); err != nil {
						logger.Debugw("evicting peer after failed ping", "peer", ps.Id, "error", err)
						dht.routingTable.RemovePeer(ps.Id)
					}
					cancel()
				}
			}

		}
	})
}

func collectWaitingChannels(source chan chan<- error) []chan<- error {
	var waiting []chan<- error
	for {
		select {
		case res := <-source:
			if res != nil {
				waiting = append(waiting, res)
			}
		default:
			return waiting
		}
	}
}

func (dht *KadDHT) doRefresh(ctx context.Context) error {
	var merr error

	// wait for the self walk result
	selfWalkres := make(chan error, 1)

	select {
	case dht.triggerSelfLookup <- selfWalkres:
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case err := <-selfWalkres:
		if err != nil {
			merr = multierror.Append(merr, err)
		}
	case <-ctx.Done():
		return ctx.Err()
	}

	if err := dht.refreshCpls(ctx); err != nil {
		merr = multierror.Append(merr, err)
	}
	return merr
}

// refreshCpls scans the routing table, and does a random walk for cpl's that haven't been queried since the given period
func (dht *KadDHT) refreshCpls(ctx context.Context) error {
	doQuery := func(cpl uint, target string, f func(context.Context) error) error {
		logger.Infof("starting refreshing cpl %d to %s (routing table size was %d)",
			cpl, target, dht.routingTable.Size())
		defer func() {
			logger.Infof("finished refreshing cpl %d to %s (routing table size is now %d)",
				cpl, target, dht.routingTable.Size())
		}()
		queryCtx, cancel := context.WithTimeout(ctx, dht.rtRefreshQueryTimeout)
		defer cancel()
		err := f(queryCtx)
		if err == context.DeadlineExceeded && queryCtx.Err() == context.DeadlineExceeded && ctx.Err() == nil {
			return nil
		}
		return err
	}

	trackedCpls := dht.routingTable.GetTrackedCplsForRefresh()

	var merr error
	for cpl, lastRefreshedAt := range trackedCpls {
		if time.Since(lastRefreshedAt) <= dht.rtRefreshInterval {
			continue
		}

		// gen rand peer with the cpl
		randPeer, err := dht.routingTable.GenRandPeerID(uint(cpl))
		if err != nil {
			logger.Errorw("failed to generate peer ID", "cpl", cpl, "error", err)
			continue
		}

		// walk to the generated peer
		walkFnc := func(c context.Context) error {
			_, err := dht.GetClosestPeers(c, string(randPeer))
			return err
		}

		if err := doQuery(uint(cpl), randPeer.String(), walkFnc); err != nil {
			merr = multierror.Append(
				merr,
				fmt.Errorf("failed to do a random walk for cpl %d: %w", cpl, err),
			)
		}
	}
	return merr
}

// Bootstrap tells the DHT to get into a bootstrapped state satisfying the
// IpfsRouter interface.
//
// This just calls `RefreshRoutingTable`.
func (dht *KadDHT) Bootstrap(_ context.Context) error {
	dht.RefreshRoutingTable()
	return nil
}

// RefreshRoutingTable tells the DHT to refresh it's routing tables.
//
// The returned channel will block until the refresh finishes, then yield the
// error and close. The channel is buffered and safe to ignore.
func (dht *KadDHT) RefreshRoutingTable() <-chan error {
	res := make(chan error, 1)
	select {
	case dht.triggerRtRefresh <- res:
	case <-dht.ctx.Done():
		res <- dht.ctx.Err()
	}
	return res
}
