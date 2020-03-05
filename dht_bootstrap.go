package dht

import (
	"context"
	"fmt"
	"time"

	"github.com/libp2p/go-libp2p-core/event"
	"github.com/libp2p/go-libp2p-core/routing"

	multierror "github.com/hashicorp/go-multierror"
	process "github.com/jbenet/goprocess"
	processctx "github.com/jbenet/goprocess/context"
	"github.com/multiformats/go-multiaddr"
	_ "github.com/multiformats/go-multiaddr-dns"
)

var DefaultBootstrapPeers []multiaddr.Multiaddr

// Minimum number of peers in the routing table. If we drop below this and we
// see a new peer, we trigger a bootstrap round.
var minRTRefreshThreshold = 20

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
func (dht *IpfsDHT) startSelfLookup() error {
	dht.proc.Go(func(proc process.Process) {
		ctx := processctx.OnClosingContext(proc)
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
			defer cancel()
			_, err := dht.FindPeer(queryCtx, dht.self)
			if err == routing.ErrNotFound {
				err = nil
			} else if err != nil {
				err = fmt.Errorf("failed to query self during routing table refresh: %s", err)
			}

			// send back the error status
			for _, w := range waiting {
				w <- err
				close(w)
			}
			if err != nil {
				logger.Warning(err)
			}
		}
	})

	return nil
}

// Start the refresh worker.
func (dht *IpfsDHT) startRefreshing() error {
	// scan the RT table periodically & do a random walk for cpl's that haven't been queried since the given period
	dht.proc.Go(func(proc process.Process) {
		ctx := processctx.OnClosingContext(proc)

		refreshTicker := time.NewTicker(dht.rtRefreshPeriod)
		defer refreshTicker.Stop()

		// refresh if option is set
		if dht.autoRefresh {
			dht.doRefresh(ctx)
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
			waiting = append(waiting, collectWaitingChannels(dht.triggerSelfLookup)...)

			err := dht.doRefresh(ctx)
			for _, w := range waiting {
				w <- err
				close(w)
			}
			if err != nil {
				logger.Warning(err)
			}
		}
	})

	// when our address changes, we should proactively tell our closest peers about it so
	// we become discoverable quickly. The Identify protocol will push a signed peer record
	// with our new address to all peers we are connected to. However, we might not necessarily be connected
	// to our closet peers & so in the true spirit of Zen, searching for ourself in the network really is the best way
	// to to forge connections with those matter.
	dht.proc.Go(func(proc process.Process) {
		for {
			select {
			case evt, more := <-dht.subscriptions.everPeerAddressChanged.Out():
				if !more {
					return
				}
				if _, ok := evt.(event.EvtLocalAddressesUpdated); ok {
					// our address has changed, trigger a self walk so our closet peers know about it
					select {
					case dht.triggerSelfLookup <- nil:
					default:

					}
				} else {
					logger.Error("should not get an event other than EvtLocalAddressesUpdated on that subscription")
				}
			case <-proc.Closing():
				return
			}
		}
	})

	return nil
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

func (dht *IpfsDHT) doRefresh(ctx context.Context) error {
	var merr error

	// wait for self walk result
	selfWalkres := make(chan error, 1)
	dht.triggerSelfLookup <- selfWalkres
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
func (dht *IpfsDHT) refreshCpls(ctx context.Context) error {
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
	for _, tcpl := range trackedCpls {
		if time.Since(tcpl.LastRefreshAt) <= dht.rtRefreshPeriod {
			continue
		}

		// do not refresh if bucket is full
		if dht.routingTable.IsBucketFull(tcpl.Cpl) {
			continue
		}

		// gen rand peer with the cpl
		randPeer, err := dht.routingTable.GenRandPeerID(tcpl.Cpl)
		if err != nil {
			logger.Errorf("failed to generate peerID for cpl %d, err: %s", tcpl.Cpl, err)
			continue
		}

		// walk to the generated peer
		walkFnc := func(c context.Context) error {
			_, err := dht.FindPeer(c, randPeer)
			if err == routing.ErrNotFound {
				return nil
			}
			return err
		}

		if err := doQuery(tcpl.Cpl, randPeer.String(), walkFnc); err != nil {
			merr = multierror.Append(
				merr,
				fmt.Errorf("failed to do a random walk for cpl %d: %s", tcpl.Cpl, err),
			)
		}
	}
	return merr
}

// Bootstrap tells the DHT to get into a bootstrapped state satisfying the
// IpfsRouter interface.
//
// This just calls `RefreshRoutingTable`.
func (dht *IpfsDHT) Bootstrap(_ context.Context) error {
	dht.RefreshRoutingTable()
	return nil
}

// RefreshRoutingTable tells the DHT to refresh it's routing tables.
//
// The returned channel will block until the refresh finishes, then yield the
// error and close. The channel is buffered and safe to ignore.
func (dht *IpfsDHT) RefreshRoutingTable() <-chan error {
	res := make(chan error, 1)
	dht.triggerRtRefresh <- res
	return res
}
