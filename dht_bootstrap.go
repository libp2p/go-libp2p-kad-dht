package dht

import (
	"context"
	"fmt"
	"time"

	"github.com/opentracing/opentracing-go"

	multierror "github.com/hashicorp/go-multierror"
	process "github.com/jbenet/goprocess"
	processctx "github.com/jbenet/goprocess/context"
	"github.com/libp2p/go-libp2p-core/routing"
	"github.com/multiformats/go-multiaddr"
	_ "github.com/multiformats/go-multiaddr-dns"
)

var DefaultBootstrapPeers []multiaddr.Multiaddr

// Minimum number of peers in the routing table. If we drop below this and we
// see a new peer, we trigger a bootstrap round.
var minRTRefreshThreshold = 4

func init() {
	for _, s := range []string{
		"/dnsaddr/bootstrap.libp2p.io/ipfs/QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN",
		"/dnsaddr/bootstrap.libp2p.io/ipfs/QmQCU2EcMqAqQPR2i9bChDtGNJchTbq5TbXJJ16u19uLTa",
		"/dnsaddr/bootstrap.libp2p.io/ipfs/QmbLHAnMoJPWSCR5Zhtx6BHJX9KiKNN6tpvbUcqanj75Nb",
		"/dnsaddr/bootstrap.libp2p.io/ipfs/QmcZf59bWwK5XFi76CZX8cbJ4BhTzzA3gU1ZjYZcYW3dwt",
		"/ip4/104.131.131.82/tcp/4001/ipfs/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ",            // mars.i.ipfs.io
		"/ip4/104.236.179.241/tcp/4001/ipfs/QmSoLPppuBtQSGwKDZT2M73ULpjvfd3aZ6ha4oFGL1KrGM",           // pluto.i.ipfs.io
		"/ip4/128.199.219.111/tcp/4001/ipfs/QmSoLSafTMBsPKadTEgaXctDQVcqN88CNLHXMkTNwMKPnu",           // saturn.i.ipfs.io
		"/ip4/104.236.76.40/tcp/4001/ipfs/QmSoLV4Bbm51jM9C4gDYZQ9Cy3U6aXMJDAbzgu2fzaDs64",             // venus.i.ipfs.io
		"/ip4/178.62.158.247/tcp/4001/ipfs/QmSoLer265NRgSp2LA3dPaeykiS1J6DifTC88f5uVQKNAd",            // earth.i.ipfs.io
		"/ip6/2604:a880:1:20::203:d001/tcp/4001/ipfs/QmSoLPppuBtQSGwKDZT2M73ULpjvfd3aZ6ha4oFGL1KrGM",  // pluto.i.ipfs.io
		"/ip6/2400:6180:0:d0::151:6001/tcp/4001/ipfs/QmSoLSafTMBsPKadTEgaXctDQVcqN88CNLHXMkTNwMKPnu",  // saturn.i.ipfs.io
		"/ip6/2604:a880:800:10::4a:5001/tcp/4001/ipfs/QmSoLV4Bbm51jM9C4gDYZQ9Cy3U6aXMJDAbzgu2fzaDs64", // venus.i.ipfs.io
		"/ip6/2a03:b0c0:0:1010::23:1001/tcp/4001/ipfs/QmSoLer265NRgSp2LA3dPaeykiS1J6DifTC88f5uVQKNAd", // earth.i.ipfs.io
	} {
		ma, err := multiaddr.NewMultiaddr(s)
		if err != nil {
			panic(err)
		}
		DefaultBootstrapPeers = append(DefaultBootstrapPeers, ma)
	}
}

// Start the refresh worker.
func (dht *IpfsDHT) startRefreshing() error {
	// scan the RT table periodically & do a random walk on k-buckets that haven't been queried since the given bucket period
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
		collectWaiting:
			for {
				select {
				case res := <-dht.triggerRtRefresh:
					if res != nil {
						waiting = append(waiting, res)
					}
				default:
					break collectWaiting
				}
			}

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

	return nil
}

func (dht *IpfsDHT) doRefresh(ctx context.Context) error {
	var merr error
	sp, ctx := opentracing.StartSpanFromContext(context.Background(), "routing_table_refresh", opentracing.Tags{
		"system":    "dht",
		"component": "routing_table",
	})
	defer LinkedFinish(sp, &merr)

	if err := dht.selfWalk(ctx); err != nil {
		merr = multierror.Append(merr, err)
	}
	if err := dht.refreshBuckets(ctx); err != nil {
		merr = multierror.Append(merr, err)
	}
	return merr
}

// refreshBuckets scans the routing table, and does a random walk on k-buckets
// that haven't been queried since the given bucket period.
func (dht *IpfsDHT) refreshBuckets(ctx context.Context) error {
	var merr error
	sp, ctx := opentracing.StartSpanFromContext(context.Background(), "refresh_buckets", opentracing.Tags{
		"system": "dht",
	})
	defer LinkedFinish(sp, &merr)

	doQuery := func(bucketId int, target string, f func(context.Context) error) error {
		var err error
		sp, ctx := opentracing.StartSpanFromContext(context.Background(), "refresh_bucket", opentracing.Tags{
			"system": "dht",
			"bucket": bucketId,
			"target": target,
		})
		defer LinkedFinish(sp, &err)

		logger.Infof(
			"starting refreshing bucket %d to %s (routing table size was %d)",
			bucketId,
			target,
			dht.routingTable.Size())

		defer func() {
			logger.Infof(
				"finished refreshing bucket %d to %s (routing table size is now %d)",
				bucketId,
				target,
				dht.routingTable.Size())
		}()

		queryCtx, cancel := context.WithTimeout(ctx, dht.rtRefreshQueryTimeout)
		defer cancel()

		if err = f(queryCtx); err == context.DeadlineExceeded && queryCtx.Err() == context.DeadlineExceeded && ctx.Err() == nil {
			return nil
		}

		return err
	}

	buckets := dht.routingTable.GetAllBuckets()
	if len(buckets) > 16 {
		// Don't bother bootstrapping more than 16 buckets.
		// GenRandPeerID can't generate target peer IDs with more than
		// 16 bits specified anyways.
		buckets = buckets[:16]
	}

	for bucketID, bucket := range buckets {
		if time.Since(bucket.RefreshedAt()) <= dht.rtRefreshPeriod {
			continue
		}
		// gen rand peer in the bucket
		randPeerInBucket := dht.routingTable.GenRandPeerID(bucketID)

		// walk to the generated peer
		walkFnc := func(c context.Context) error {
			_, err := dht.FindPeer(c, randPeerInBucket)
			if err == routing.ErrNotFound {
				return nil
			}
			return err
		}

		if err := doQuery(bucketID, randPeerInBucket.String(), walkFnc); err != nil {
			merr = multierror.Append(
				merr,
				fmt.Errorf("failed to do a random walk on bucket %d: %s", bucketID, err),
			)
		}
	}
	return merr
}

// Traverse the DHT toward the self ID
func (dht *IpfsDHT) selfWalk(ctx context.Context) error {
	var err error
	sp, ctx := opentracing.StartSpanFromContext(context.Background(), "self_walk", opentracing.Tags{
		"system": "dht",
	})
	defer LinkedFinish(sp, &err)

	queryCtx, cancel := context.WithTimeout(ctx, dht.rtRefreshQueryTimeout)
	defer cancel()
	_, err = dht.FindPeer(queryCtx, dht.self)
	if err == routing.ErrNotFound {
		err = nil
		return nil
	}
	err = fmt.Errorf("failed to query self during routing table refresh: %s", err)
	return err
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
	select {
	case dht.triggerRtRefresh <- res:
	default:
	}
	return res
}
