package dht

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	process "github.com/jbenet/goprocess"
	processctx "github.com/jbenet/goprocess/context"
	"github.com/libp2p/go-libp2p-core/routing"
	"github.com/multiformats/go-multiaddr"
	_ "github.com/multiformats/go-multiaddr-dns"
	"github.com/pkg/errors"
)

var DefaultBootstrapPeers []multiaddr.Multiaddr

var minRTBootstrapThreshold = 4

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

type bootstrapReq struct {
	errChan chan error
}

func makeBootstrapReq() *bootstrapReq {
	errChan := make(chan error, 1)
	return &bootstrapReq{errChan}
}

// Bootstrap  i
func (dht *IpfsDHT) startBootstrapping() error {
	// scan the RT table periodically & do a random walk on k-buckets that haven't been queried since the given bucket period
	dht.proc.Go(func(proc process.Process) {
		ctx := processctx.OnClosingContext(proc)

		scanInterval := time.NewTicker(dht.bootstrapCfg.BucketPeriod)
		defer scanInterval.Stop()

		// run bootstrap if option is set
		if dht.triggerAutoBootstrap {
			if err := dht.doBootstrap(ctx, true); err != nil {
				logger.Warningf("bootstrap error: %s", err)
			}
		} else {
			// disable the "auto-bootstrap" ticker so that no more ticks are sent to his channel
			scanInterval.Stop()
		}

		for {
			select {
			case now := <-scanInterval.C:
				walkSelf := now.After(dht.latestSelfWalk.Add(dht.bootstrapCfg.SelfQueryInterval))
				if err := dht.doBootstrap(ctx, walkSelf); err != nil {
					logger.Warning("bootstrap error: %s", err)
				}
			case req := <-dht.triggerBootstrap:
				logger.Infof("triggering a bootstrap: RT has %d peers", dht.routingTable.Size())
				err := dht.doBootstrap(ctx, true)
				select {
				case req.errChan <- err:
					close(req.errChan)
				default:
				}
			case <-ctx.Done():
				return
			}
		}
	})

	return nil
}

func (dht *IpfsDHT) doBootstrap(ctx context.Context, walkSelf bool) error {
	if walkSelf {
		if err := dht.selfWalk(ctx); err != nil {
			return fmt.Errorf("self walk: error: %s", err)
		}
		dht.latestSelfWalk = time.Now()
	}

	if err := dht.bootstrapBuckets(ctx); err != nil {
		return fmt.Errorf("bootstrap buckets: error bootstrapping: %s", err)
	}

	return nil
}

// bootstrapBuckets scans the routing table, and does a random walk on k-buckets that haven't been queried since the given bucket period
func (dht *IpfsDHT) bootstrapBuckets(ctx context.Context) error {
	doQuery := func(bucketId int, target string, f func(context.Context) error) error {
		logger.Infof("starting bootstrap query for bucket %d to %s (routing table size was %d)",
			bucketId, target, dht.routingTable.Size())
		defer func() {
			logger.Infof("finished bootstrap query for bucket %d to %s (routing table size is now %d)",
				bucketId, target, dht.routingTable.Size())
		}()
		queryCtx, cancel := context.WithTimeout(ctx, dht.bootstrapCfg.Timeout)
		defer cancel()
		err := f(queryCtx)
		if err == context.DeadlineExceeded && queryCtx.Err() == context.DeadlineExceeded && ctx.Err() == nil {
			return nil
		}
		return err
	}

	buckets := dht.routingTable.GetAllBuckets()
	var wg sync.WaitGroup
	errChan := make(chan error)

	for bucketID, bucket := range buckets {
		if time.Since(bucket.RefreshedAt()) > dht.bootstrapCfg.BucketPeriod {
			wg.Add(1)
			go func(bucketID int, errChan chan<- error) {
				defer wg.Done()
				// gen rand peer in the bucket
				randPeerInBucket := dht.routingTable.GenRandPeerID(bucketID)

				// walk to the generated peer
				walkFnc := func(c context.Context) error {
					_, err := dht.FindPeer(ctx, randPeerInBucket)
					if err == routing.ErrNotFound {
						return nil
					}
					return err
				}

				if err := doQuery(bucketID, randPeerInBucket.String(), walkFnc); err != nil {
					errChan <- errors.Wrapf(err, "failed to do a random walk on bucket %d", bucketID)
				}
			}(bucketID, errChan)
		}
	}

	// wait for all walks to finish & close the error channel
	go func() {
		wg.Wait()
		close(errChan)
	}()

	// accumulate errors from all go-routines. ensures wait group is completed by reading errChan until closure.
	var errStrings []string
	for err := range errChan {
		errStrings = append(errStrings, err.Error())
	}
	if len(errStrings) == 0 {
		return nil
	} else {
		return fmt.Errorf("errors encountered while running bootstrap on RT:\n%s", strings.Join(errStrings, "\n"))
	}
}

// Traverse the DHT toward the self ID
func (dht *IpfsDHT) selfWalk(ctx context.Context) error {
	queryCtx, cancel := context.WithTimeout(ctx, dht.bootstrapCfg.Timeout)
	defer cancel()
	_, err := dht.FindPeer(queryCtx, dht.self)
	if err == routing.ErrNotFound {
		return nil
	}
	return err
}

// Bootstrap tells the DHT to get into a bootstrapped state.
//
// Note: the context is ignored.
func (dht *IpfsDHT) Bootstrap(_ context.Context) error {
	select {
	case dht.triggerBootstrap <- makeBootstrapReq():
	default:
	}
	return nil
}
