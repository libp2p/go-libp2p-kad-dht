package dht

import (
	"context"
	"crypto/rand"
	"fmt"
	"strings"
	"sync"
	"time"

	u "github.com/ipfs/go-ipfs-util"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/routing"
	"github.com/multiformats/go-multiaddr"
	_ "github.com/multiformats/go-multiaddr-dns"
	"github.com/pkg/errors"
)

var DefaultBootstrapPeers []multiaddr.Multiaddr

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

// BootstrapConfig specifies parameters used for bootstrapping the DHT.
type BootstrapConfig struct {
	BucketPeriod             time.Duration // how long to wait for a k-bucket to be queried before doing a random walk on it
	Timeout                  time.Duration // how long to wait for a bootstrap query to run
	RoutingTableScanInterval time.Duration // how often to scan the RT for k-buckets that haven't been queried since the given period
	SelfQueryInterval        time.Duration // how often to query for self
}

var DefaultBootstrapConfig = BootstrapConfig{
	// same as that mentioned in the kad dht paper
	BucketPeriod: 1 * time.Hour,

	// since the default bucket period is 1 hour, a scan interval of 30 minutes sounds reasonable
	RoutingTableScanInterval: 5 * time.Minute,

	Timeout: 10 * time.Second,

	SelfQueryInterval: 5 * time.Hour,
}

// A method in the IpfsRouting interface. It calls BootstrapWithConfig with
// the default bootstrap config.
func (dht *IpfsDHT) Bootstrap(ctx context.Context) error {
	return dht.BootstrapWithConfig(ctx, DefaultBootstrapConfig)
}

// Runs cfg.Queries bootstrap queries every cfg.BucketPeriod.
func (dht *IpfsDHT) BootstrapWithConfig(ctx context.Context, cfg BootstrapConfig) error {
	// we should query for self periodically so we can discover closer peers
	go func() {
		for {
			dht.logBuckets("before self bootstrap")
			err := dht.BootstrapSelf(ctx)
			if err != nil {
				logger.Warningf("error bootstrapping while searching for my self (I'm Too Shallow ?): %s", err)
			}
			select {
			case <-time.After(cfg.SelfQueryInterval):
			case <-ctx.Done():
				return
			}
			dht.logBuckets("after self bootstrap")
		}
	}()

	// scan the RT table periodically & do a random walk on k-buckets that haven't been queried since the given bucket period
	go func() {
		for {
			dht.logBuckets("before bootstrap")
			err := dht.runBootstrap(ctx, cfg)
			if err != nil {
				logger.Warningf("error bootstrapping: %s", err)
			}
			select {
			case <-time.After(cfg.RoutingTableScanInterval):
			case <-dht.triggerBootstrap:
				dht.logBuckets("before self bootstrap")
				dht.BootstrapSelf(ctx)
				dht.logBuckets("after self bootstrap")
			case <-ctx.Done():
				return
			}
			dht.logBuckets("after bootstrap")
		}
	}()
	return nil
}

func newRandomPeerId() peer.ID {
	id := make([]byte, 32) // SHA256 is the default. TODO: Use a more canonical way to generate random IDs.
	rand.Read(id)
	id = u.Hash(id) // TODO: Feed this directly into the multihash instead of hashing it.
	return peer.ID(id)
}

// Traverse the DHT toward the given ID.
func (dht *IpfsDHT) walk(ctx context.Context, target peer.ID) (peer.AddrInfo, error) {
	// TODO: Extract the query action (traversal logic?) inside FindPeer,
	// don't actually call through the FindPeer machinery, which can return
	// things out of the peer store etc.
	return dht.FindPeer(ctx, target)
}

// Traverse the DHT toward a random ID.
func (dht *IpfsDHT) randomWalk(ctx context.Context) error {
	id := newRandomPeerId()
	p, err := dht.walk(ctx, id)
	switch err {
	case routing.ErrNotFound:
		return nil
	case nil:
		// We found a peer from a randomly generated ID. This should be very
		// unlikely.
		logger.Warningf("random walk toward %s actually found peer: %s", id, p)
		return nil
	default:
		return err
	}
}

// Traverse the DHT toward the self ID
func (dht *IpfsDHT) selfWalk(ctx context.Context) error {
	_, err := dht.walk(ctx, dht.self)
	if err == routing.ErrNotFound {
		return nil
	}
	return err
}

func (dht *IpfsDHT) logBuckets(when string) {
	fmt.Printf("checking buckets: %s (%s)\n", when, time.Now())
	for i, b := range dht.routingTable.GetAllBuckets() {
		fmt.Printf("  bucket %d has %d peers\n", i, b.Len())
	}
	fmt.Println("")
}

//scan the RT,& do a random walk on k-buckets that haven't been queried since the given bucket period
func (dht *IpfsDHT) runBootstrap(ctx context.Context, cfg BootstrapConfig) error {
	doQuery := func(n int, target string, f func(context.Context) error) error {
		logger.Infof("starting bootstrap query for bucket %d to %s (routing table size was %d)",
			n, target, dht.routingTable.Size())
		defer func() {
			logger.Infof("finished bootstrap query for bucket %d to %s (routing table size is now %d)",
				n, target, dht.routingTable.Size())
		}()
		queryCtx, cancel := context.WithTimeout(ctx, cfg.Timeout)
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
		if time.Since(bucket.RefreshedAt()) > cfg.BucketPeriod {
			wg.Add(1)
			go func(bucketID int, errChan chan<- error) {
				defer wg.Done()
				// gen rand peer in the bucket
				randPeerInBucket := dht.routingTable.GenRandPeerID(bucketID)

				// walk to the generated peer
				walkFnc := func(c context.Context) error {
					_, err := dht.walk(ctx, randPeerInBucket)
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

	// accumulate errors from all go-routines
	var errStrings []string
	for err := range errChan {
		errStrings = append(errStrings, err.Error())
	}
	if len(errStrings) == 0 {
		return nil
	} else {
		return fmt.Errorf("errors encountered while running bootstrap on RT: %s", strings.Join(errStrings, "\n"))
	}
}

// This is a synchronous bootstrap.
func (dht *IpfsDHT) BootstrapOnce(ctx context.Context, cfg BootstrapConfig) error {
	if err := dht.BootstrapSelf(ctx); err != nil {
		return errors.Wrap(err, "failed bootstrap while searching for self")
	} else {
		return dht.runBootstrap(ctx, cfg)
	}
}

func (dht *IpfsDHT) BootstrapRandom(ctx context.Context) error {
	return dht.randomWalk(ctx)
}

func (dht *IpfsDHT) BootstrapSelf(ctx context.Context) error {
	return dht.selfWalk(ctx)
}
