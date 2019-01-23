package dht

import (
	"context"
	"crypto/rand"
	"fmt"
	"time"

	u "github.com/ipfs/go-ipfs-util"
	peer "github.com/libp2p/go-libp2p-peer"
	peerstore "github.com/libp2p/go-libp2p-peerstore"
	routing "github.com/libp2p/go-libp2p-routing"
)

// BootstrapConfig specifies parameters used bootstrapping the DHT.
//
// Note there is a tradeoff between the bootstrap period and the
// number of queries. We could support a higher period with less
// queries.
type BootstrapConfig struct {
	Queries int           // how many queries to run per period
	Period  time.Duration // how often to run periodic bootstrap.
	Timeout time.Duration // how long to wait for a bootstrap query to run
}

var DefaultBootstrapConfig = BootstrapConfig{
	// For now, this is set to 1 query.
	// We are currently more interested in ensuring we have a properly formed
	// DHT than making sure our dht minimizes traffic. Once we are more certain
	// of our implementation's robustness, we should lower this down to 8 or 4.
	Queries: 1,

	// For now, this is set to 5 minutes, which is a medium period. We are
	// We are currently more interested in ensuring we have a properly formed
	// DHT than making sure our dht minimizes traffic.
	Period: time.Duration(5 * time.Minute),

	Timeout: time.Duration(10 * time.Second),
}

// A method in the IpfsRouting interface. It calls BootstrapWithConfig with
// the default bootstrap config.
func (dht *IpfsDHT) Bootstrap(ctx context.Context) error {
	return dht.BootstrapWithConfig(ctx, DefaultBootstrapConfig)
}

// Runs cfg.Queries bootstrap queries every cfg.Period.
func (dht *IpfsDHT) BootstrapWithConfig(ctx context.Context, cfg BootstrapConfig) error {
	// Because this method is not synchronous, we have to duplicate sanity
	// checks on the config so that callers aren't oblivious.
	if cfg.Queries <= 0 {
		return fmt.Errorf("invalid number of queries: %d", cfg.Queries)
	}
	go func() {
		for {
			err := dht.runBootstrap(ctx, cfg)
			if err != nil {
				log.Warningf("error bootstrapping: %s", err)
			}
			select {
			case <-time.After(cfg.Period):
			case <-ctx.Done():
				return
			}
		}
	}()
	return nil
}

// This is a synchronous bootstrap. cfg.Queries queries will run each with a
// timeout of cfg.Timeout. cfg.Period is not used.
func (dht *IpfsDHT) BootstrapOnce(ctx context.Context, cfg BootstrapConfig) error {
	if cfg.Queries <= 0 {
		return fmt.Errorf("invalid number of queries: %d", cfg.Queries)
	}
	return dht.runBootstrap(ctx, cfg)
}

func newRandomPeerId() peer.ID {
	id := make([]byte, 32) // SHA256 is the default. TODO: Use a more canonical way to generate random IDs.
	rand.Read(id)
	id = u.Hash(id) // TODO: Feed this directly into the multihash instead of hashing it.
	return peer.ID(id)
}

// Traverse the DHT toward the given ID.
func (dht *IpfsDHT) walk(ctx context.Context, target peer.ID) (peerstore.PeerInfo, error) {
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
		log.Warningf("random walk toward %s actually found peer: %s", id, p)
		return nil
	default:
		return err
	}
}

// runBootstrap builds up list of peers by requesting random peer IDs
func (dht *IpfsDHT) runBootstrap(ctx context.Context, cfg BootstrapConfig) error {
	bslog := func(msg string) {
		log.Debugf("DHT %s dhtRunBootstrap %s -- routing table size: %d", dht.self, msg, dht.routingTable.Size())
	}
	bslog("start")
	defer bslog("end")
	defer log.EventBegin(ctx, "dhtRunBootstrap").Done()

	doQuery := func(n int, target string, f func(context.Context) error) error {
		log.Debugf("Bootstrapping query (%d/%d) to %s", n, cfg.Queries, target)
		ctx, cancel := context.WithTimeout(ctx, cfg.Timeout)
		defer cancel()
		return f(ctx)
	}

	// Do all but one of the bootstrap queries as random walks.
	for i := 1; i < cfg.Queries; i++ {
		err := doQuery(i, "random ID", dht.randomWalk)
		if err != nil {
			return err
		}
	}

	// Find self to distribute peer info to our neighbors.
	return doQuery(cfg.Queries, fmt.Sprintf("self: %s", dht.self), func(ctx context.Context) error {
		_, err := dht.walk(ctx, dht.self)
		return err
	})
}
