package persist

import (
	"context"
	"errors"
	"math/rand"
	"time"

	"github.com/ipfs/go-todocounter"

	"github.com/libp2p/go-libp2p-host"
	"github.com/libp2p/go-libp2p-kbucket"
	inet "github.com/libp2p/go-libp2p-net"
	"github.com/libp2p/go-libp2p-peer"
)

// SeedDialGracePeriod is the grace period for one dial attempt
var SeedDialGracePeriod = 5 * time.Second

// TotalSeedDialGracePeriod is the total grace period for a group of dial attempts
// TODO Make this configurable so it's easy to run tests
var TotalSeedDialGracePeriod = 30 * time.Second

// NSimultaneousDial is the number of peers we will dial simultaneously
var NSimultaneousDial = 50

var ErrPartialSeed = errors.New("routing table seeded partially")

type randomSeeder struct {
	host   host.Host
	target int
}

var _ Seeder = (*randomSeeder)(nil)

// NewRandomSeeder returns a Seeder that seeds a routing table with `target` random peers from
// the supplied candidate set, resorting to fallback peers if the candidates are unworkable.
//
// The fallback peers are guaranteed to exist in the peerstore.
func NewRandomSeeder(host host.Host, target int) Seeder {
	return &randomSeeder{host, target}
}

func (rs *randomSeeder) Seed(into *kbucket.RoutingTable, candidates []peer.ID, fallback []peer.ID) error {
	// copy the candidates & shuffle to randomize seeding
	cpy := make([]peer.ID, len(candidates))
	copy(cpy, candidates)
	rand.Shuffle(len(cpy), func(i, j int) {
		cpy[i], cpy[j] = cpy[j], cpy[i]
	})

	left := rs.target
	addPeer := func(p peer.ID) { // adds a peer to the routing table and decrements the left counter if successful.
		evicted, err := into.Update(p)
		if err == nil && evicted == "" {
			left-- // if we evict a peer, do not decrement the counter.
		} else if err != nil {
			logSeed.Warningf("error while adding candidate to routing table: %s", err)
		}
	}

	// make a list of the candidates we need to dial to to ensure they are available
	var todial []peer.ID
	for _, p := range cpy {
		if left == 0 {
			return nil // lucky case: we were already connected to all our candidates.
		}
		if rs.host.Network().Connectedness(p) == inet.Connected {
			// Note: initial seeding MUST be done before dht registers for connection notifications
			// this is because once we register for notifs, peers will get added to the RT upon connection itself
			// we don't want that as it will wrongly mark the peer as more active in the k-bucket & move it to the front of the bucket
			// take a look at notif.go
			// the long term solution to this problem is being discussed at https://github.com/libp2p/go-libp2p-kad-dht/issues/283
			addPeer(p)
			continue
		}
		if addrs := rs.host.Peerstore().Addrs(p); len(addrs) == 0 {
			logSeed.Infof("discarding routing table candidate as we no longer have addresses: %s", p)
			continue
		}
		todial = append(todial, p)
	}

	type result struct {
		p   peer.ID
		err error
	}

	// attempts to dial to a given peer to verify it's available
	dialFn := func(ctx context.Context, p peer.ID, res chan<- result) {
		childCtx, cancel := context.WithTimeout(ctx, SeedDialGracePeriod)
		defer cancel()
		_, err := rs.host.Network().DialPeer(childCtx, p)
		select {
		case <-ctx.Done(): // caller has already hung up & gone away
		case res <- result{p, err}:
		}
	}

	// attempt to seed the RT with the given peers
	attemptSeedWithPeers := func(peers []peer.ID) error {
		resCh := make(chan result) // dial results.
		ctx, cancel := context.WithTimeout(context.Background(), TotalSeedDialGracePeriod)
		defer cancel()

		// start dialing
		sempahore := make(chan struct{}, NSimultaneousDial)
		go func(peers []peer.ID) {
			for _, p := range peers {
				sempahore <- struct{}{}
				go func(p peer.ID, res chan<- result) {
					dialFn(ctx, p, resCh)
					<-sempahore
				}(p, resCh)
			}
		}(peers)

		// number of results we expect on the result channel
		todo := todocounter.NewSyncCounter()
		todo.Increment(uint32(len(peers)))

		for left > 0 {
			select {
			case res := <-resCh:
				todo.Decrement(1)
				if res.err != nil {
					logSeed.Infof("discarded routing table candidate due to dial error; peer ID: %s, err: %s", res.p, res.err)
				} else {
					addPeer(res.p)
				}

			case <-todo.Done():
				logSeed.Warningf("unable to seed routing table to target due to failed dials, still missing: %d", left)
				return ErrPartialSeed

			case <-ctx.Done():
				logSeed.Warningf("unable to seed routing table to target due to slow dials, still missing: %d", left)
				return ErrPartialSeed
			}
		}

		return nil
	}

	// attempt to seed with candidates
	if err := attemptSeedWithPeers(todial); err != nil {
		// fallback
		logSeed.Warningf("resorting to fallback peers to fill %d routing table members", left)
		return attemptSeedWithPeers(fallback)
	}

	// There is a God after all
	return nil
}
