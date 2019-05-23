package persist

import (
	"context"
	"errors"
	"math/rand"
	"time"

	todocounter "github.com/ipfs/go-todocounter"

	host "github.com/libp2p/go-libp2p-host"
	kbucket "github.com/libp2p/go-libp2p-kbucket"
	inet "github.com/libp2p/go-libp2p-net"
	peer "github.com/libp2p/go-libp2p-peer"
)

var SeedDialGracePeriod = 5 * time.Second
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

	var todial []peer.ID
	for _, p := range cpy {
		if left == 0 {
			return nil // lucky case: we were already connected to all our candidates.
		}
		if rs.host.Network().Connectedness(p) == inet.Connected {
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

	todo := todocounter.NewSyncCounter()
	todo.Increment(uint32(len(todial)))

	ctx, cancel := context.WithTimeout(context.Background(), SeedDialGracePeriod)
	defer cancel()

	dialFn := func(p peer.ID, res chan<- result) {
		_, err := rs.host.Network().DialPeer(ctx, p)
		if err == context.Canceled {
			return
		}
		select {
		case res <- result{p, err}:
		case <-time.After(5 * time.Second):
			// discard this result, no one needs it. a default branch could be racy, so this is better.
		}
	}

	var fallingBack bool       // are we falling back?
	resCh := make(chan result) // dial results.

DialCandidates:
	for _, p := range todial {
		go dialFn(p, resCh)
	}

	for left > 0 {
		select {
		case res := <-resCh:
			todo.Decrement(1)
			if res.err == nil {
				addPeer(res.p)
			}
			logSeed.Infof("discarded routing table candidate due to dial error; peer ID: %s, err: %s, "+
				"falling back: %s", res.p, res.err, fallingBack)
		case <-todo.Done():
			if fallingBack {
				logSeed.Warningf("unable to seed routing table to target due to failed dials, still missing: %d", left)
				return ErrPartialSeed
			}
			logSeed.Warningf("resorting to fallback peers to fill %d routing table members", left)
			goto AddFallbackPeers
		case <-ctx.Done():
			if fallingBack {
				logSeed.Warningf("unable to seed routing table to target due to slow dials, still missing: %d", left)
				return ErrPartialSeed
			}
			logSeed.Warningf("resorting to fallback peers to fill %d routing table members", left)
			goto AddFallbackPeers
		}

	AddFallbackPeers:
		fallingBack = true
		todial = append(todial, fallback...)
		todo = todocounter.NewSyncCounter()
		todo.Increment(uint32(len(fallback)))
		ctx, cancel = context.WithTimeout(context.Background(), SeedDialGracePeriod)
		defer cancel()
		goto DialCandidates
	}

	return nil
}
