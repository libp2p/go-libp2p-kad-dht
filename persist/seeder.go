package persist

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"time"

	"github.com/ipfs/go-todocounter"

	"github.com/libp2p/go-libp2p-host"
	"github.com/libp2p/go-libp2p-kbucket"
	inet "github.com/libp2p/go-libp2p-net"
	"github.com/libp2p/go-libp2p-peer"
)

var ErrPartialSeed = errors.New("routing table seeded partially")

// SeedDialGracePeriod is the time we will wait before giving up on a single dial attempt
var SeedDialGracePeriod = 5 * time.Second

// NumDialSimultaneous is  the number of peers we will dial simultaneously to verify availability
var NumDialSimultaneous = 50

// TotalDialGracePeriod is the total time we will wait before giving up on dialling to the current candidates
var TotalDialGracePeriod = 30 * time.Second

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
	// copy & shuffle the candidates
	cpy := make([]peer.ID, len(candidates))
	copy(cpy, candidates)
	rand.Shuffle(len(cpy), func(i, j int) {
		cpy[i], cpy[j] = cpy[j], cpy[i]
	})

	// number of peers we have yet to add to the RT
	var mu sync.Mutex
	left := rs.target

	// create a list of peers we need to dial to verify if they are available
	var todial []peer.ID
	for _, p := range cpy {
		if left == 0 {
			return nil // lucky case: we were already connected to all our candidates.
		}
		if rs.host.Network().Connectedness(p) == inet.Connected {
			// if we are already connected to a peer, it is already in the routing table
			// we should not add the peer to the RT here as it will move it to front of the bucket considering it to be more active
			left--
			continue
		}
		if addrs := rs.host.Peerstore().Addrs(p); len(addrs) == 0 {
			logSeed.Infof("discarding routing table candidate as we no longer have addresses: %s", p)
			continue
		}
		todial = append(todial, p)
	}

	todo := todocounter.NewSyncCounter()
	todo.Increment(uint32(left))

	attemptSeedWithPeers := func(peers []peer.ID) error {
		ctx, cancel := context.WithTimeout(context.Background(), TotalDialGracePeriod)
		defer cancel()
		semaphore := make(chan struct{}, NumDialSimultaneous)

		isSeeded := func() error {
			select {
			case <-todo.Done():
				return nil
			default:
				return ErrPartialSeed
			}
		}

		for _, p := range peers {
			select {
			case <-todo.Done():
				return nil
			case <-ctx.Done():
				logSeed.Warningf("unable to seed routing table to target due to slow dials, still missing: %d", left)
				// sanity check
				return isSeeded()
			case semaphore <- struct{}{}:
				go func(p peer.ID) {
					if rs.addPeerIfAvailable(ctx, into, p) {
						todo.Decrement(1)
						mu.Lock()
						left-- // need to do this to be able to log the number of remaining peers as todocounter dosen't have a Get
						mu.Unlock()
					}
					<-semaphore
				}(p)
			}
		}

		return isSeeded()
	}

	// dial the candidates & add them to RT if applicable
	if err := attemptSeedWithPeers(todial); err != nil {
		// fallback
		logSeed.Warningf("unable to completely seed RT with candidates, resorting to fallback peers to fill %d routing table members", left)
		return attemptSeedWithPeers(fallback)
	}

	// There is a God after all
	return nil
}

// tries to add a peer to the RT if dial to it is successful & returns true if number of peers in RT increased
func (rs *randomSeeder) addPeerIfAvailable(ctx context.Context, into *kbucket.RoutingTable, p peer.ID) bool {
	// dial peer
	childCtx, cancel := context.WithTimeout(ctx, SeedDialGracePeriod)
	defer cancel()
	_, err := rs.host.Network().DialPeer(childCtx, p)
	if err != nil {
		logSeed.Infof("discarded routing table candidate due to dial error; peer ID: %s, err: %+v", p, err)
		return false
	}

	// peer is available, try to add it to RT
	evicted, err := into.Update(p)
	if err == nil && evicted == "" {
		return true
	} else if err != nil {
		logSeed.Warningf("error while adding candidate to routing table; peer ID: %s, err: %s", p, err)
	}
	return false
}

/*Changes
1) Dont add a connected peer to the rt, just decrement counter


*/

/* Questions
1) Should we seed periodically if the RT is empty
2) target as aparam to func rather than struct member
3 ""
f rs.host.Network().Connectedness(p) == inet.Connected {
			// if we are already connected to a peer, it is already in the routing table
			// we should not add the peer to the RT here as it will move it to front of the bucket considering it to be more active
			left--
			continue
		}
""
should we guard every add peer call with this ?


*/
