package dht

import (
	"context"
	"time"

	"github.com/ipfs/go-todocounter"
	"github.com/libp2p/go-libp2p-core/peer"
)

// SeederDialTimeout is the grace period for one dial attempt
var SeederDialTimeout = 5 * time.Second

// TotalSeederDialTimeout is the total grace period for a group of dial attempts
var TotalSeederDialTimeout = 30 * time.Second

// SeederConcurrentDials is the number of peers we will dial simultaneously
var SeederConcurrentDials = 50

func (dht *IpfsDHT) seedRoutingTable(candidates, fallbacks []peer.ID) error {
	peersFromSet := func(m map[peer.ID]struct{}) []peer.ID {
		var peers []peer.ID
		for p := range m {
			peers = append(peers, p)
		}

		return peers
	}

	// candidates we have still not tried dialling to
	remainingCandidates := make(map[peer.ID]struct{})
	// fallbacks we have still not tried dialling to
	remainingFallbacks := make(map[peer.ID]struct{})

	for i := range candidates {
		remainingCandidates[candidates[i]] = struct{}{}
	}

	for i := range fallbacks {
		remainingFallbacks[fallbacks[i]] = struct{}{}
	}

	for {

		todial := dht.seedsProposer.Propose(dht.routingTable, peersFromSet(remainingCandidates), peersFromSet(remainingFallbacks))
		if len(todial) == 0 {
			logger.Infof("dht rt seeder: finished as proposer returned nil, RT size is now %d", dht.routingTable.Size())
			return nil
		} else {
			// start dialing
			type result struct {
				p   peer.ID
				err error
			}

			// attempts to dial to a given peer to verify it's available
			dialFn := func(ctx context.Context, p peer.ID, res chan<- result) {
				childCtx, cancel := context.WithTimeout(ctx, SeederDialTimeout)
				defer cancel()
				_, err := dht.host.Network().DialPeer(childCtx, p)
				select {
				case <-ctx.Done(): // caller has already hung up & gone away
				case res <- result{p, err}:
				}
			}

			resCh := make(chan result) // dial results.
			ctx, cancel := context.WithTimeout(dht.ctx, TotalSeederDialTimeout)
			defer cancel()

			// start dialing
			semaphore := make(chan struct{}, SeederConcurrentDials)
			go func(peers []peer.ID) {
				for _, p := range peers {
					semaphore <- struct{}{}
					go func(p peer.ID, res chan<- result) {
						dialFn(ctx, p, res)
						<-semaphore
					}(p, resCh)
				}
			}(todial)

			// number of results we expect on the result channel
			todo := todocounter.NewSyncCounter()
			todo.Increment(uint32(len(todial)))

		LOOP:
			for {
				select {
				case res := <-resCh:
					todo.Decrement(1)
					if res.err != nil {
						logger.Infof("dht rt seeder: discarded proposed peer due to dial error; peer ID: %s, err: %s", res.p, res.err)
					} else {
						if _, err := dht.routingTable.Update(res.p); err != nil {
							logger.Warningf("dht rt seeder: failed to add proposed peer to routing table; peer ID: %s, err: %s", res.p, err)
						}
					}

				case <-todo.Done():
					logger.Infof("dht rt seeder: finished current round of dialing & seeding RT with proposed peer set; RT size is now %d ",
						dht.routingTable.Size())
					break LOOP

				case <-ctx.Done():
					logger.Warningf("dht rt seeder: aborting current round of RT seed because dials are too slow; RT size is now %d",
						dht.routingTable.Size())
					break LOOP
				}
			}

			// remove the peers we just tried dialling from the set of remaining peers
			for i := range todial {
				delete(remainingCandidates, todial[i])
				delete(remainingFallbacks, todial[i])
			}
		}
	}
}
