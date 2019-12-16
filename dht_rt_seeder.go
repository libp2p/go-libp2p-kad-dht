package dht

import (
	"context"
	"sync"

	"github.com/libp2p/go-libp2p-core/peer"
)

func (dht *IpfsDHT) seedRoutingTable(candidates, fallbacks []peer.ID) error {
	seederCtx, cancel := context.WithTimeout(dht.ctx, dht.totalSeederTimeout)
	defer cancel()

	// filter out peers that are either NOT in the peer store OR already in the RT
	findEligible := func(peers []peer.ID) []peer.ID {
		var eligiblePeers []peer.ID
		for _, p := range peers {

			if dht.routingTable.Find(p) == p {
				logger.Info("discarding candidate as it is already in the RT: %s", p)
				continue
			}

			if addrs := dht.host.Peerstore().Addrs(p); len(addrs) == 0 {
				logger.Infof("discarding candidate as we no longer have addresses: %s", p)
				continue
			}

			eligiblePeers = append(eligiblePeers, p)
		}
		return eligiblePeers
	}

	// result of a dial attempt
	type result struct {
		p   peer.ID
		err error
	}

	// rate-limit dials
	semaphore := make(chan struct{}, dht.seederConcurrentDials)

	// attempts to dial to a given peer to verify it's available
	dialFn := func(ctx context.Context, p peer.ID, res chan<- result) {
		childCtx, cancel := context.WithTimeout(ctx, dht.seederDialTimeout)
		defer cancel()
		_, err := dht.host.Network().DialPeer(childCtx, p)
		select {
		case <-ctx.Done(): // caller has already hung up & gone away
		case res <- result{p, err}:
		}
	}

	// ask the proposer to start proposing peers & write them on the peer channel
	peersChan := dht.seedsProposer.Propose(seederCtx, dht.routingTable, findEligible(candidates), findEligible(fallbacks))

	resCh := make(chan result) // dial results.

	// start dialing to the peers received on the result channel
	go func() {
		defer close(resCh)

		var wg sync.WaitGroup
		for p := range peersChan {
			select {
			case <-seederCtx.Done():
				return
			default:
				// start dialing
				semaphore <- struct{}{}
				wg.Add(1)
				go func(p peer.ID, res chan<- result) {
					dialFn(seederCtx, p, res)
					<-semaphore
					wg.Done()
				}(p, resCh)
			}
		}
		wg.Wait()

	}()

LOOP:
	for {
		select {
		case res, hasMore := <-resCh:
			if !hasMore {
				logger.Infof("dht rt seeder: finished seeding RT with proposed peer set; RT size is now %d ",
					dht.routingTable.Size())
				break LOOP
			}
			if res.err != nil {
				logger.Infof("dht rt seeder: discarded proposed peer due to dial error; peer ID: %s, err: %s", res.p, res.err)
			} else {
				if _, err := dht.routingTable.Update(res.p); err != nil {
					logger.Warningf("dht rt seeder: failed to add proposed peer to routing table; peer ID: %s, err: %s", res.p, err)
				}
				if dht.routingTable.Size() >= dht.seederRTSizeTarget {
					break LOOP
				}
			}

		case <-seederCtx.Done():
			logger.Info("dht rt seeder: finishing as we have exceeded the seeder timeout")
			break LOOP
		}
	}

	return nil
}
