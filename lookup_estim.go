package dht

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-kad-dht/netsize"
	"github.com/libp2p/go-libp2p-kad-dht/qpeerset"
	kb "github.com/libp2p/go-libp2p-kbucket"
	ks "github.com/whyrusleeping/go-keyspace"
)

type addProviderRPCState int

const (
	Sent addProviderRPCState = iota + 1
	Success
	Failure
)

type estimatorState struct {
	dht *IpfsDHT

	// the most recent network size estimation
	networkSize float64

	// a channel indicating when an ADD_PROVIDER RPC completed (successful or not)
	doneChan chan struct{}

	// tracks which peers we have stored the provider records with
	peerStatesLk sync.RWMutex
	peerStates   map[peer.ID]addProviderRPCState

	// the key to provide
	key string

	// the key to provide transformed into the Kademlia key space
	ksKey ks.Key

	// routing table error in percent (stale routing table entries)
	rtErrPct float64

	// distance threshold for individual peers
	individualThreshold float64

	// distance threshold for the set of bucketSize closest peers
	setThreshold float64
}

func (dht *IpfsDHT) newEstimatorState(key string) (*estimatorState, error) {
	// get network size and err out if there is no reasonable estimate
	networkSize, err := dht.nsEstimator.NetworkSize()
	if err != nil {
		return nil, err
	}

	return &estimatorState{
		dht:                 dht,
		key:                 key,
		doneChan:            make(chan struct{}, int(float64(dht.bucketSize)*0.75)), // 15
		ksKey:               ks.XORKeySpace.Key([]byte(key)),
		networkSize:         networkSize,
		peerStates:          map[peer.ID]addProviderRPCState{},
		individualThreshold: float64(dht.bucketSize) * 0.75 / (networkSize + 1), // 15 / (n+1)
		setThreshold:        float64(dht.bucketSize) / (networkSize + 1),        // 20 / (n+1)
	}, nil
}

func (dht *IpfsDHT) GetAndProvideToClosestPeers(ctx context.Context, key string) error {
	if key == "" {
		return fmt.Errorf("can't lookup empty key")
	}

	es, err := dht.newEstimatorState(key)
	if err != nil {
		return err
	}

	lookupRes, err := dht.runLookupWithFollowup(ctx, key, dht.pmGetClosestPeers(key), es.stopFn)
	if err != nil {
		return err
	}

	// Store the provider records with all of the closest peers
	// we haven't already contacted.
	es.peerStatesLk.Lock()
	for _, p := range lookupRes.peers {
		if _, found := es.peerStates[p]; found {
			continue
		}
		go es.putProviderRecord(ctx, p)
		es.peerStates[p] = Sent
	}
	es.peerStatesLk.Unlock()

	// wait until at least bucketSize * 0.75 ADD_PROVIDER RPCs have completed
	es.waitForRPCs(int(float64(es.dht.bucketSize) * 0.75))

	if ctx.Err() == nil && lookupRes.completed { // likely completed is false but that's not a given
		// refresh the cpl for this key as the query was successful
		dht.routingTable.ResetCplRefreshedAtForID(kb.ConvertKey(key), time.Now())
	}

	return ctx.Err()
}

func (es *estimatorState) stopFn(ctx context.Context, qps *qpeerset.QueryPeerset) bool {
	es.peerStatesLk.Lock()
	defer es.peerStatesLk.Unlock()

	// get currently known closest peers and check if any of them is already very close.
	// If so -> store provider records straight away.
	closest := qps.GetClosestNInStates(es.dht.bucketSize, qpeerset.PeerHeard, qpeerset.PeerWaiting, qpeerset.PeerQueried)
	distances := make([]float64, es.dht.bucketSize)
	for i, p := range closest {
		// calculate distance of peer p to the target key
		distances[i] = netsize.NormedDistance(ks.XORKeySpace.Key([]byte(p)), es.ksKey)

		// Check if we have already interacted with that peer
		if _, found := es.peerStates[p]; found {
			continue
		}

		// Check if peer is close enough to store the provider record with
		if distances[i] > es.individualThreshold {
			continue
		}

		// peer is indeed very close already -> store the provider record directly with it!
		go es.putProviderRecord(ctx, p)

		// keep state that we've contacted that peer
		es.peerStates[p] = Sent
	}

	// count number of peers we have already (successfully) contacted via the above method
	sentAndSuccessCount := 0
	for _, s := range es.peerStates {
		if s == Sent || s == Success {
			sentAndSuccessCount += 1
		}
	}

	// if we have already contacted more than bucketSize peers stop the procedure
	if sentAndSuccessCount >= es.dht.bucketSize {
		fmt.Printf("Stopping due to sentAndSuccessCount %d < %d\n", sentAndSuccessCount, es.dht.bucketSize)
		return true
	}

	// calculate average distance of the set of closest peers
	sum := 0.0
	for _, d := range distances {
		sum += d
	}
	avg := sum / float64(len(distances))

	// if the average is below the set threshold stop the procedure
	if avg < es.setThreshold {
		fmt.Printf("Stopping due to average %f < %f\n", avg, es.setThreshold)
		return true
	}

	return false
}

func (es *estimatorState) putProviderRecord(ctx context.Context, pid peer.ID) {
	err := es.dht.protoMessenger.PutProvider(ctx, pid, []byte(es.key), es.dht.host)
	es.peerStatesLk.Lock()
	if err != nil {
		es.peerStates[pid] = Failure
	} else {
		es.peerStates[pid] = Success
	}
	es.peerStatesLk.Unlock()

	// indicate that this ADD_PROVIDER RPC has completed
	es.doneChan <- struct{}{}
}

func (es *estimatorState) waitForRPCs(returnThreshold int) {
	es.peerStatesLk.RLock()
	rpcCount := len(es.peerStates)
	es.peerStatesLk.RUnlock()

	// returnThreshold can't be larger than the total number issued RPCs
	if returnThreshold > rpcCount {
		returnThreshold = rpcCount
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		dones := 0
		for range es.doneChan {
			dones += 1

			// Indicate to the wait group that returnThreshold RPCs have finished but
			// don't break here. Keep go-routine around so that the putProviderRecord
			// go-routines can write to the done channel and everything gets cleaned
			// up properly.
			if dones == returnThreshold {
				wg.Done()
			}

			// If the total number RPCs was reached break for loop and close the done channel.
			if dones == rpcCount {
				break
			}
		}
		close(es.doneChan)
	}()

	// wait until returnThreshold ADD_PROVIDER RPCs have finished
	wg.Wait()
}
