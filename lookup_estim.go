package dht

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ipfs/go-cid"
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
	// context for all ADD_PROVIDER RPCs
	putCtx context.Context

	// reference to the DHT
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

	// debug fields
	start time.Time
	cid   cid.Cid
}

func (dht *IpfsDHT) newEstimatorState(ctx context.Context, key string) (*estimatorState, error) {
	// get network size and err out if there is no reasonable estimate
	networkSize, err := dht.nsEstimator.NetworkSize()
	if err != nil {
		return nil, err
	}

	_, c, err := cid.CidFromBytes([]byte(key))
	if err != nil {
		return nil, err
	}
	return &estimatorState{
		putCtx:              ctx,
		dht:                 dht,
		key:                 key,
		doneChan:            make(chan struct{}, int(float64(dht.bucketSize)*0.75)), // 15
		ksKey:               ks.XORKeySpace.Key([]byte(key)),
		networkSize:         networkSize,
		peerStates:          map[peer.ID]addProviderRPCState{},
		individualThreshold: float64(dht.bucketSize) * 0.75 / (networkSize + 1), // 15 / (n+1)
		setThreshold:        float64(dht.bucketSize) / (networkSize + 1),        // 20 / (n+1)
		start:               time.Now(),
		cid:                 c,
	}, nil
}

func (es *estimatorState) log(a ...interface{}) {
	fmt.Println(append([]interface{}{es.cid.String()[:16], time.Since(es.start).Seconds()}, a...)...)
}

func (dht *IpfsDHT) GetAndProvideToClosestPeers(outerCtx context.Context, key string) error {
	if key == "" {
		return fmt.Errorf("can't lookup empty key")
	}

	// initialize new context for all putProvider operations.
	// We don't want to give the outer context to the put operations as we return early before all
	// put operations have finished to avoid the long tail of the latency distribution. If we
	// provided the outer context the put operations may be cancelled depending on what happens
	// with the context on the user side.
	putCtx, putCtxCancel := context.WithTimeout(context.Background(), time.Minute)

	es, err := dht.newEstimatorState(putCtx, key)
	if err != nil {
		putCtxCancel()
		return err
	}

	// initialize context that finishes when this function returns
	innerCtx, innerCtxCancel := context.WithCancel(outerCtx)
	defer innerCtxCancel()

	go func() {
		select {
		case <-outerCtx.Done():
			// If the outer context gets cancelled while we're still in this function. We stop all
			// pending put operations.
			putCtxCancel()
		case <-innerCtx.Done():
			// We have returned from this function. Ignore cancellations of the outer context and continue
			// with the remaining put operations.
		}
	}()

	es.log("start")
	es.log("networkSize", es.networkSize)
	es.log("individualThreshold", es.individualThreshold)
	es.log("setThreshold", es.setThreshold)
	es.log("doneThreshold", 15)

	defer func() { es.log("return") }()

	lookupRes, err := dht.runLookupWithFollowup(outerCtx, key, dht.pmGetClosestPeers(key), es.stopFn)
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

		es.log("[1] write provider", p.Pretty()[:16], netsize.NormedDistance(ks.XORKeySpace.Key([]byte(p)), es.ksKey))
		go es.putProviderRecord(p)
		es.peerStates[p] = Sent
	}
	es.peerStatesLk.Unlock()

	// wait until at least bucketSize * 0.75 ADD_PROVIDER RPCs have completed
	es.waitForRPCs(int(float64(es.dht.bucketSize) * 0.75))

	if outerCtx.Err() == nil && lookupRes.completed { // likely completed is false but that's not a given
		// refresh the cpl for this key as the query was successful
		dht.routingTable.ResetCplRefreshedAtForID(kb.ConvertKey(key), time.Now())
	}

	return outerCtx.Err()
}

func (es *estimatorState) stopFn(qps *qpeerset.QueryPeerset) bool {
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

		es.log("[0] write provider", p.Pretty()[:16], distances[i])
		// peer is indeed very close already -> store the provider record directly with it!
		go es.putProviderRecord(p)

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
		es.log(fmt.Sprintf("Stopping due to sentAndSuccessCount %d < %d", sentAndSuccessCount, es.dht.bucketSize))
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
		es.log(fmt.Sprintf("Stopping due to average %f < %f", avg, es.setThreshold))
		return true
	}

	return false
}

func (es *estimatorState) putProviderRecord(pid peer.ID) {
	err := es.dht.protoMessenger.PutProvider(es.putCtx, pid, []byte(es.key), es.dht.host)
	es.peerStatesLk.Lock()
	if err != nil {
		es.peerStates[pid] = Failure
	} else {
		es.peerStates[pid] = Success
	}
	es.peerStatesLk.Unlock()

	// indicate that this ADD_PROVIDER RPC has completed
	es.log("[x] Write provider record done!", pid.Pretty()[:16], err != nil)
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
			es.log("received done", dones, returnThreshold)

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
		es.log("done")
	}()

	// wait until returnThreshold ADD_PROVIDER RPCs have finished
	wg.Wait()
}
