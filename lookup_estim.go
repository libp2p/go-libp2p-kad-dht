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
	ctx context.Context
	dht *IpfsDHT

	// the most recent network size estimation
	networkSize float64

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

func (dht *IpfsDHT) newEstimatorState(ctx context.Context, key string) (*estimatorState, error) {
	// get network size and err out if there is no reasonable estimate
	networkSize, err := dht.nsEstimator.NetworkSize()
	if err != nil {
		return nil, err
	}

	return &estimatorState{
		ctx:                 ctx,
		dht:                 dht,
		key:                 key,
		ksKey:               ks.XORKeySpace.Key([]byte(key)),
		networkSize:         networkSize,
		peerStates:          map[peer.ID]addProviderRPCState{},
		individualThreshold: float64(dht.bucketSize) * 0.75 / (networkSize + 1), // 15 / (n+1)
		setThreshold:        float64(dht.bucketSize) / (networkSize + 1),        // 20 / (n+1)
	}, nil
}

func (dht *IpfsDHT) GetClosestPeersEstimator(ctx context.Context, key string) ([]peer.ID, error) {
	if key == "" {
		return nil, fmt.Errorf("can't lookup empty key")
	}

	es, err := dht.newEstimatorState(ctx, key)
	if err != nil {
		return nil, err
	}

	lookupRes, err := dht.runLookupWithFollowup(ctx, key, dht.pmGetClosestPeers(key), es.stopFn)
	if err != nil {
		return nil, err
	}

	if ctx.Err() == nil && lookupRes.completed {
		// refresh the cpl for this key as the query was successful
		dht.routingTable.ResetCplRefreshedAtForID(kb.ConvertKey(key), time.Now())
	}

	return lookupRes.peers, ctx.Err()
}

func (es *estimatorState) stopFn(qps *qpeerset.QueryPeerset) bool {
	es.peerStatesLk.Lock()
	defer es.peerStatesLk.Unlock()

	// get currently known closest peers
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
		go es.putProviderRecord(es.ctx, p)

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
	defer es.peerStatesLk.Unlock()
	if err != nil {
		es.peerStates[pid] = Failure
	} else {
		es.peerStates[pid] = Success
	}
}
