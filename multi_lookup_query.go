package dht

import (
	"context"
	"sync"
	"time"

	kb "github.com/libp2p/go-libp2p-kbucket"

	"github.com/google/uuid"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/routing"
	"github.com/libp2p/go-libp2p-kad-dht/qpeerset"
)

// multiLookupQuery represents multiple DHT queries.
type multiLookupQuery struct {
	// the overarching query context. This context is used to forward
	// lookup events to
	ctx context.Context

	// a reference to the DHT instance
	dht *IpfsDHT

	// queries keeps track of all running queries
	queries []*query

	// intersThresh is the intersection threshold that must be reached for
	// the concurrent queries to be stopped
	intersThresh int
}

func (dht *IpfsDHT) runMultiLookup(ctx context.Context, target string, queryFn queryFn, stopFn stopFn) ([]peer.ID, error) {
	// Convert key to a Kademlia compatible ID (hash it again with SHA256)
	kadKey := kb.ConvertKey(target)

	// Calculate seed peers
	sortedPeers := kb.SortClosestPeers(dht.routingTable.ListPeers(), kadKey)
	if len(sortedPeers) < 2*dht.bucketSize {
		routing.PublishQueryEvent(ctx, &routing.QueryEvent{
			Type:  routing.QueryError,
			Extra: kb.ErrLookupFailure.Error(),
		})
		return nil, kb.ErrLookupFailure
	}

	closestPeers := sortedPeers[:dht.bucketSize]
	furthestPeers := sortedPeers[len(sortedPeers)-dht.bucketSize:]

	lookupRes, err := dht.runMultiLookupQuery(ctx, target, queryFn, stopFn, closestPeers, furthestPeers)
	if err != nil {
		return nil, err
	}

	return lookupRes, nil
}

// runMultiLookupQuery initializes a new multi lookup query struct. The number
// of underlying queries is determined by the number of seed peer lists.
func (dht *IpfsDHT) runMultiLookupQuery(ctx context.Context, target string, queryFn queryFn, stopFn stopFn, seedPeerLists ...[]peer.ID) ([]peer.ID, error) {
	for _, seedPeers := range seedPeerLists {
		if len(seedPeers) == 0 {
			routing.PublishQueryEvent(ctx, &routing.QueryEvent{
				Type:  routing.QueryError,
				Extra: kb.ErrLookupFailure.Error(),
			})
			return nil, kb.ErrLookupFailure
		}
	}

	mlq := &multiLookupQuery{
		ctx:          ctx,
		dht:          dht,
		queries:      make([]*query, len(seedPeerLists)),
		intersThresh: dht.bucketSize,
	}

	// for each list of seed peers initialize a query.
	for i, seedPeers := range seedPeerLists {
		mlq.queries[i] = &query{
			id:         uuid.New(),
			key:        target,
			ctx:        ctx,
			dht:        dht,
			queryPeers: qpeerset.NewQueryPeerset(target),
			seedPeers:  seedPeers,
			peerTimes:  make(map[peer.ID]time.Duration),
			terminated: false,
			queryFn:    queryFn,
			stopFn: func() bool {
				return stopFn() || len(mlq.getQueriesIntersection()) >= mlq.intersThresh
			},
		}
	}

	mlq.run()

	if ctx.Err() == nil {
		mlq.recordValuablePeers()
	}

	intersection := mlq.getQueriesIntersection()
	if len(intersection) <= mlq.intersThresh {
		// can happen if stopFn fires
		return intersection, nil
	}

	return intersection[:mlq.intersThresh], nil
}

func (mlq *multiLookupQuery) getQueriesIntersection() []peer.ID {
	// slice access is guarded by enforcing >= queries in newMultiLookupQuery
	peerSets := make([]*qpeerset.QueryPeerset, len(mlq.queries)-1)
	for i, q := range mlq.queries[1:] {
		peerSets[i] = q.queryPeers
	}

	return mlq.queries[0].queryPeers.GetIntersectionNotInState(qpeerset.PeerUnreachable, peerSets...)
}

func (mlq *multiLookupQuery) run() {
	// start all queries
	var wg sync.WaitGroup
	for _, query := range mlq.queries {
		wg.Add(1)
		queryCpy := query
		go func() {
			queryCpy.run()
			wg.Done()
		}()
	}

	// Wait fo all to finish
	wg.Wait()
}

func (mlq *multiLookupQuery) recordValuablePeers() {
	for _, query := range mlq.queries {
		query.recordValuablePeers()
	}
}
