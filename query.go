package dht

import (
	"context"
	"errors"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	pstore "github.com/libp2p/go-libp2p-core/peerstore"
	"github.com/libp2p/go-libp2p-core/routing"
	"github.com/libp2p/go-libp2p-kad-dht/kpeerset/peerheap"
	"math/big"
	"time"

	"github.com/libp2p/go-libp2p-kad-dht/kpeerset"
	kb "github.com/libp2p/go-libp2p-kbucket"
)

// ErrNoPeersQueried is returned when we failed to connect to any peers.
var ErrNoPeersQueried = errors.New("failed to query any peers")

type queryFn func(context.Context, peer.ID) ([]*peer.AddrInfo, error)
type stopFn func(*kpeerset.SortedPeerset) bool

// query represents a single disjoint query.
type query struct {
	// the query context.
	ctx context.Context
	// the cancellation function for the query context.
	cancel context.CancelFunc

	dht *IpfsDHT

	// localPeers is the set of peers that need to be queried or have already been queried for this query.
	localPeers *kpeerset.SortedPeerset

	// globallyQueriedPeers is the set of peers that have been queried across ALL queries.
	// for now, this is the combined set of peers queried across ALL the disjoint queries.
	globallyQueriedPeers *peer.Set

	// the function that will be used to query a single peer.
	queryFn queryFn

	// stopFn is used to determine if we should stop the WHOLE disjoint query.
	stopFn stopFn
}

func (dht *IpfsDHT) runDisjointQueries(ctx context.Context, d int, target string, queryFn queryFn, stopFn stopFn) ([]*query, error) {
	queryCtx, cancelQuery := context.WithCancel(ctx)

	numQueriesComplete := 0
	queryDone := make(chan struct{}, d)

	// pick the K closest peers to the key in our Routing table and shuffle them.
	seedPeers := dht.routingTable.NearestPeers(kb.ConvertKey(target), dht.bucketSize)
	if len(seedPeers) == 0 {
		routing.PublishQueryEvent(ctx, &routing.QueryEvent{
			Type:  routing.QueryError,
			Extra: kb.ErrLookupFailure.Error(),
		})
		return nil, kb.ErrLookupFailure
	}

	dht.rng.Shuffle(len(seedPeers), func(i, j int) {
		seedPeers[i], seedPeers[j] = seedPeers[j], seedPeers[i]
	})

	// create "d" disjoint queries
	queries := make([]*query, d)
	peersQueried := peer.NewSet()
	for i := 0; i < d; i++ {
		query := &query{
			ctx:                  queryCtx,
			cancel:               cancelQuery,
			dht:                  dht,
			localPeers:           kpeerset.NewSortedPeerset(dht.bucketSize, target),
			globallyQueriedPeers: peersQueried,
			queryFn:              queryFn,
			stopFn:               stopFn,
		}

		queries[i] = query
	}

	// distribute the shuffled K closest peers as seeds among the "d" disjoint queries
	for i := 0; i < len(seedPeers); i++ {
		queries[i%d].localPeers.Add(seedPeers[i])
	}

	// start the "d"  disjoint queries
	for i := 0; i < d; i++ {
		query := queries[i]
		go func() {
			strictParallelismQuery(query)
			queryDone <- struct{}{}
		}()
	}

loop:
	// wait for all the "d" disjoint queries to complete before we return
	for {
		select {
		case <-queryDone:
			numQueriesComplete++
			if numQueriesComplete == d {
				break loop
			}
		case <-ctx.Done():
			break loop
		}
	}

	return queries, nil
}

// compareByDistanceAndLatency compares two peerHeap Items using a scoring function
// that considers metrics such as connectendness of the peer, it's distance from the key
// and it's current known latency.
// Returns true if i1 has a lower score than i2.
func (q *query) compareByDistanceAndLatency(i1 peerheap.Item, i2 peerheap.Item) bool {
	scorePeer := func(item peerheap.Item) *big.Int {
		connectedness := q.dht.host.Network().Connectedness(item.Peer)
		distanceFromKey := item.Value.(*big.Int)
		latency := q.dht.host.Peerstore().LatencyEWMA(item.Peer)

		var c int64
		switch connectedness {
		case network.Connected:
			c = 1
		case network.CanConnect:
			c = 5
		case network.CannotConnect:
			c = 10000
		default:
			c = 20
		}

		l := int64(latency)
		if l <= 0 {
			l = int64(time.Second) * 10
		}

		res := big.NewInt(c)
		tmp := big.NewInt(l)
		res.Mul(res, tmp)
		res.Mul(res, distanceFromKey)

		return res
	}

	return scorePeer(i1).Cmp(scorePeer(i2)) == -1
}

// strictParallelismQuery concurrently sends the query RPC to all eligible peers
// and waits for ALL the RPC's to complete before starting the next round of RPC's.
func strictParallelismQuery(q *query) {
	foundCloser := false
	for {
		// get the unqueried peers from among the K closest peers to the key sorted in ascending order
		// of their 'distance-latency` score.
		// We sort peers like this so that "better" peers are chosen to be in the α peers
		// which get queried from among the unqueried K  closet.
		peersToQuery := q.localPeers.UnqueriedFromKClosest(q.compareByDistanceAndLatency)

		// The lookup terminates when the initiator has queried and gotten responses from the k
		// closest nodes it has heard about.
		if len(peersToQuery) == 0 {
			return
		}

		// Of the k nodes the initiator has heard of closest to the target,
		// it picks α that it has not yet queried and resends the FIND NODE RPC to them.
		numQuery := q.dht.alpha

		// However, If a round of RPC's fails to return a node any closer than the closest already heard about,
		// the initiator resends the RPC'S to all of the k closest nodes it has
		// not already queried.
		if !foundCloser {
			numQuery = len(peersToQuery)
		} else if pqLen := len(peersToQuery); pqLen < numQuery {
			// if we don't have α peers, pick whatever number we have.
			numQuery = pqLen
		}

		// reset foundCloser to false for the next round of RPC's
		foundCloser = false

		queryResCh := make(chan *queryResult, numQuery)
		resultsReceived := 0

		// send RPC's to all the chosen peers concurrently
		for _, p := range peersToQuery[:numQuery] {
			go func(p peer.ID) {
				queryResCh <- q.queryPeer(p)
			}(p)
		}

	loop:
		// wait for all outstanding RPC's to complete before we start the next round.
		for {
			select {
			case res := <-queryResCh:
				foundCloser = foundCloser || res.foundCloserPeer
				resultsReceived++
				if resultsReceived == numQuery {
					break loop
				}
			case <-q.ctx.Done():
				return
			}
		}
	}
}

type queryResult struct {
	success bool
	// foundCloserPeer is true if the peer we're querying returns a peer
	// closer than the closest we've already heard about
	foundCloserPeer bool
}

// queryPeer queries a single peer.
func (q *query) queryPeer(p peer.ID) *queryResult {
	dialCtx, queryCtx := q.ctx, q.ctx

	// dial the peer
	if err := q.dht.dialPeer(dialCtx, p); err != nil {
		q.localPeers.Remove(p)
		return &queryResult{}
	}

	// add the peer to the global set of queried peers since the dial was successful
	// so that no other disjoint query tries sending an RPC to the same peer
	if !q.globallyQueriedPeers.TryAdd(p) {
		q.localPeers.Remove(p)
		return &queryResult{}
	}

	// did the dial fulfill the stop condition ?
	if q.stopFn(q.localPeers) {
		q.cancel()
		return &queryResult{}
	}

	// send query RPC to the remote peer
	newPeers, err := q.queryFn(queryCtx, p)
	if err != nil {
		q.localPeers.Remove(p)
		return &queryResult{}
	}

	// mark the peer as queried.
	q.localPeers.MarkQueried(p)

	// did the successful query RPC fulfill the query stop condition ?
	if q.stopFn(q.localPeers) {
		q.cancel()
	}

	if len(newPeers) == 0 {
		logger.Debugf("QUERY worker for: %v - not found, and no closer peers.", p)
	}

	foundCloserPeer := false
	for _, next := range newPeers {
		if next.ID == q.dht.self { // don't add self.
			logger.Debugf("PEERS CLOSER -- worker for: %v found self", p)
			continue
		}

		// add their addresses to the dialer's peerstore
		q.dht.peerstore.AddAddrs(next.ID, next.Addrs, pstore.TempAddrTTL)
		closer := q.localPeers.Add(next.ID)
		foundCloserPeer = foundCloserPeer || closer
	}

	return &queryResult{
		success:         true,
		foundCloserPeer: foundCloserPeer,
	}
}

func (dht *IpfsDHT) dialPeer(ctx context.Context, p peer.ID) error {
	// short-circuit if we're already connected.
	if dht.host.Network().Connectedness(p) == network.Connected {
		return nil
	}

	logger.Debug("not connected. dialing.")
	routing.PublishQueryEvent(ctx, &routing.QueryEvent{
		Type: routing.DialingPeer,
		ID:   p,
	})

	pi := peer.AddrInfo{ID: p}
	if err := dht.host.Connect(ctx, pi); err != nil {
		logger.Debugf("error connecting: %s", err)
		routing.PublishQueryEvent(ctx, &routing.QueryEvent{
			Type:  routing.QueryError,
			Extra: err.Error(),
			ID:    p,
		})

		return err
	}
	logger.Debugf("connected. dial success.")
	return nil
}
