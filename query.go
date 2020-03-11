package dht

import (
	"context"
	"errors"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/routing"
	"github.com/libp2p/go-libp2p-kad-dht/kpeerset"
	kb "github.com/libp2p/go-libp2p-kbucket"

	pstore "github.com/libp2p/go-libp2p-core/peerstore"
)

// ErrNoPeersQueried is returned when we failed to connect to any peers.
var ErrNoPeersQueried = errors.New("failed to query any peers")

type queryFn func(context.Context, peer.ID) ([]*peer.AddrInfo, error)
type stopFn func(*kpeerset.SortedPeerset) bool

type query struct {
	ctx    context.Context
	cancel context.CancelFunc

	dht *IpfsDHT

	localPeers           *kpeerset.SortedPeerset
	globallyQueriedPeers *peer.Set
	queryFn              queryFn
	stopFn               stopFn
}

func (dht *IpfsDHT) runDisjointQueries(ctx context.Context, d int, target string, queryFn queryFn, stopFn stopFn) ([]*query, error) {
	queryCtx, cancelQuery := context.WithCancel(ctx)

	numQueriesComplete := 0
	queryDone := make(chan struct{}, d)

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

	for i := 0; i < len(seedPeers); i++ {
		queries[i%d].localPeers.Add(seedPeers[i])
	}

	for i := 0; i < d; i++ {
		query := queries[i]
		go func() {
			strictParallelismQuery(query)
			queryDone <- struct{}{}
		}()
	}

loop:
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

func strictParallelismQuery(q *query) {
	/*
		start with K closest peers (some queried already some not)
		take best alpha (sorted by some metric)
		query those alpha
		once they complete:
			if the alpha requests did not add any new peers to top K, repeat with unqueried top K
			else repeat
	*/

	foundCloser := false
	for {
		peersToQuery := q.localPeers.UnqueriedFromKClosest()

		if len(peersToQuery) == 0 {
			return
		}

		// TODO: Is it finding a closer peer if it's closer than one we know about or one we have queried?
		numQuery := q.dht.alpha
		if foundCloser {
			numQuery = len(peersToQuery)
		} else if pqLen := len(peersToQuery); pqLen < numQuery {
			numQuery = pqLen
		}
		foundCloser = false

		queryResCh := make(chan *queryResult, numQuery)
		resultsReceived := 0

		for _, p := range peersToQuery[:numQuery] {
			go func(p peer.ID) {
				queryResCh <- q.queryPeer(q.ctx, p)
			}(p)
		}

	loop:
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
	success         bool
	foundCloserPeer bool
}

func (q *query) queryPeer(ctx context.Context, p peer.ID) *queryResult {
	dialCtx, queryCtx := ctx, ctx

	if err := q.dht.dialPeer(dialCtx, p); err != nil {
		q.localPeers.Remove(p)
		return &queryResult{}
	}
	if !q.globallyQueriedPeers.TryAdd(p) {
		q.localPeers.Remove(p)
		return &queryResult{}
	}

	if q.stopFn(q.localPeers) {
		q.cancel()
		return &queryResult{}
	}

	newPeers, err := q.queryFn(queryCtx, p)
	if err != nil {
		q.localPeers.Remove(p)
		return &queryResult{}
	}

	q.localPeers.MarkQueried(p)

	if len(newPeers) == 0 {
		logger.Debugf("QUERY worker for: %v - not found, and no closer peers.", p)
	}

	for _, next := range newPeers {
		if next.ID == q.dht.self { // don't add self.
			logger.Debugf("PEERS CLOSER -- worker for: %v found self", p)
			continue
		}

		// add their addresses to the dialer's peerstore
		q.dht.peerstore.AddAddrs(next.ID, next.Addrs, pstore.TempAddrTTL)
	}

	foundCloserPeer := false
	for _, np := range newPeers {
		closer := q.localPeers.Add(np.ID)
		foundCloserPeer = foundCloserPeer || closer
	}

	if q.stopFn(q.localPeers) {
		q.cancel()
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
