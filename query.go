package dht

import (
	"context"
	"errors"

	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	pstore "github.com/libp2p/go-libp2p-core/peerstore"
	"github.com/libp2p/go-libp2p-core/routing"

	"github.com/libp2p/go-libp2p-kad-dht/qpeerset"
	kb "github.com/libp2p/go-libp2p-kbucket"
)

// ErrNoPeersQueried is returned when we failed to connect to any peers.
var ErrNoPeersQueried = errors.New("failed to query any peers")

type queryFn func(context.Context, peer.ID) ([]*peer.AddrInfo, error)
type stopFn func() bool

// query represents a single disjoint query.
type query struct {
	// the query context.
	ctx context.Context

	// the cancellation function for the query context.
	cancel context.CancelFunc

	dht *IpfsDHT

	// seedPeers is the set of peers that seed the query
	seedPeers []peer.ID

	// queryPeers is the set of peers known by this query and their respective states.
	queryPeers *qpeerset.QueryPeerset

	// terminated is set when the first worker thread encounters the termination condition.
	// Its role is to make sure that once termination is determined, it is sticky.
	terminated bool

	// globallyQueriedPeers is the combined set of peers queried across ALL the disjoint queries.
	globallyQueriedPeers *peer.Set // TODO: abstract this away from specifics of disjoint paths

	// the function that will be used to query a single peer.
	queryFn queryFn

	// stopFn is used to determine if we should stop the WHOLE disjoint query.
	stopFn stopFn
}

// d is the number of disjoint queries.
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

	dht.rnglk.Lock()
	dht.rng.Shuffle(len(seedPeers), func(i, j int) {
		seedPeers[i], seedPeers[j] = seedPeers[j], seedPeers[i]
	})
	dht.rnglk.Unlock()

	// create "d" disjoint queries
	queries := make([]*query, d)
	peersQueried := peer.NewSet()
	for i := 0; i < d; i++ {
		query := &query{
			ctx:                  queryCtx,
			cancel:               cancelQuery,
			dht:                  dht,
			queryPeers:           qpeerset.NewQueryPeerset(target),
			seedPeers:            []peer.ID{},
			terminated:           false,
			globallyQueriedPeers: peersQueried,
			queryFn:              queryFn,
			stopFn:               stopFn,
		}

		queries[i] = query
	}

	// distribute the shuffled K closest peers as seeds among the "d" disjoint queries
	for i := 0; i < len(seedPeers); i++ {
		queries[i%d].seedPeers = append(queries[i%d].seedPeers, seedPeers[i])
	}

	// start the "d"  disjoint queries
	for i := 0; i < d; i++ {
		query := queries[i]
		go func() {
			query.runWithGreedyParallelism()
			queryDone <- struct{}{}
		}()
	}

loop:
	// wait for all the "d" disjoint queries to complete before we return
	// XXX: Waiting until all queries are done is a vector for DoS attacks:
	// The disjoint lookup paths that are taken over by adversarial peers
	// can easily be fooled to go on forever.
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

type queryUpdate struct {
	seen        []peer.ID
	queried     []peer.ID
	unreachable []peer.ID
}

func (q *query) runWithGreedyParallelism() {
	pathCtx, cancelPath := context.WithCancel(q.ctx)
	defer cancelPath()

	alpha := q.dht.alpha

	ch := make(chan *queryUpdate, alpha)
	ch <- &queryUpdate{seen: q.seedPeers}

	for {
		select {
		case update := <-ch:
			q.updateState(update)
		case <-pathCtx.Done():
			q.terminate()
			return
		}

		// termination is triggered on end-of-lookup conditions or starvation of unused peers
		if q.readyToTerminate() {
			q.terminate()
			return
		}

		// if all "threads" are busy, wait until someone finishes
		if q.queryPeers.NumWaiting() >= alpha {
			continue
		}

		// spawn new queries, up to the parallelism allowance
		for j := 0; j < alpha-q.queryPeers.NumWaiting(); j++ {
			q.spawnQuery(ch)
		}
	}
}

// spawnQuery starts one query, if an available seen peer is found
func (q *query) spawnQuery(ch chan<- *queryUpdate) {
	if peers := q.queryPeers.GetSortedHeard(); len(peers) == 0 {
		return
	} else {
		q.queryPeers.SetState(peers[0], qpeerset.PeerWaiting)
		go q.queryPeer(ch, peers[0])
	}
}

func (q *query) readyToTerminate() bool {
	// if termination has already been determined, the query is considered terminated forever,
	// regardless of any change to queryPeers that might occur after the initial termination.
	if q.terminated {
		return true
	}
	// give the application logic a chance to terminate
	if q.stopFn() {
		return true
	}
	if q.isStarvationTermination() {
		return true
	}
	if q.isLookupTermination() {
		return true
	}
	return false
}

// From the set of all nodes that are not unreachable,
// if the closest beta nodes are all queried, the lookup can terminate.
func (q *query) isLookupTermination() bool {
	var peers []peer.ID
	peers = q.queryPeers.GetClosestNotUnreachable(q.dht.beta)
	for _, p := range peers {
		if q.queryPeers.GetState(p) != qpeerset.PeerQueried {
			return false
		}
	}
	return true
}

func (q *query) isStarvationTermination() bool {
	return q.queryPeers.NumHeard() == 0 && q.queryPeers.NumWaiting() == 0
}

func (q *query) terminate() {
	q.terminated = true
}

// queryPeer queries a single peer and reports its findings on the channel.
// queryPeer does not access the query state in queryPeers!
func (q *query) queryPeer(ch chan<- *queryUpdate, p peer.ID) {
	dialCtx, queryCtx := q.ctx, q.ctx

	// dial the peer
	if err := q.dht.dialPeer(dialCtx, p); err != nil {
		ch <- &queryUpdate{unreachable: []peer.ID{p}}
		return
	}

	// add the peer to the global set of queried peers since the dial was successful
	// so that no other disjoint query tries sending an RPC to the same peer
	if !q.globallyQueriedPeers.TryAdd(p) {
		ch <- &queryUpdate{unreachable: []peer.ID{p}}
		return
	}

	// send query RPC to the remote peer
	newPeers, err := q.queryFn(queryCtx, p)
	if err != nil {
		ch <- &queryUpdate{unreachable: []peer.ID{p}}
		return
	}

	// process new peers
	saw := []peer.ID{}
	for _, next := range newPeers {
		if next.ID == q.dht.self { // don't add self.
			logger.Debugf("PEERS CLOSER -- worker for: %v found self", p)
			continue
		}

		// add their addresses to the dialer's peerstore
		q.dht.peerstore.AddAddrs(next.ID, next.Addrs, pstore.TempAddrTTL)
		saw = append(saw, next.ID)
	}

	ch <- &queryUpdate{seen: saw, queried: []peer.ID{p}}
}

func (q *query) updateState(up *queryUpdate) {
	for _, p := range up.seen {
		if p == q.dht.self { // don't add self.
			continue
		}
		q.queryPeers.TryAdd(p)
		q.queryPeers.SetState(p, qpeerset.PeerHeard)
	}
	for _, p := range up.queried {
		if p == q.dht.self { // don't add self.
			continue
		}
		q.queryPeers.TryAdd(p)
		q.queryPeers.SetState(p, qpeerset.PeerQueried)
	}
	for _, p := range up.unreachable {
		if p == q.dht.self { // don't add self.
			continue
		}
		q.queryPeers.TryAdd(p)
		q.queryPeers.SetState(p, qpeerset.PeerUnreachable)
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
