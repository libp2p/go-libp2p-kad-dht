package dht

import (
	"context"
	"sync"

	u "github.com/ipfs/go-ipfs-util"
	logging "github.com/ipfs/go-log"
	todoctr "github.com/ipfs/go-todocounter"
	process "github.com/jbenet/goprocess"
	ctxproc "github.com/jbenet/goprocess/context"
	inet "github.com/libp2p/go-libp2p-net"
	peer "github.com/libp2p/go-libp2p-peer"
	pset "github.com/libp2p/go-libp2p-peer/peerset"
	pstore "github.com/libp2p/go-libp2p-peerstore"
	queue "github.com/libp2p/go-libp2p-peerstore/queue"
	routing "github.com/libp2p/go-libp2p-routing"
	notif "github.com/libp2p/go-libp2p-routing/notifications"
)

var maxQueryConcurrency = AlphaValue

type dhtQuery struct {
	dht         *IpfsDHT
	key         string        // the key we're querying for
	qfunc       queryPathFunc // the function that returns the function to execute per peer
	concurrency int           // the concurrency parameter
}

type dhtQueryResult struct {
	value         []byte             // GetValue
	peer          *pstore.PeerInfo   // FindPeer
	providerPeers []pstore.PeerInfo  // GetProviders
	closerPeers   []*pstore.PeerInfo // *
	success       bool
	stopAllPaths  bool // Set to true to halt all disjoint paths
}

type dhtQueryFullResults struct {
	paths []dhtQueryResult

	finalSet   *pset.PeerSet
	queriedSet *pset.PeerSet
}

// Constructs a query. Per the S/Kademlia paper, each query consists of
// multiple paths that query disjoint sets of peers. A query is setup with a
// target key, a queryPathFunc that sets up a given path and returns a
// QueryFunc tasked to communicate with a peer, and a set of initial peers. As
// the query progress, QueryFunc can return closer peers that will be used to
// navigate closer to the target key in the DHT until an answer is reached.
func (dht *IpfsDHT) newQuery(k string, f queryPathFunc) *dhtQuery {
	return &dhtQuery{
		key:         k,
		dht:         dht,
		qfunc:       f,
		concurrency: maxQueryConcurrency,
	}
}

// QueryPathFunc is a function that holds a path's state in its closure.
// It returns the QueryFunc for one particular path. Its parameter, `path`,
// specifies the path index number from 0 to numPaths - 1
type queryPathFunc func(pathIndex int, numPaths int) QueryFunc

// QueryFunc is a function that runs a particular query with a given peer.
// It returns either:
// - the value
// - a list of peers potentially better able to serve the query
// - an error
type QueryFunc func(context.Context, peer.ID) (*dhtQueryResult, error)

// Run runs the query at hand. pass in a list of peers to use first.
func (q *dhtQuery) Run(ctx context.Context, peers []peer.ID) (*dhtQueryFullResults, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	numPaths := q.dht.disjointPaths
	// we don't want empty paths
	if numPaths > len(peers) {
		numPaths = len(peers)
	}

	runner := newQueryRunner(q, numPaths)
	return runner.Run(ctx, peers)
}

type dhtQueryPath struct {
	runner         *dhtQueryRunner
	qfunc          QueryFunc
	peersToQuery   *queue.ChanQueue // peers remaining to be queried
	peersRemaining todoctr.Counter  // peersToQuery + currently processing

	result *dhtQueryResult // query result

	rateLimit chan struct{} // processing semaphore
	proc      process.Process
}

type dhtQueryRunner struct {
	sync.RWMutex
	query          *dhtQuery // query to run
	paths          []*dhtQueryPath
	pathsRemaining todoctr.Counter
	peersSeen      *pset.PeerSet // all peers queried. prevent querying same peer 2x
	peersQueried   *pset.PeerSet // peers successfully connected to and queried
	errs           u.MultiErr    // result errors. maybe should be a map[peer.ID]error
	log            logging.EventLogger
	runCtx         context.Context
	result         *dhtQueryFullResults
	proc           process.Process
}

func newQueryRunner(q *dhtQuery, numPaths int) *dhtQueryRunner {
	proc := process.WithParent(process.Background())
	ctx := ctxproc.OnClosingContext(proc)
	paths := make([]*dhtQueryPath, numPaths)
	for i := range paths {
		paths[i] = &dhtQueryPath{
			qfunc:          q.qfunc(i, len(paths)),
			peersToQuery:   queue.NewChanQueue(ctx, queue.NewXORDistancePQ(string(q.key))),
			peersRemaining: todoctr.NewSyncCounter(),
			rateLimit:      make(chan struct{}, q.concurrency),
		}
	}

	runner := &dhtQueryRunner{
		query:          q,
		pathsRemaining: todoctr.NewSyncCounter(),
		paths:          paths,
		peersSeen:      pset.New(),
		peersQueried:   pset.New(),
		proc:           proc,
	}

	for i := range paths {
		paths[i].runner = runner
	}

	return runner
}

func (r *dhtQueryRunner) Run(ctx context.Context, peers []peer.ID) (*dhtQueryFullResults, error) {
	r.log = log
	r.runCtx = ctx

	if len(peers) == 0 {
		log.Warning("Running query with no peers!")
		return nil, nil
	}

	// add all the peers we got first.
	numPaths := len(r.paths)
	occupied := make([]bool, numPaths) // is each occupied?
	for i, p := range peers {
		pathNum := i % numPaths
		if r.paths[pathNum].addPeerToPath(p) {
			occupied[pathNum] = true
		}
	}

	for i, p := range r.paths {
		// skip paths that ended up empty
		if !occupied[i] {
			continue
		}
		// setup concurrency rate limiting
		for i := 0; i < r.query.concurrency; i++ {
			p.rateLimit <- struct{}{}
		}
		r.pathsRemaining.Increment(1)

		// go do this thing.
		// do it as a child proc to make sure Run exits
		// ONLY AFTER spawn workers has exited.
		r.proc.Go(p.spawnWorkers)
	}
	// so workers are working.

	// wait until they're done.
	err := routing.ErrNotFound

	// now, if the context finishes, close the proc.
	// we have to do it here because the logic before is setup, which
	// should run without closing the proc.
	ctxproc.CloseAfterContext(r.proc, ctx)

	select {
	case <-r.pathsRemaining.Done():
		go r.proc.Close()
		r.RLock()
		defer r.RUnlock()

	case <-r.proc.Closed():
		r.RLock()
		defer r.RUnlock()
		err = context.DeadlineExceeded
	}

	// if every query to every peer failed, something must be very wrong.
	if len(r.errs) > 0 && len(r.errs) == r.peersSeen.Size() {
		log.Debugf("query errs: %s", r.errs)
		err = r.errs[0]
	}

	successfulPaths := make([]dhtQueryResult, 0)
	for _, p := range r.paths {
		if p.result != nil && p.result.success {
			successfulPaths = append(successfulPaths, *p.result)
		}
	}

	r.result = &dhtQueryFullResults{
		paths:      successfulPaths,
		finalSet:   r.peersSeen,
		queriedSet: r.peersQueried,
	}

	if len(successfulPaths) > 0 {
		return r.result, nil
	}

	return r.result, err
}

func (p *dhtQueryPath) addPeerToPath(next peer.ID) bool {
	r := p.runner
	// if new peer is ourselves...
	if next == r.query.dht.self {
		r.log.Debug("addPeerToPath skip self")
		return false
	}

	if !r.peersSeen.TryAdd(next) {
		return false
	}

	notif.PublishQueryEvent(r.runCtx, &notif.QueryEvent{
		Type: notif.AddingPeer,
		ID:   next,
	})

	p.peersRemaining.Increment(1)
	select {
	case p.peersToQuery.EnqChan <- next:
	case <-r.proc.Closing():
	}
	return true
}

func (p *dhtQueryPath) spawnWorkers(proc process.Process) {
	p.proc = proc
	defer func() {
		p.runner.pathsRemaining.Decrement(1)
		// Check if we should terminate not just this path but all paths
		if p.result != nil && p.result.stopAllPaths {
			go p.runner.proc.Close()
		}
	}()

	for {

		select {
		case <-p.peersRemaining.Done():
			return

		case <-p.proc.Closing():
			return

		case <-p.rateLimit:
			select {
			case peer, more := <-p.peersToQuery.DeqChan:
				if !more {
					// Put this back so we can finish any outstanding queries.
					p.rateLimit <- struct{}{}
					return // channel closed.
				}

				// do it as a child func to make sure Run exits
				// ONLY AFTER spawn workers has exited.
				proc.Go(func(proc process.Process) {
					p.queryPeer(proc, peer)
				})
			case <-p.proc.Closing():
				return
			case <-p.peersRemaining.Done():
				return
			}
		}
	}
}

func (p *dhtQueryPath) queryPeer(proc process.Process, pid peer.ID) {
	r := p.runner
	// ok let's do this!

	// create a context from our proc.
	ctx := ctxproc.OnClosingContext(proc)

	// make sure we do this when we exit
	defer func() {
		// signal we're done processing peer pid
		p.peersRemaining.Decrement(1)
		p.rateLimit <- struct{}{}
	}()

	// make sure we're connected to the peer.
	// FIXME abstract away into the network layer
	// Note: Failure to connect in this block will cause the function to
	// short circuit.
	if r.query.dht.host.Network().Connectedness(pid) == inet.NotConnected {
		log.Debug("not connected. dialing.")

		notif.PublishQueryEvent(r.runCtx, &notif.QueryEvent{
			Type: notif.DialingPeer,
			ID:   pid,
		})
		// while we dial, we do not take up a rate limit. this is to allow
		// forward progress during potentially very high latency dials.
		p.rateLimit <- struct{}{}

		pi := pstore.PeerInfo{ID: pid}

		if err := r.query.dht.host.Connect(ctx, pi); err != nil {
			log.Debugf("Error connecting: %s", err)

			notif.PublishQueryEvent(r.runCtx, &notif.QueryEvent{
				Type:  notif.QueryError,
				Extra: err.Error(),
				ID:    pid,
			})

			r.Lock()
			r.errs = append(r.errs, err)
			r.Unlock()
			<-p.rateLimit // need to grab it again, as we deferred.
			return
		}
		<-p.rateLimit // need to grab it again, as we deferred.
		log.Debugf("connected. dial success.")
	}

	// finally, run the query against this peer
	res, err := p.qfunc(ctx, pid)

	r.peersQueried.Add(pid)

	if err != nil {
		log.Debugf("ERROR worker for: %v %v", pid, err)
		r.Lock()
		r.errs = append(r.errs, err)
		r.Unlock()

	} else if res.success {
		log.Debugf("SUCCESS worker for: %v %s", pid, res)
		r.Lock()
		p.result = res
		r.Unlock()
		go p.proc.Close() // signal to everyone that we're done.
		// must be async, as we're one of the children, and Close blocks.

	} else if len(res.closerPeers) > 0 {
		log.Debugf("PEERS CLOSER -- worker for: %v (%d closer peers)", pid, len(res.closerPeers))
		for _, next := range res.closerPeers {
			if next.ID == r.query.dht.self { // don't add self.
				log.Debugf("PEERS CLOSER -- worker for: %v found self", pid)
				continue
			}

			// add their addresses to the dialer's peerstore
			r.query.dht.peerstore.AddAddrs(next.ID, next.Addrs, pstore.TempAddrTTL)
			p.addPeerToPath(next.ID)
			log.Debugf("PEERS CLOSER -- worker for: %v added %v (%v)", pid, next.ID, next.Addrs)
		}
	} else {
		log.Debugf("QUERY worker for: %v - not found, and no closer peers.", pid)
	}
}
