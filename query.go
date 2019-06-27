package dht

import (
	"context"
	"errors"
	"sync"

	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	pstore "github.com/libp2p/go-libp2p-core/peerstore"
	"github.com/libp2p/go-libp2p-core/routing"

	"github.com/libp2p/go-libp2p-kad-dht/kpeerset"

	logging "github.com/ipfs/go-log"
	todoctr "github.com/ipfs/go-todocounter"
	process "github.com/jbenet/goprocess"
	ctxproc "github.com/jbenet/goprocess/context"
	kb "github.com/libp2p/go-libp2p-kbucket"
	"github.com/libp2p/go-libp2p-peerstore/queue"
)

// ErrNoPeersQueried is returned when we failed to connect to any peers.
var ErrNoPeersQueried = errors.New("failed to query any peers")

var maxQueryConcurrency = AlphaValue

type dhtQuery struct {
	dht         *IpfsDHT
	key         string      // the key we're querying for
	rfunc       recurseFunc // the function to execute per peer when recursing
	ffunc       finishFunc  // the function to execute per peer when finishing
	concurrency int         // the concurrency parameter
}

type dhtQueryFinishStep struct {
	query                 *dhtQuery
	seen, queried, failed []peer.ID
}

// constructs query
func (dht *IpfsDHT) newQuery(k string, recurse recurseFunc, finish finishFunc) *dhtQuery {
	return &dhtQuery{
		key:         k,
		dht:         dht,
		rfunc:       recurse,
		ffunc:       finish,
		concurrency: maxQueryConcurrency,
	}
}

// recurseFunc is a function that runs a particular query with a given peer (when recursing). It
// returns the set of peers to query next.
type recurseFunc func(context.Context, peer.ID) ([]*peer.AddrInfo, error)

// finishFunc is a function that runs a particular query with a given peer (when finishing).
type finishFunc func(context.Context, peer.ID) error

func (q *dhtQuery) Run(ctx context.Context, peers []peer.ID) ([]peer.ID, error) {
	if len(peers) == 0 {
		logger.Warning("Running query with no peers!")
		return nil, kb.ErrLookupFailure
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	return newQueryRunner(q).run(ctx, peers)
}

type dhtQueryRunner struct {
	query          *dhtQuery          // query to run
	peersSeen      *peer.Set          // all peers seen.
	peersQueried   *peer.Set          // all peers successfully queried.
	peersFailed    *peer.Set          // all peers not successfully queried.
	aPeers         *kpeerset.KPeerSet // k best peers queried.
	peersDialed    *dialQueue         // peers we have dialed to
	peersToQuery   *queue.ChanQueue   // peers remaining to be queried
	peersRemaining todoctr.Counter    // peersToQuery + currently processing

	rateLimit chan struct{} // processing semaphore
	log       logging.EventLogger

	runCtx context.Context

	proc process.Process
	sync.RWMutex
}

func newQueryRunner(q *dhtQuery) *dhtQueryRunner {
	proc := process.WithParent(process.Background())
	ctx := ctxproc.OnClosingContext(proc)
	peersToQuery := queue.NewChanQueue(ctx, queue.NewXORDistancePQ(string(q.key)))
	r := &dhtQueryRunner{
		query:          q,
		peersRemaining: todoctr.NewSyncCounter(),
		peersSeen:      peer.NewSet(),
		peersQueried:   peer.NewSet(),
		peersFailed:    peer.NewSet(),
		aPeers:         kpeerset.New(AlphaValue, q.key),
		rateLimit:      make(chan struct{}, q.concurrency),
		peersToQuery:   peersToQuery,
		proc:           proc,
	}
	dq, err := newDialQueue(&dqParams{
		ctx:    ctx,
		target: q.key,
		in:     peersToQuery,
		dialFn: r.dialPeer,
		config: dqDefaultConfig(),
	})
	if err != nil {
		panic(err)
	}
	r.peersDialed = dq
	return r
}

func (r *dhtQueryRunner) run(ctx context.Context, peers []peer.ID) ([]peer.ID, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	r.log = logger
	r.runCtx = ctx

	err := r.recurse(ctx, peers)
	if err != nil {
		return nil, err
	}
	return r.finish(ctx)
}

func (r *dhtQueryRunner) recurse(ctx context.Context, peers []peer.ID) error {
	// setup concurrency rate limiting
	for len(r.rateLimit) < cap(r.rateLimit) {
		r.rateLimit <- struct{}{}
	}

	// add all the peers we got first.
	for _, p := range peers {
		r.addPeerToQuery(p)
	}

	// start the dial queue only after we've added the initial set of peers.
	// this is to avoid race conditions that could cause the peersRemaining todoctr
	// to be done too early if the initial dial fails before others make it into the queue.
	r.peersDialed.Start()

	// go do this thing.
	// do it as a child proc to make sure Run exits
	// ONLY AFTER spawn workers has exited.
	r.proc.Go(r.spawnWorkers)

	// now, if the context finishes, close the proc.
	// we have to do it here because the logic before is setup, which
	// should run without closing the proc.
	ctxproc.CloseAfterContext(r.proc, ctx)

	var err error
	select {
	case <-r.peersRemaining.Done():
		// Cleanup workers.
		r.proc.Close()
		if r.peersQueried.Size() == 0 {
			err = ErrNoPeersQueried
		}
	case <-r.proc.Closed():
		err = r.runCtx.Err()
	}
	return err
}

func (r *dhtQueryRunner) addPeerToQuery(next peer.ID) {
	// if new peer is ourselves...
	if next == r.query.dht.self {
		r.log.Debug("addPeerToQuery skip self")
		return
	}

	if !r.peersSeen.TryAdd(next) {
		return
	}

	routing.PublishQueryEvent(r.runCtx, &routing.QueryEvent{
		Type: routing.AddingPeer,
		ID:   next,
	})

	if !r.aPeers.Check(next) {
		return
	}

	r.peersRemaining.Increment(1)
	select {
	case r.peersToQuery.EnqChan <- next:
	case <-r.proc.Closing():
	}
}

func (r *dhtQueryRunner) spawnWorkers(proc process.Process) {
	for {
		select {
		case <-r.peersRemaining.Done():
			return

		case <-r.proc.Closing():
			return

		case <-r.rateLimit:
			ch := r.peersDialed.Consume()
			select {
			case p, ok := <-ch:
				if !ok {
					// this signals context cancellation.
					return
				}
				// do it as a child func to make sure Run exits
				// ONLY AFTER spawn workers has exited.
				proc.Go(func(proc process.Process) {
					r.queryPeer(proc, p)
				})
			case <-r.proc.Closing():
				return
			case <-r.peersRemaining.Done():
				return
			}
		}
	}
}

func (r *dhtQueryRunner) dialPeer(ctx context.Context, p peer.ID) error {
	if !r.aPeers.Check(p) {
		// Don't bother with this peer. We'll skip it in the query phase as well.
		return nil
	}

	// short-circuit if we're already connected.
	if r.query.dht.host.Network().Connectedness(p) == network.Connected {
		return nil
	}

	logger.Debug("not connected. dialing.")
	routing.PublishQueryEvent(r.runCtx, &routing.QueryEvent{
		Type: routing.DialingPeer,
		ID:   p,
	})

	pi := peer.AddrInfo{ID: p}
	if err := r.query.dht.host.Connect(ctx, pi); err != nil {
		logger.Debugf("error connecting: %s", err)
		routing.PublishQueryEvent(r.runCtx, &routing.QueryEvent{
			Type:  routing.QueryError,
			Extra: err.Error(),
			ID:    p,
		})

		r.peersFailed.Add(p)
		// This peer is dropping out of the race.
		r.peersRemaining.Decrement(1)
		return err
	}
	logger.Debugf("connected. dial success.")
	return nil
}

func (r *dhtQueryRunner) queryPeer(proc process.Process, p peer.ID) {
	// ok let's do this!

	// create a context from our proc.
	ctx := ctxproc.OnClosingContext(proc)

	// make sure we do this when we exit
	defer func() {
		// signal we're done processing peer p
		r.peersRemaining.Decrement(1)
		r.rateLimit <- struct{}{}
	}()

	if !r.aPeers.Check(p) {
		// Don't bother with this peer.
		return
	}

	conn := r.query.dht.connForPeer(p)

	r.peersQueried.Add(p)

	// finally, run the query against this peer
	closerPeers, err := r.query.rfunc(ctx, p)

	if err == nil {
		r.aPeers.Add(p)
	} else {
		r.peersFailed.Add(p)
	}

	if err != nil {
		logger.Debugf("ERROR worker for: %v %v", p, err)
	} else if filtered := filterCandidatesPtr(conn, closerPeers); len(filtered) > 0 {
		logger.Debugf("PEERS CLOSER -- worker for: %v (%d closer filtered peers; unfiltered: %d)", p,
			len(filtered), len(closerPeers))
		for _, next := range filtered {
			if next.ID == r.query.dht.self { // don't add self.
				logger.Debugf("PEERS CLOSER -- worker for: %v found self", p)
				continue
			}

			// add their addresses to the dialer's peerstore
			r.query.dht.peerstore.AddAddrs(next.ID, next.Addrs, pstore.TempAddrTTL)
			r.addPeerToQuery(next.ID)
			// logger.Debugf("PEERS CLOSER -- worker for: %v added %v (%v)", p, next.ID, next.Addrs)
		}
	} else {
		logger.Debugf("QUERY worker for: %v - not found, and no closer (filtered) peers.", p)
	}
}

func (r *dhtQueryRunner) finish(ctx context.Context) ([]peer.ID, error) {

	// Get a sorted list of peers to query.
	seen := r.peersSeen.Peers()
	closest := make([]peer.ID, 0, len(seen))
	for _, p := range seen {
		if r.peersFailed.Contains(p) {
			continue
		}
		closest = append(closest, p)
	}
	closest = kb.SortClosestPeers(closest, kb.ConvertKey(r.query.key))

	// Query them.
	bucket := make([]peer.ID, 0, KValue)
	workQ := make(chan peer.ID)
	resultQ := make(chan peer.ID, KValue)

	requery := true
	qfunc := r.query.ffunc
	if qfunc == nil {
		requery = false
		qfunc = func(ctx context.Context, p peer.ID) error {
			_, err := r.query.rfunc(ctx, p)
			return err
		}
	}

	var wg sync.WaitGroup
	defer wg.Wait()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	wg.Add(KValue)
	for i := 0; i < KValue; i++ {
		go func() {
			defer wg.Done()
			for p := range workQ {
				if !requery && r.peersQueried.Contains(p) {
					// no need to re-query this peer as we've already sent
					// them this query.
					resultQ <- p
					return
				}

				if qfunc(ctx, p) == nil {
					resultQ <- p
					return
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(resultQ)
	}()

	// No need to handle the context, assuming the _user_ does in rfunc.

	for len(bucket) < KValue && len(closest) > 0 {
		select {
		case workQ <- closest[0]:
			closest = closest[1:]
		case successPeer, ok := <-resultQ:
			if !ok {
				return bucket, ctx.Err()
			}
			bucket = append(bucket, successPeer)
		}
	}

	close(workQ)

	for p := range resultQ {
		bucket = append(bucket, p)
	}

	return bucket, ctx.Err()
}
