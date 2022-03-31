package dht

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/routing"
	"github.com/libp2p/go-libp2p-kad-dht/qpeerset"
	"go.uber.org/atomic"
)

// multiLookupQuery represents multiple DHT queries.
type multiLookupQuery struct {
	// the overarching query context.
	ctx context.Context

	// cancelQueries is the cancel function for the context above
	cancelQueries context.CancelFunc

	// a reference to the DHT instance
	dht *IpfsDHT

	// queries keeps track of all running queries
	queries map[uuid.UUID]*query

	// mqpeerset tracks the states of all peers across all queries
	mqpeerset *qpeerset.MultiQueryPeerset

	// eventsChan emits all lookup events from the underlying queries
	// so that we can update the multi query peerset: mqpeerset
	eventsChan <-chan *LookupEvent

	// termCount keeps track of the amount of terminated queries. If this
	// reaches len(queries) no underlying query is running anymore and
	// we can stop.
	termCount int

	// stop indicates to the underlying queries that they should stop
	// looking for peers.
	stop *atomic.Bool
}

// newMultiLookupQuery initializes a new multi lookup query struct. The number
// of underlying queries is determined by the number of seed peer lists.
func (dht *IpfsDHT) newMultiLookupQuery(ctx context.Context, key string, seedPeerLists ...[]peer.ID) *multiLookupQuery {
	queryCtx, cancelQueries := context.WithCancel(ctx)
	queryCtx, eventsChan := RegisterForLookupEvents(ctx)

	// for each list of seed peers generate a unique query ID
	queryIDs := make([]uuid.UUID, len(seedPeerLists))
	for i := range seedPeerLists {
		queryIDs[i] = uuid.New()
	}

	mlq := &multiLookupQuery{
		ctx:           ctx,
		dht:           dht,
		cancelQueries: cancelQueries,
		queries:       map[uuid.UUID]*query{},
		mqpeerset:     qpeerset.NewMultiQueryPeerset(key, queryIDs...),
		eventsChan:    eventsChan,
		termCount:     0,
		stop:          atomic.NewBool(false),
	}

	queryFn := func(ctx context.Context, p peer.ID) ([]*peer.AddrInfo, error) {
		routing.PublishQueryEvent(ctx, &routing.QueryEvent{
			Type: routing.SendingQuery,
			ID:   p,
		})

		peers, err := dht.protoMessenger.GetClosestPeers(ctx, p, peer.ID(key))
		if err != nil {
			logger.Debugf("error getting closer peers: %s", err)
			routing.PublishQueryEvent(ctx, &routing.QueryEvent{
				Type: routing.QueryError,
				ID:   p,
			})
			return nil, err
		}

		routing.PublishQueryEvent(ctx, &routing.QueryEvent{
			Type:      routing.PeerResponse,
			ID:        p,
			Responses: peers,
		})

		return peers, err
	}

	// for each list of seed peers initialize a query.
	for i, seedPeers := range seedPeerLists {
		query := &query{
			id:         queryIDs[i],
			key:        key,
			ctx:        queryCtx,
			dht:        dht,
			queryPeers: qpeerset.NewQueryPeerset(key),
			seedPeers:  seedPeers,
			peerTimes:  make(map[peer.ID]time.Duration),
			terminated: false,
			queryFn:    queryFn,
			stopFn:     func() bool { return mlq.stop.Load() },
		}
		mlq.queries[query.id] = query
		mlq.mqpeerset.TryAddMany(query.id, dht.self, seedPeers...)
	}

	ps := []peer.ID{}
	for _, p := range mlq.mqpeerset.GetIntersections() {
		ps = append(ps, p.ID)
	}
	logger.Debugw("Initial Intersection", "peers", ps)
	return mlq
}

func (mlq *multiLookupQuery) run() {
	defer mlq.cancelQueries()

	// start all queries
	for _, query := range mlq.queries {
		go query.run()
	}

	// listen for all lookup events
	for event := range mlq.eventsChan {

		// forward lookup event to application
		PublishLookupEvent(mlq.ctx, event)

		if event.Request != nil {
			mlq.handleRequestEvent(event.ID, event.Request)
		} else if event.Response != nil {
			mlq.handleResponseEvent(event.ID, event.Response)
		} else if event.Terminate != nil {
			mlq.termCount += 1
		} else {
			panic(fmt.Errorf("unexpected lookup event"))
		}

		if mlq.termCount == len(mlq.queries) {
			// doneChan <- mlq.mqpeerset.GetIntersections()[:mlq.dht.bucketSize]
			return
		}

		if len(mlq.mqpeerset.GetIntersections()) >= mlq.dht.bucketSize {
			mlq.stop.Store(true)
		}
	}
}

func (mlq *multiLookupQuery) handleRequestEvent(qid uuid.UUID, request *LookupUpdateEvent) {
	for _, p := range request.Waiting {
		if p.Peer == mlq.dht.self {
			continue
		}
		mlq.mqpeerset.SetState(qid, p.Peer, qpeerset.PeerWaiting)
	}
}

func (mlq *multiLookupQuery) handleResponseEvent(qid uuid.UUID, response *LookupUpdateEvent) {
	for _, p := range response.Heard {
		if p.Peer == mlq.dht.self { // don't add self.
			continue
		}
		mlq.mqpeerset.TryAdd(qid, response.Cause.Peer, p.Peer)
	}

	for _, p := range response.Queried {
		if p.Peer == mlq.dht.self { // don't add self.
			continue
		}
		if st := mlq.mqpeerset.GetState(qid, p.Peer); st == qpeerset.PeerWaiting {
			mlq.mqpeerset.SetState(qid, p.Peer, qpeerset.PeerUnreachable)
		} else {
			panic(fmt.Errorf("kademlia protocol error: tried to transition to the queried state from state %v", st))
		}
	}

	for _, p := range response.Unreachable {
		if p.Peer == mlq.dht.self { // don't add self.
			continue
		}

		if st := mlq.mqpeerset.GetState(qid, p.Peer); st == qpeerset.PeerWaiting {
			mlq.mqpeerset.SetState(qid, p.Peer, qpeerset.PeerUnreachable)
		} else {
			panic(fmt.Errorf("kademlia protocol error: tried to transition to the unreachable state from state %v", st))
		}
	}
}
