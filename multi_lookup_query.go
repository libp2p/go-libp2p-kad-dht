package dht

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/routing"
	"github.com/libp2p/go-libp2p-kad-dht/mqpeerset"
	"github.com/libp2p/go-libp2p-kad-dht/qpeerset"
	"go.uber.org/atomic"
)

type multiLookupQuery struct {
	ctx           context.Context
	dht           *IpfsDHT
	queries       map[uuid.UUID]*query
	peerset       *mqpeerset.MultiQueryPeerset
	eventsChan    <-chan *LookupEvent
	termCount     int
	cancelQueries context.CancelFunc
	stop          *atomic.Bool
}

func (dht *IpfsDHT) newMultiLookupQuery(ctx context.Context, key string, seedPeerLists ...[]peer.ID) *multiLookupQuery {
	queryCtx, cancelQueries := context.WithCancel(ctx)
	queryCtx, eventsChan := RegisterForLookupEvents(ctx)

	queryIDs := make([]uuid.UUID, len(seedPeerLists))
	for i := range seedPeerLists {
		queryIDs[i] = uuid.New()
	}
	mlq := &multiLookupQuery{
		ctx:           ctx,
		dht:           dht,
		cancelQueries: cancelQueries,
		queries:       map[uuid.UUID]*query{},
		peerset:       mqpeerset.NewMultiQueryPeerset(key, queryIDs...),
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

		// For DHT query command
		routing.PublishQueryEvent(ctx, &routing.QueryEvent{
			Type:      routing.PeerResponse,
			ID:        p,
			Responses: peers,
		})

		return peers, err
	}

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
		mlq.peerset.TryAddMany(query.id, dht.self, seedPeers...)
	}

	ps := []peer.ID{}
	for _, p := range mlq.peerset.GetIntersections() {
		ps = append(ps, p.ID)
	}
	logger.Infow("Initial Intersection", "peers", ps)
	return mlq
}

func (mlq *multiLookupQuery) run() []*mqpeerset.IntersectionPeerState {
	defer mlq.cancelQueries()
	doneChan := make(chan []*mqpeerset.IntersectionPeerState)

	go mlq.consumeLookupEvents(doneChan)
	for _, query := range mlq.queries {
		go query.run()
	}
	return <-doneChan
}

func (mlq *multiLookupQuery) consumeLookupEvents(doneChan chan []*mqpeerset.IntersectionPeerState) {
	for event := range mlq.eventsChan {

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
			doneChan <- mlq.peerset.GetIntersections()[:mlq.dht.bucketSize]
			break
		}

		if len(mlq.peerset.GetIntersections()) >= mlq.dht.bucketSize {
			mlq.stop.Store(true)
		}
	}
}

func (mlq *multiLookupQuery) handleRequestEvent(qid uuid.UUID, request *LookupUpdateEvent) {
	for _, p := range request.Waiting {
		if p.Peer == mlq.dht.self {
			continue
		}
		mlq.peerset.SetState(qid, p.Peer, mqpeerset.PeerWaiting)
	}
}

func (mlq *multiLookupQuery) handleResponseEvent(qid uuid.UUID, response *LookupUpdateEvent) {
	for _, p := range response.Heard {
		if p.Peer == mlq.dht.self { // don't add self.
			continue
		}
		mlq.peerset.TryAdd(qid, response.Cause.Peer, p.Peer)
	}

	for _, p := range response.Queried {
		if p.Peer == mlq.dht.self { // don't add self.
			continue
		}
		if st := mlq.peerset.GetState(qid, p.Peer); st == mqpeerset.PeerWaiting {
			mlq.peerset.SetState(qid, p.Peer, mqpeerset.PeerUnreachable)
		} else {
			panic(fmt.Errorf("kademlia protocol error: tried to transition to the queried state from state %v", st))
		}
	}

	for _, p := range response.Unreachable {
		if p.Peer == mlq.dht.self { // don't add self.
			continue
		}

		if st := mlq.peerset.GetState(qid, p.Peer); st == mqpeerset.PeerWaiting {
			mlq.peerset.SetState(qid, p.Peer, mqpeerset.PeerUnreachable)
		} else {
			panic(fmt.Errorf("kademlia protocol error: tried to transition to the unreachable state from state %v", st))
		}
	}
}
