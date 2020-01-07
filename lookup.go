package dht

import (
	"context"
	"time"

	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/opentracing/opentracing-go"

	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
	kb "github.com/libp2p/go-libp2p-kbucket"
	notif "github.com/libp2p/go-libp2p-routing/notifications"
)

// Kademlia 'node lookup' operation. Returns a channel of the K closest peers
// to the given key
func (dht *IpfsDHT) GetClosestPeers(ctx context.Context, key string) (<-chan peer.ID, error) {
	nearest := dht.routingTable.NearestPeers(kb.ConvertKey(key), AlphaValue)
	if len(nearest) == 0 {
		return nil, kb.ErrLookupFailure
	}

	out := make(chan peer.ID, dht.bucketSize)

	// since the query doesnt actually pass our context down
	// we have to hack this here. whyrusleeping isnt a huge fan of goprocess
	parent := ctx
	query := dht.newQuery(key, func(ctx context.Context, p peer.ID) (*dhtQueryResult, error) {
		// For DHT query command
		notif.PublishQueryEvent(parent, &notif.QueryEvent{
			Type: notif.SendingQuery,
			ID:   p,
		})

		pmes, err := dht.findPeerSingle(ctx, p, peer.ID(key))
		if err != nil {
			logger.Debugf("error getting closer peers: %s", err)
			return nil, err
		}
		peers := pb.PBPeersToPeerInfos(pmes.GetCloserPeers())

		// For DHT query command
		notif.PublishQueryEvent(parent, &notif.QueryEvent{
			Type:      notif.PeerResponse,
			ID:        p,
			Responses: peers,
		})

		return &dhtQueryResult{closerPeers: peers}, nil
	})

	go func() {
		var err error
		sp, ctx := opentracing.StartSpanFromContext(ctx, "dht.get_closest_peers", opentracing.Tags{
			"lookup.key": key,
		})
		defer LinkedFinish(sp, &err)

		defer close(out)

		timedCtx, cancel := context.WithTimeout(ctx, time.Minute)
		defer cancel()

		res, err := query.Run(timedCtx, nearest)
		if err != nil {
			logger.Debugf("closestPeers query run error: %s", err)
			return
		}

		if res != nil && res.queriedSet != nil {
			// refresh the k-bucket containing this key as the query was successful
			dht.routingTable.BucketForID(kb.ConvertKey(key)).ResetRefreshedAt(time.Now())

			sorted := kb.SortClosestPeers(res.queriedSet.Peers(), kb.ConvertKey(key))
			l := len(sorted)
			if l > dht.bucketSize {
				sorted = sorted[:dht.bucketSize]
			}

			for _, p := range sorted {
				out <- p
			}
		}
	}()

	return out, nil
}
