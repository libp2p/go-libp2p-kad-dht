package dht

import (
	"context"

	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/routing"
	kb "github.com/libp2p/go-libp2p-kbucket"
)

// GetClosestPeersMultiLookup .
func (dht *IpfsDHT) GetClosestPeersMultiLookup(ctx context.Context, key string) ([]peer.ID, error) {
	// Convert key to a Kademlia compatible ID (hash it again with SHA256)
	kadKey := kb.ConvertKey(key)

	// Calculate seed peers
	closestPeers := dht.routingTable.NearestPeers(kadKey, dht.bucketSize)
	if len(closestPeers) == 0 {
		routing.PublishQueryEvent(ctx, &routing.QueryEvent{
			Type:  routing.QueryError,
			Extra: kb.ErrLookupFailure.Error(),
		})
		return nil, kb.ErrLookupFailure
	}

	// Create set of other peers
	var otherPeers []peer.ID
	for _, p := range dht.routingTable.ListPeers() {
		// Check if the peer is already in the other list
		inClosestPeers := false
		for _, closestPeer := range closestPeers {
			if p == closestPeer {
				inClosestPeers = true
				break
			}
		}
		// If it's in the other list don't do anything with this peer
		if inClosestPeers {
			continue
		}

		otherPeers = append(otherPeers, p)
		if len(otherPeers) == dht.bucketSize {
			break
		}
	}
	if len(otherPeers) == 0 {
		routing.PublishQueryEvent(ctx, &routing.QueryEvent{
			Type:  routing.QueryError,
			Extra: kb.ErrLookupFailure.Error(),
		})
		return nil, kb.ErrLookupFailure
	}

	mq := dht.newMultiLookupQuery(ctx, key, closestPeers, otherPeers)

	results := mq.run()

	peers := make([]peer.ID, len(results))
	for i, result := range results {
		peers[i] = result.ID
	}

	return peers, nil
}
