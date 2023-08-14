package dht

import (
	"context"
	"fmt"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/plprobelab/go-kademlia/coord"
	"github.com/plprobelab/go-kademlia/kad"
	"github.com/plprobelab/go-kademlia/key"
	"github.com/plprobelab/go-kademlia/query"
)

func (d *DHT) Start(ctx context.Context) {
	ctx, span := startSpan(ctx, "mainLoop")
	defer span.End()

	kadEvents := d.kad.Events()
	for {
		select {
		case <-ctx.Done():
			return
		case ev := <-kadEvents:
			switch tev := ev.(type) {
			case *coord.KademliaOutboundQueryProgressedEvent[key.Key256, ma.Multiaddr]:
				// TODO: locking
				ch, ok := d.qSubs[tev.QueryID]
				if !ok {
					// we have lost the query waiter somehow
					d.kad.StopQuery(ctx, tev.QueryID)
					continue
				}

				// notify the waiter
				ch <- tev.Response
			}
		}
	}
}

// Assert that IPFS assumptions about interfaces aren't broken. These aren't a
// guarantee, but we can use them to aid refactoring.
var (
	_ routing.Routing = (*DHT)(nil)
)

func (d *DHT) Provide(ctx context.Context, cid cid.Cid, b bool) error {
	panic("implement me")
}

func (d *DHT) FindProvidersAsync(ctx context.Context, cid cid.Cid, i int) <-chan peer.AddrInfo {
	panic("implement me")
}

func (d *DHT) registerQuery() (query.QueryID, chan kad.Response[key.Key256, ma.Multiaddr]) {
	ch := make(chan kad.Response[key.Key256, ma.Multiaddr])

	d.qSubsLk.Lock()
	d.qSubCnt += 1
	qid := query.QueryID(fmt.Sprintf("query-%d", d.qSubCnt))
	d.qSubs[qid] = ch
	d.qSubsLk.Unlock()

	return qid, ch
}

func (d *DHT) FindPeer(ctx context.Context, id peer.ID) (peer.AddrInfo, error) {
	// TODO: look in local peer store first

	qid, ch := d.registerQuery()

	nid := nodeID(d.host.ID())
	err := d.kad.StartQuery(ctx, qid, protoID, &FindNodeRequest[key.Key256, ma.Multiaddr]{NodeID: nid})
	if err != nil {
		return peer.AddrInfo{}, fmt.Errorf("failed to start query: %w", err)
	}

	for {
		select {
		case <-ctx.Done():
			d.kad.StopQuery(ctx, qid)
			return peer.AddrInfo{}, ctx.Err()
		case resp, ok := <-ch:
			if !ok {
				// channel was closed, so query can't progress
				d.kad.StopQuery(ctx, qid)
				return peer.AddrInfo{}, fmt.Errorf("query was unexpectedly stopped")
			}

			// we got a response from a message sent by query
			switch resp := resp.(type) {
			case *FindNodeResponse[key.Key256, ma.Multiaddr]:
				// interpret the response
				println("IpfsHandler.FindNode: got FindNode response")
				for _, found := range resp.CloserPeers {
					if key.Equal(found.ID().Key(), nid.Key()) {
						// found the node we were looking for
						d.kad.StopQuery(ctx, qid)
						pid := peer.AddrInfo{
							ID:    peer.ID(found.ID().(nodeID)),
							Addrs: found.Addresses(),
						}
						return pid, nil
					}
				}
			default:
				return peer.AddrInfo{}, fmt.Errorf("unknown response: %v", resp)
			}
		}
	}
}

func (d *DHT) PutValue(ctx context.Context, s string, bytes []byte, option ...routing.Option) error {
	panic("implement me")
}

func (d *DHT) GetValue(ctx context.Context, s string, option ...routing.Option) ([]byte, error) {
	panic("implement me")
}

func (d *DHT) SearchValue(ctx context.Context, s string, option ...routing.Option) (<-chan []byte, error) {
	panic("implement me")
}

func (d *DHT) Bootstrap(ctx context.Context) error {
	panic("implement me")
}
