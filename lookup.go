package dht

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/routing"

	"github.com/ipfs/go-cid"
	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
	kb "github.com/libp2p/go-libp2p-kbucket"
	"github.com/multiformats/go-base32"
)

func tryFormatLoggableKey(k string) (string, error) {
	if len(k) == 0 {
		return "", fmt.Errorf("loggableKey is empty")
	}
	var proto, cstr string
	if k[0] == '/' {
		// it's a path (probably)
		protoEnd := strings.IndexByte(k[1:], '/')
		if protoEnd < 0 {
			return k, fmt.Errorf("loggableKey starts with '/' but is not a path: %x", k)
		}
		proto = k[1 : protoEnd+1]
		cstr = k[protoEnd+2:]
	} else {
		proto = "provider"
		cstr = k
	}

	var encStr string
	c, err := cid.Cast([]byte(cstr))
	if err == nil {
		encStr = c.String()
	} else {
		encStr = base32.RawStdEncoding.EncodeToString([]byte(cstr))
	}

	return fmt.Sprintf("/%s/%s", proto, encStr), nil
}

type loggableKeyBytes []byte

func (lk loggableKeyString) String() string {
	k := string(lk)
	newKey, err := tryFormatLoggableKey(k)
	if err == nil {
		return newKey
	}
	return k
}

type loggableKeyString string

func (lk loggableKeyBytes) String() string {
	k := string(lk)
	newKey, err := tryFormatLoggableKey(k)
	if err == nil {
		return newKey
	}
	return k
}

// Kademlia 'node lookup' operation. Returns a channel of the K closest peers
// to the given key
//
// If the context is canceled, this function will return the context error along
// with the closest K peers it has found so far.
func (dht *KadDHT) GetClosestPeers(ctx context.Context, key string) (<-chan peer.ID, error) {
	if key == "" {
		return nil, fmt.Errorf("can't lookup empty key")
	}
	//TODO: I can break the interface! return []peer.ID
	lookupRes, err := dht.runLookupWithFollowup(ctx, key,
		func(ctx context.Context, p peer.ID) ([]*peer.AddrInfo, error) {
			// For DHT query command
			routing.PublishQueryEvent(ctx, &routing.QueryEvent{
				Type: routing.SendingQuery,
				ID:   p,
			})

			pmes, err := dht.findPeerSingle(ctx, p, peer.ID(key))
			if err != nil {
				logger.Debugf("error getting closer peers: %s", err)
				return nil, err
			}
			peers := pb.PBPeersToPeerInfos(pmes.GetCloserPeers())

			// For DHT query command
			routing.PublishQueryEvent(ctx, &routing.QueryEvent{
				Type:      routing.PeerResponse,
				ID:        p,
				Responses: peers,
			})

			return peers, err
		},
		func() bool { return false },
	)

	if err != nil {
		return nil, err
	}

	out := make(chan peer.ID, dht.bucketSize)
	defer close(out)

	for _, p := range lookupRes.peers {
		out <- p
	}

	if ctx.Err() == nil && lookupRes.completed {
		// refresh the cpl for this key as the query was successful
		dht.routingTable.ResetCplRefreshedAtForID(kb.ConvertKey(key), time.Now())
	}

	return out, ctx.Err()
}
