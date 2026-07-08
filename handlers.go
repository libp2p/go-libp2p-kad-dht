package dht

import (
	"bytes"
	"context"
	"errors"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"

	"github.com/libp2p/go-libp2p-kad-dht/internal"
	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
)

// dhthandler specifies the signature of functions that handle DHT messages.
type dhtHandler func(context.Context, peer.ID, *pb.Message) (*pb.Message, error)

func (dht *IpfsDHT) handlerForMsgType(t pb.Message_MessageType) dhtHandler {
	switch t {
	case pb.Message_FIND_NODE:
		return dht.handleFindPeer
	case pb.Message_PING:
		return dht.handlePing
	}

	if dht.enableValues {
		switch t {
		case pb.Message_GET_VALUE:
			return dht.handleGetValue
		case pb.Message_PUT_VALUE:
			return dht.handlePutValue
		}
	}

	if dht.enableProviders {
		switch t {
		case pb.Message_ADD_PROVIDER:
			return dht.handleAddProvider
		case pb.Message_GET_PROVIDERS:
			return dht.handleGetProviders
		}
	}

	return nil
}

func (dht *IpfsDHT) handleGetValue(ctx context.Context, p peer.ID, pmes *pb.Message) (_ *pb.Message, err error) {
	// first, is there even a key?
	k := pmes.GetKey()
	if len(k) == 0 {
		return nil, errors.New("handleGetValue but no key was provided")
	}

	// setup response
	resp := pb.NewMessage(pmes.GetType(), pmes.GetKey(), pmes.GetClusterLevel())

	rec, err := dht.valueStore.Get(ctx, string(k))
	if err != nil {
		return nil, err
	}
	resp.Record = rec

	// Find closest peer on given cluster to desired key and reply with that info
	closestPeers := dht.closestPeersToQuery(pmes, p, dht.bucketSize)
	if len(closestPeers) > 0 {
		closestInfos := peerstore.AddrInfos(dht.peerstore, closestPeers)
		for _, pi := range closestInfos {
			logger.Debugf("handleGetValue returning closest peer: '%s'", pi.ID)
			if len(pi.Addrs) < 1 {
				logger.Warnw("no addresses on peer being sent",
					"local", dht.self,
					"to", p,
					"sending", pi.ID,
				)
			}
		}

		resp.CloserPeers = pb.PeerInfosToPBPeers(dht.host.Network(), closestInfos)
	}

	return resp, nil
}

// Store a value in this peer local storage
func (dht *IpfsDHT) handlePutValue(ctx context.Context, p peer.ID, pmes *pb.Message) (_ *pb.Message, err error) {
	if len(pmes.GetKey()) == 0 {
		return nil, errors.New("handlePutValue but no key was provided")
	}

	rec := pmes.GetRecord()
	if rec == nil {
		logger.Debugw("got nil record from", "from", p)
		return nil, errors.New("nil record")
	}

	if !bytes.Equal(pmes.GetKey(), rec.GetKey()) {
		return nil, errors.New("put key doesn't match record key")
	}

	if err := dht.valueStore.Put(ctx, string(rec.GetKey()), rec); err != nil {
		logger.Infow("failed to store PUT_VALUE record", "from", p, "key", internal.LoggableRecordKeyBytes(rec.GetKey()), "error", err)
		return nil, err
	}

	return pmes, nil
}

func (dht *IpfsDHT) handlePing(_ context.Context, p peer.ID, pmes *pb.Message) (*pb.Message, error) {
	logger.Debugf("%s Responding to ping from %s!\n", dht.self, p)
	return pmes, nil
}

func (dht *IpfsDHT) handleFindPeer(ctx context.Context, from peer.ID, pmes *pb.Message) (_ *pb.Message, _err error) {
	resp := pb.NewMessage(pmes.GetType(), nil, pmes.GetClusterLevel())

	if len(pmes.GetKey()) == 0 {
		return nil, errors.New("handleFindPeer with empty key")
	}

	targetPid := peer.ID(pmes.GetKey())
	closest := dht.closestPeersToQuery(pmes, from, dht.bucketSize)

	// Prepend targetPid to the front of the list if not already present.
	// targetPid is always the closest key to itself.
	//
	// Per IPFS Kademlia DHT spec: FIND_PEER has a special exception where the
	// target peer MUST be included in the response (if present in peerstore),
	// even if it is self, the requester, or not a DHT server. This allows peers
	// to discover multiaddresses for any peer, not just DHT servers.
	if len(closest) == 0 || closest[0] != targetPid {
		closest = append([]peer.ID{targetPid}, closest...)
	}

	closestinfos := peerstore.AddrInfos(dht.peerstore, closest)
	// possibly an over-allocation but this array is temporary anyways.
	withAddresses := make([]peer.AddrInfo, 0, len(closestinfos))
	for _, pi := range closestinfos {
		if len(pi.Addrs) > 0 {
			withAddresses = append(withAddresses, pi)
		}
	}

	resp.CloserPeers = pb.PeerInfosToPBPeers(dht.host.Network(), withAddresses)
	return resp, nil
}

func (dht *IpfsDHT) handleGetProviders(ctx context.Context, p peer.ID, pmes *pb.Message) (_ *pb.Message, _err error) {
	key := pmes.GetKey()
	if len(key) > 80 {
		return nil, errors.New("handleGetProviders key size too large")
	} else if len(key) == 0 {
		return nil, errors.New("handleGetProviders key is empty")
	}

	resp := pb.NewMessage(pmes.GetType(), pmes.GetKey(), pmes.GetClusterLevel())

	// setup providers
	providers, err := dht.providerStore.GetProviders(ctx, key)
	if err != nil {
		return nil, err
	}

	filtered := make([]peer.AddrInfo, len(providers))
	for i, provider := range providers {
		filtered[i] = peer.AddrInfo{
			ID:    provider.ID,
			Addrs: dht.filterAddrs(provider.Addrs),
		}
	}

	resp.ProviderPeers = pb.PeerInfosToPBPeers(dht.host.Network(), filtered)

	// Also send closest dht servers we know about.
	closestPeers := dht.closestPeersToQuery(pmes, p, dht.bucketSize)
	if closestPeers != nil {
		infos := peerstore.AddrInfos(dht.peerstore, closestPeers)
		resp.CloserPeers = pb.PeerInfosToPBPeers(dht.host.Network(), infos)
	}

	return resp, nil
}

func (dht *IpfsDHT) handleAddProvider(ctx context.Context, p peer.ID, pmes *pb.Message) (_ *pb.Message, _err error) {
	key := pmes.GetKey()
	if len(key) > 80 {
		return nil, errors.New("handleAddProvider key size too large")
	} else if len(key) == 0 {
		return nil, errors.New("handleAddProvider key is empty")
	}

	logger.Debugw("adding provider", "from", p, "key", internal.LoggableProviderRecordBytes(key))

	// add provider should use the address given in the message
	pinfos := pb.PBPeersToPeerInfos(pmes.GetProviderPeers())
	success := false
	for _, pi := range pinfos {
		if pi.ID != p {
			// we should ignore this provider record! not from originator.
			// (we should sign them and check signature later...)
			logger.Debugw("received provider from wrong peer", "from", p, "peer", pi.ID)
			continue
		}

		if len(pi.Addrs) < 1 {
			logger.Debugw("no valid addresses for provider", "from", p)
			continue
		}

		// We run the addrs filter after checking for the length, this allows
		// transient nodes with varying /p2p-circuit addresses to still have their
		// announcement go through.
		addrs := dht.filterAddrs(pi.Addrs)
		dht.providerStore.AddProvider(ctx, key, peer.AddrInfo{ID: pi.ID, Addrs: addrs})
		success = true
	}
	if !success {
		return nil, errors.New("handleAddProvider no valid provider")
	}

	return nil, nil
}
