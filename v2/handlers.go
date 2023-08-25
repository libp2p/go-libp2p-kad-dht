package dht

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	ds "github.com/ipfs/go-datastore"
	record "github.com/libp2p/go-libp2p-record"
	recpb "github.com/libp2p/go-libp2p-record/pb"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/plprobelab/go-kademlia/key"
	"golang.org/x/exp/slog"

	pb "github.com/libp2p/go-libp2p-kad-dht/v2/pb"
)

// handleFindPeer handles FIND_NODE requests from remote peers.
func (d *DHT) handleFindPeer(ctx context.Context, remote peer.ID, req *pb.Message) (*pb.Message, error) {
	if len(req.GetKey()) == 0 {
		return nil, fmt.Errorf("handleFindPeer with empty key")
	}

	// "parse" requested peer ID from the key field
	target := peer.ID(req.GetKey())

	// initialize the response message
	resp := &pb.Message{
		Type: pb.Message_FIND_NODE,
		Key:  req.GetKey(),
	}

	// get reference to peer store
	pstore := d.host.Peerstore()

	// if the remote is asking for us, short-circuit and return us only
	if target == d.host.ID() {
		resp.CloserPeers = []pb.Message_Peer{pb.FromAddrInfo(pstore.PeerInfo(d.host.ID()))}
		return resp, nil
	}

	// gather closer peers that we know
	resp.CloserPeers = d.closerPeers(ctx, remote, nodeID(target).Key())

	// if we happen to know the target peers addresses (e.g., although we are
	// far away in the keyspace), we add the peer to the result set. This means
	// we potentially return bucketSize + 1 peers. We won't add the peer to the
	// response if it's already contained in CloserPeers.
	targetInfo := pstore.PeerInfo(target)
	if len(targetInfo.Addrs) > 0 && !resp.ContainsCloserPeer(target) && target != remote {
		resp.CloserPeers = append(resp.CloserPeers, pb.FromAddrInfo(targetInfo))
	}

	return resp, nil
}

// handlePing handles PING requests from remote peers.
func (d *DHT) handlePing(ctx context.Context, remote peer.ID, req *pb.Message) (*pb.Message, error) {
	d.log.LogAttrs(ctx, slog.LevelDebug, "Responding to ping", slog.String("remote", remote.String()))
	return &pb.Message{Type: pb.Message_PING}, nil
}

// handleGetValue handles PUT_VALUE RPCs from remote peers.
func (d *DHT) handlePutValue(ctx context.Context, remote peer.ID, req *pb.Message) (*pb.Message, error) {
	// validate incoming request -> key and record must not be empty/nil
	k := string(req.GetKey())
	if len(k) == 0 {
		return nil, fmt.Errorf("no key was provided")
	}

	rec := req.GetRecord()
	if rec == nil {
		return nil, fmt.Errorf("nil record")
	}

	if !bytes.Equal(req.GetKey(), rec.GetKey()) {
		return nil, fmt.Errorf("key doesn't match record key")
	}

	// key is /$namespace/$binary_id
	ns, path, err := record.SplitKey(k) // get namespace (prefix of the key)
	if err != nil || len(path) == 0 {
		return nil, fmt.Errorf("invalid key %s: %w", k, err)
	}

	backend, found := d.backends[ns]
	if !found {
		return nil, fmt.Errorf("unsupported record type: %s", ns)
	}

	_, err = backend.Store(ctx, path, rec)

	return nil, err
}

// handleGetValue handles GET_VALUE RPCs from remote peers.
func (d *DHT) handleGetValue(ctx context.Context, remote peer.ID, req *pb.Message) (*pb.Message, error) {
	k := string(req.GetKey())
	if len(k) == 0 {
		return nil, fmt.Errorf("handleGetValue but no key in request")
	}

	// prepare the response message
	resp := &pb.Message{
		Type:        pb.Message_GET_VALUE,
		Key:         req.GetKey(),
		CloserPeers: d.closerPeers(ctx, remote, newSHA256Key(req.GetKey())),
	}

	ns, path, err := record.SplitKey(k) // get namespace (prefix of the key)
	if err != nil || path == "" {
		return nil, fmt.Errorf("invalid key %s: %w", k, err)
	}

	backend, found := d.backends[ns]
	if !found {
		return nil, fmt.Errorf("unsupported record type: %s", ns)
	}

	fetched, err := backend.Fetch(ctx, path)
	if err != nil {
		if errors.Is(err, ds.ErrNotFound) {
			return resp, nil
		}
		return nil, fmt.Errorf("fetch record for key %s: %w", k, err)
	} else if fetched == nil {
		return resp, nil
	}

	rec, ok := fetched.(*recpb.Record)
	if ok {
		resp.Record = rec
		return resp, nil
	}
	// the returned value wasn't a record, which could be the case if the
	// key was prefixed with "providers."

	pset, ok := fetched.(*providerSet)
	if ok {
		resp.ProviderPeers = make([]pb.Message_Peer, len(pset.providers))
		for i, p := range pset.providers {
			resp.ProviderPeers[i] = pb.FromAddrInfo(p)
		}

		return resp, nil
	}

	return nil, fmt.Errorf("expected *recpb.Record or *providerSet value type, got: %T", pset)
}

// handleAddProvider handles ADD_PROVIDER RPCs from remote peers.
func (d *DHT) handleAddProvider(ctx context.Context, remote peer.ID, req *pb.Message) (*pb.Message, error) {
	k := string(req.GetKey())
	if len(k) > 80 {
		return nil, fmt.Errorf("key size too large")
	} else if len(k) == 0 {
		return nil, fmt.Errorf("key is empty")
	} else if len(req.GetProviderPeers()) == 0 {
		return nil, fmt.Errorf("no provider peers given")
	}

	var addrInfos []peer.AddrInfo
	for _, addrInfo := range req.ProviderAddrInfos() {
		addrInfo := addrInfo // TODO: remove after go.mod was updated to go 1.21

		if addrInfo.ID != remote {
			return nil, fmt.Errorf("attempted to store provider record for other peer %s", addrInfo.ID)
		}

		if len(addrInfo.Addrs) == 0 {
			return nil, fmt.Errorf("no addresses for provider")
		}

		addrInfos = append(addrInfos, addrInfo)
	}

	backend, ok := d.backends[namespaceProviders]
	if !ok {
		return nil, fmt.Errorf("unsupported record type: %s", namespaceProviders)
	}

	for _, addrInfo := range addrInfos {
		if _, err := backend.Store(ctx, k, addrInfo); err != nil {
			return nil, fmt.Errorf("storing provider record: %w", err)
		}
	}

	return nil, nil
}

// handleGetProviders handles GET_PROVIDERS RPCs from remote peers.
func (d *DHT) handleGetProviders(ctx context.Context, remote peer.ID, req *pb.Message) (*pb.Message, error) {
	k := req.GetKey()
	if len(k) > 80 {
		return nil, fmt.Errorf("handleGetProviders key size too large")
	} else if len(k) == 0 {
		return nil, fmt.Errorf("handleGetProviders key is empty")
	}

	backend, ok := d.backends[namespaceProviders]
	if !ok {
		return nil, fmt.Errorf("unsupported record type: %s", namespaceProviders)
	}

	fetched, err := backend.Fetch(ctx, string(req.GetKey()))
	if err != nil {
		return nil, fmt.Errorf("fetch providers from datastore: %w", err)
	}

	pset, ok := fetched.(*providerSet)
	if !ok {
		return nil, fmt.Errorf("expected *providerSet value type, got: %T", pset)
	}

	pbProviders := make([]pb.Message_Peer, len(pset.providers))
	for i, p := range pset.providers {
		pbProviders[i] = pb.FromAddrInfo(p)
	}

	resp := &pb.Message{
		Type:          pb.Message_GET_PROVIDERS,
		Key:           k,
		CloserPeers:   d.closerPeers(ctx, remote, newSHA256Key(k)),
		ProviderPeers: pbProviders,
	}

	return resp, nil
}

// closerPeers returns the closest peers to the given target key this host knows
// about. It doesn't return 1) itself 2) the peer that asked for closer peers.
func (d *DHT) closerPeers(ctx context.Context, remote peer.ID, target key.Key256) []pb.Message_Peer {
	ctx, span := tracer.Start(ctx, "DHT.closerPeers")
	defer span.End()

	peers := d.rt.NearestNodes(target, d.cfg.BucketSize)
	if len(peers) == 0 {
		return nil
	}

	// pre-allocated the result set slice.
	filtered := make([]pb.Message_Peer, 0, len(peers))
	for _, p := range peers {
		pid := peer.ID(p.(nodeID)) // TODO: type cast

		// check for own peer ID
		if pid == d.host.ID() {
			d.log.Warn("routing table NearestNodes returned our own ID")
			continue
		}

		// Don't send a peer back themselves
		if pid == remote {
			continue
		}

		// extract peer information from peer store and only add it to the
		// final list if we know any addresses of that peer.
		addrInfo := d.host.Peerstore().PeerInfo(pid)
		if len(addrInfo.Addrs) == 0 {
			continue
		}

		filtered = append(filtered, pb.FromAddrInfo(addrInfo))
	}

	return filtered
}
