package dht

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	ds "github.com/ipfs/go-datastore"
	recpb "github.com/libp2p/go-libp2p-record/pb"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-base32"
	"github.com/plprobelab/go-kademlia/key"
	"golang.org/x/exp/slog"

	pb "github.com/libp2p/go-libp2p-kad-dht/v2/pb"
)

// handleFindPeer handles FIND_NODE requests from remote peers.
func (d *DHT) handleFindPeer(ctx context.Context, remote peer.ID, req *pb.Message) (*pb.Message, error) {
	if len(req.GetKey()) == 0 {
		return nil, fmt.Errorf("handleFindPeer with empty key")
	}

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
	if len(req.GetKey()) == 0 {
		return nil, fmt.Errorf("no key was provided")
	}

	rec := req.GetRecord()
	if rec == nil {
		return nil, fmt.Errorf("nil record")
	}

	if !bytes.Equal(req.GetKey(), rec.GetKey()) {
		return nil, fmt.Errorf("key doesn't match record key")
	}

	// avoid storing arbitrary data
	rec.TimeReceived = ""

	if err := d.validator.Validate(string(rec.GetKey()), rec.GetValue()); err != nil {
		return nil, fmt.Errorf("put bad record: %w", err)
	}

	txn, err := d.ds.NewTransaction(ctx, false)
	if err != nil {
		return nil, fmt.Errorf("new transaction: %w", err)
	}
	defer txn.Discard(ctx) // discard is a no-op if committed beforehand

	shouldReplace, err := d.shouldReplaceExistingRecord(ctx, txn, rec)
	if err != nil {
		return nil, fmt.Errorf("checking datastore for better record: %w", err)
	} else if !shouldReplace {
		return nil, fmt.Errorf("received worse record")
	}

	rec.TimeReceived = time.Now().UTC().Format(time.RFC3339Nano)
	data, err := rec.Marshal()
	if err != nil {
		return nil, fmt.Errorf("marshal incoming record: %w", err)
	}

	if err = txn.Put(ctx, datastoreKey(rec.GetKey()), data); err != nil {
		return nil, fmt.Errorf("storing record in datastore: %w", err)
	}

	if err = txn.Commit(ctx); err != nil {
		return nil, fmt.Errorf("committing new record to datastore: %w", err)
	}

	return nil, nil
}

// handleGetValue handles GET_VALUE RPCs from remote peers.
func (d *DHT) handleGetValue(ctx context.Context, remote peer.ID, req *pb.Message) (*pb.Message, error) {
	k := req.GetKey()
	if len(k) == 0 {
		return nil, fmt.Errorf("handleGetValue but no key in request")
	}

	// prepare the response message
	resp := &pb.Message{
		Type:        pb.Message_GET_VALUE,
		Key:         k,
		CloserPeers: d.closerPeers(ctx, remote, key.NewSha256(k)),
	}

	// fetch record from the datastore for the requested key
	dsKey := ds.NewKey(base32.RawStdEncoding.EncodeToString(k))
	buf, err := d.ds.Get(ctx, dsKey)
	if err != nil {
		// if we don't have the record, that's fine, just return closer peers
		if errors.Is(err, ds.ErrNotFound) {
			return resp, nil
		}

		return nil, err
	}

	// we have found a record, parse it and do basic validation
	rec := &recpb.Record{}
	err = rec.Unmarshal(buf)
	if err != nil {
		// we have a corrupt record in the datastore -> delete it and pretend
		// that we don't know about it
		if err := d.ds.Delete(ctx, dsKey); err != nil {
			d.log.LogAttrs(ctx, slog.LevelWarn, "Failed deleting corrupt record from datastore", slog.String("err", err.Error()))
		}

		return resp, nil
	}

	// validate that we don't serve stale records.
	receivedAt, err := time.Parse(time.RFC3339Nano, rec.GetTimeReceived())
	if err != nil || time.Since(receivedAt) > d.cfg.MaxRecordAge {
		errStr := ""
		if err != nil {
			errStr = err.Error()
		}

		d.log.LogAttrs(ctx, slog.LevelWarn, "Invalid received timestamp on stored record", slog.String("err", errStr), slog.Duration("age", time.Since(receivedAt)))
		if err = d.ds.Delete(ctx, dsKey); err != nil {
			d.log.LogAttrs(ctx, slog.LevelWarn, "Failed deleting bad record from datastore", slog.String("err", err.Error()))
		}
		return resp, nil
	}

	// We don't do any additional validation beyond checking the above
	// timestamp. We put the burden of validating the record on the requester as
	// checking a record may be computationally expensive.

	// finally, attach the record to the response
	resp.Record = rec

	return resp, nil
}

// handleAddProvider handles ADD_PROVIDER RPCs from remote peers.
func (d *DHT) handleAddProvider(ctx context.Context, remote peer.ID, req *pb.Message) (*pb.Message, error) {
	k := req.GetKey()
	if len(k) > 80 {
		return nil, fmt.Errorf("handleAddProvider key size too large")
	} else if len(k) == 0 {
		return nil, fmt.Errorf("handleAddProvider key is empty")
	}

	for _, addrInfo := range req.ProviderAddrInfos() {
		if addrInfo.ID == remote {
			continue
		}

		if len(addrInfo.Addrs) == 0 {
			continue
		}

		// TODO: store
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

	// TODO: fetch providers

	resp := &pb.Message{
		Type:          pb.Message_GET_PROVIDERS,
		Key:           k,
		CloserPeers:   d.closerPeers(ctx, remote, key.NewSha256(k)),
		ProviderPeers: nil, // TODO: Fill
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

// shouldReplaceExistingRecord returns true if the given record should replace any
// existing one in the local datastore. It queries the datastore, unmarshalls
// the record, validates it, and compares it to the incoming record. If the
// incoming one is "better" (e.g., just newer), this function returns true.
// If unmarshalling or validation fails, this function (alongside an error) also
// returns true because the existing record should be replaced.
func (d *DHT) shouldReplaceExistingRecord(ctx context.Context, dstore ds.Read, newRec *recpb.Record) (bool, error) {
	ctx, span := tracer.Start(ctx, "DHT.shouldReplaceExistingRecord")
	defer span.End()

	existingBytes, err := dstore.Get(ctx, datastoreKey(newRec.GetKey()))
	if errors.Is(err, ds.ErrNotFound) {
		return true, nil
	} else if err != nil {
		return false, fmt.Errorf("getting record from datastore: %w", err)
	}

	existingRec := &recpb.Record{}
	if err := existingRec.Unmarshal(existingBytes); err != nil {
		return true, nil
	}

	if err := d.validator.Validate(string(existingRec.GetKey()), existingRec.GetValue()); err != nil {
		return true, nil
	}

	records := [][]byte{newRec.GetValue(), existingRec.GetValue()}
	i, err := d.validator.Select(string(newRec.GetKey()), records)
	if err != nil {
		return false, fmt.Errorf("record selection: %w", err)
	} else if i != 0 {
		return false, nil
	}

	return true, nil
}

func datastoreKey(k []byte) ds.Key {
	return ds.NewKey(base32.RawStdEncoding.EncodeToString(k))
}
