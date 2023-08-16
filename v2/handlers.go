package dht

import (
	"context"
	"fmt"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/plprobelab/go-kademlia/key"
	"golang.org/x/exp/slog"

	pb "github.com/libp2p/go-libp2p-kad-dht/v2/pb"
)

// handleFindPeer handles FIND_NODE requests from remote peers.
func (d *DHT) handleFindPeer(ctx context.Context, remote peer.ID, req *pb.Message) (*pb.Message, error) {
	ctx, span := tracer.Start(ctx, "DHT.handleFindPeer")
	defer span.End()

	target, err := peer.IDFromBytes(req.GetKey())
	if err != nil {
		return nil, fmt.Errorf("peer ID from bytes: %w", err)
	}

	// initialize the response message
	resp := &pb.Message{Type: pb.Message_FIND_NODE}

	// if the remote is asking for us, short-circuit and return us only
	if target == d.host.ID() {
		resp.CloserPeers = []pb.Message_Peer{pb.FromAddrInfo(d.pstore.PeerInfo(d.host.ID()))}
		return resp, nil
	}

	// gather closer peers that we know
	resp.CloserPeers = d.closerPeers(ctx, remote, nodeID(target).Key())

	// if we happen to know the target peers addresses (e.g., although we are
	// far away in the keyspace), we add the peer to the result set. This means
	// we potentially return bucketSize + 1 peers. We don't add the peer if it's
	// already contained in the CloserPeers.
	targetInfo := d.pstore.PeerInfo(target)
	if len(targetInfo.Addrs) > 0 && !resp.ContainsCloserPeer(target) {
		resp.CloserPeers = append(resp.CloserPeers, pb.FromAddrInfo(targetInfo))
	}

	return resp, nil
}

// handlePing handles PING requests from remote peers.
func (d *DHT) handlePing(ctx context.Context, remote peer.ID, req *pb.Message) (*pb.Message, error) {
	ctx, span := tracer.Start(ctx, "DHT.handlePing")
	defer span.End()

	d.log.LogAttrs(ctx, slog.LevelDebug, "Responding to ping", slog.String("remote", remote.String()))
	return &pb.Message{Type: pb.Message_PING}, nil
}

// closerPeers returns the closest peers to the given target key this host knows
// about. It doesn't return 1) itself 2) the peer that asked for closer peers.
func (d *DHT) closerPeers(ctx context.Context, remote peer.ID, target key.Key256) []pb.Message_Peer {
	ctx, span := tracer.Start(ctx, "DHT.closerPeers")
	defer span.End()

	peers := d.rt.NearestNodes(target, 20) // TODO: bucket size
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
		// final list if we know addresses of that peer.
		addrInfo := d.pstore.PeerInfo(pid)
		if len(addrInfo.Addrs) == 0 {
			continue
		}

		filtered = append(filtered, pb.FromAddrInfo(addrInfo))
	}

	return filtered
}
