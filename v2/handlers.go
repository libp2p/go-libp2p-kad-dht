package dht

import (
	"context"
	"fmt"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/plprobelab/go-kademlia/kad"
	"github.com/plprobelab/go-kademlia/key"

	pb "github.com/libp2p/go-libp2p-kad-dht/v2/pb"
)

func (d *DHT) handleFindPeer(ctx context.Context, remote peer.ID, req *pb.Message) (*pb.Message, error) {
	target, err := peer.IDFromBytes(req.GetKey())
	if err != nil {
		return nil, fmt.Errorf("peer ID from bytes: %w", err)
	}

	resp := &pb.Message{Type: pb.Message_FIND_NODE}
	if target == d.host.ID() {
		resp.CloserPeers = []pb.Message_Peer{pb.FromAddrInfo(d.pstore.PeerInfo(d.host.ID()))}
	} else {
		resp.CloserPeers = d.closerNodes(ctx, remote, nodeID(target))
	}

	return resp, nil
}

func (d *DHT) closerNodes(ctx context.Context, remote peer.ID, target kad.NodeID[key.Key256]) []pb.Message_Peer {
	peers := d.rt.NearestNodes(target.Key(), 20) // TODO: bucket size
	if len(peers) == 0 {
		return nil
	}

	// pre-allocated the result set slice.
	filtered := make([]pb.Message_Peer, 0, len(peers))

	// if this method should return closer nodes to a peerID (the target is
	// parameter is a nodeID), then we want to add this target to the result set
	// iff 1) it's not already part of the NearestNodes peers 2) we actually
	// know the addresses for the target peer. Therefore, targetFound tracks
	// if the target is in the set of NearestNodes from the routing table.
	// If that's the case we add it to the final filtered peers slice. This
	// means we potentially return bucketSize + 1 peers.
	// Context: https://github.com/libp2p/go-libp2p-kad-dht/pull/511
	targetFound := false
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

		// extract peer information from peer store
		addrInfo := d.pstore.PeerInfo(pid)
		if len(addrInfo.Addrs) == 0 {
			continue
		}

		// Check if this peer is our target peer
		if tid, ok := target.(nodeID); ok && peer.ID(tid) == pid {
			targetFound = true
		}

		filtered = append(filtered, pb.FromAddrInfo(addrInfo))
	}

	// check if the target peer was among the nearest nodes
	if tid, ok := target.(nodeID); ok && !targetFound && peer.ID(tid) != remote {
		// it wasn't, check if we know how to reach it and if we do, add it to
		// the filtered list.
		addrInfo := d.pstore.PeerInfo(peer.ID(tid))
		if len(addrInfo.Addrs) > 0 {
			filtered = append(filtered, pb.FromAddrInfo(addrInfo))
		}
	}

	return filtered
}

func (d *DHT) handlePing(ctx context.Context, remote peer.ID, req *pb.Message) (*pb.Message, error) {
	panic("not implemented")
}
