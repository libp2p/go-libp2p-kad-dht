package persist

import (
	"errors"
	"fmt"
	"time"

	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"

	dht_pb "github.com/libp2p/go-libp2p-kad-dht/pb"
	kb "github.com/libp2p/go-libp2p-kbucket"

	ds "github.com/ipfs/go-datastore"
	nsds "github.com/ipfs/go-datastore/namespace"
	ma "github.com/multiformats/go-multiaddr"
)

// RtSnapshotPeerInfo is the information for a peer in the Routing Table that we persist.
type RtSnapshotPeerInfo struct {
	ID      peer.ID
	Addrs   []ma.Multiaddr
	AddedAt time.Time
}

var (
	dsSnapshotKey = ds.NewKey("routing_table")
)

// DefaultSnapshotNS is the default namespace to use for the key with which
// the routing table snapshot will be persisted to the data store.
var DefaultSnapshotNS = "/kad-dht/snapshot"

type dsSnapshotter struct {
	ds.Datastore
}

var _ Snapshotter = (*dsSnapshotter)(nil)

// NewDatastoreSnapshotter returns a Snapshotter backed by a datastore, under the specified non-optional namespace.
func NewDatastoreSnapshotter(dstore ds.Datastore, namespace string) (Snapshotter, error) {
	if dstore == nil {
		return nil, errors.New("datastore is nil when creating a datastore snapshotter")
	}
	if namespace == "" {
		return nil, errors.New("blank namespace when creating a datastore snapshotter")
	}
	dstore = nsds.Wrap(dstore, ds.NewKey(namespace))
	return &dsSnapshotter{dstore}, nil
}

func (dsp *dsSnapshotter) Load() ([]*RtSnapshotPeerInfo, error) {
	val, err := dsp.Get(dsSnapshotKey)

	switch err {
	case nil:
	case ds.ErrNotFound:
		return nil, nil
	default:
		return nil, err
	}

	s := &dht_pb.RoutingTableSnapshot{}
	if err := s.Unmarshal(val); err != nil {
		return nil, fmt.Errorf("failed to unmarshal snapshot: %w", err)
	}

	result := make([]*RtSnapshotPeerInfo, 0, len(s.Peers))
	for i := range s.Peers {
		p := s.Peers[i]
		var id peer.ID
		if err := id.Unmarshal(p.Id); err != nil {
			logSnapshot.Warnw("failed to unmarshal peerId from snapshot", "err", err)
			continue
		}

		result = append(result, &RtSnapshotPeerInfo{
			ID:      id,
			Addrs:   p.Addresses(),
			AddedAt: time.Unix(0, p.AddedAtNs)})
	}
	return result, err
}

func (dsp *dsSnapshotter) Store(h host.Host, rt *kb.RoutingTable) error {
	pinfos := rt.GetPeerInfos()
	snapshotPeers := make([]*dht_pb.RoutingTableSnapshot_Peer, 0, len(pinfos))

	for _, p := range pinfos {
		id, err := p.Id.MarshalBinary()
		if err != nil {
			logSnapshot.Warnw("encountered error while adding peer to routing table snapshot; skipping", "peer", p.Id, "err", err)
			continue
		}
		rp := &dht_pb.RoutingTableSnapshot_Peer{}
		rp.Id = id
		addrs := h.Peerstore().Addrs(p.Id)
		rp.Addrs = make([][]byte, len(addrs))
		for i, maddr := range addrs {
			rp.Addrs[i] = maddr.Bytes()
		}

		rp.AddedAtNs = p.AddedAt.UnixNano()
		snapshotPeers = append(snapshotPeers, rp)
	}

	snap := dht_pb.RoutingTableSnapshot{
		Peers:       snapshotPeers,
		TimestampNs: time.Now().Unix(),
	}

	bytes, err := snap.Marshal()
	if err != nil {
		return fmt.Errorf("failed to marshal snapshot %w", err)
	}

	if err := dsp.Put(dsSnapshotKey, bytes); err != nil {
		return fmt.Errorf("failed to persist snapshot %w", err)
	}

	// flush to disk
	return dsp.Sync(dsSnapshotKey)
}
