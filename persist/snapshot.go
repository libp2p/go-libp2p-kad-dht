package persist

import (
	"errors"
	"time"

	ds "github.com/ipfs/go-datastore"
	nsds "github.com/ipfs/go-datastore/namespace"
	"github.com/libp2p/go-libp2p-kad-dht/pb"
	kb "github.com/libp2p/go-libp2p-kbucket"
	"github.com/libp2p/go-libp2p-peer"
)

var (
	dsSnapshotKey = ds.NewKey("routing_table")
)

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

func (dsp *dsSnapshotter) Load() (result []peer.ID, err error) {
	val, err := dsp.Get(dsSnapshotKey)
	switch err {
	case nil:
	case ds.ErrNotFound:
		return nil, nil
	default:
		return nil, err
	}

	var s dht_pb.RoutingTableSnapshot
	if err := s.Unmarshal(val); err != nil {
		return nil, err
	}

	var pid peer.ID
	for _, id := range s.Peers {
		if err := pid.Unmarshal(id); err != nil {
			logSnapshot.Warningf("encountered invalid peer ID while restoring routing table snapshot; err: %s", err)
			continue
		}
		result = append(result, pid)
	}
	return result, err
}

func (dsp *dsSnapshotter) Store(rt *kb.RoutingTable) error {
	var data [][]byte
	for _, p := range rt.ListPeers() {
		id, err := p.MarshalBinary()
		if err != nil {
			logSnapshot.Warningf("encountered error while adding peer to routing table snapshot; skipping; err: %s", p, err)
			continue
		}
		data = append(data, id)
	}

	snap := dht_pb.RoutingTableSnapshot{
		Peers:     data,
		Timestamp: time.Now().Unix(),
	}

	bytes, err := snap.Marshal()
	if err != nil {
		return err
	}

	return dsp.Put(dsSnapshotKey, bytes)
}
