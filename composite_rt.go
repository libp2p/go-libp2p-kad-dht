package dht

import (
	"fmt"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/peerstore"
	kbucket "github.com/libp2p/go-libp2p-kbucket"
)

type CompositeRoutingTable struct {
	// protos maps protocols to slice indices.
	protos map[string]int

	lk sync.RWMutex
	// tracks membership of peers in routing tables.
	peers map[string]int

	// once initialized, this slice will never be written to, so no lock is needed.
	rts []*kbucket.RoutingTable
}

var _ RoutingTable = (*CompositeRoutingTable)(nil)

type CompositeRoutingTableConfig struct {
	LocalID     peer.ID
	Latency     time.Duration
	Metrics     peerstore.Metrics
	PeerAdded   func(peer.ID)
	PeerRemoved func(peer.ID)
	Partitions  []CompositeRoutingTablePartition
}

type CompositeRoutingTablePartition struct {
	Protocol   string
	BucketSize int
}

// TODO: collapse all these params into a struct.
func NewCompositeRoutingTable(cfg *CompositeRoutingTableConfig) *CompositeRoutingTable {
	var (
		rts    = make([]*kbucket.RoutingTable, 0, len(cfg.Partitions))
		protos = make(map[string]int, len(cfg.Partitions))
	)

	for i, c := range cfg.Partitions {
		rt := kbucket.NewRoutingTable(c.BucketSize, kbucket.ConvertPeerID(cfg.LocalID), cfg.Latency, cfg.Metrics)
		rt.PeerAdded = cfg.PeerAdded
		rt.PeerRemoved = cfg.PeerRemoved

		rts = append(rts, rt)
		protos[c.Protocol] = i
	}

	ret := &CompositeRoutingTable{
		peers:  make(map[string]int),
		rts:    rts,
		protos: protos,
	}
	return ret
}

func (c *CompositeRoutingTable) getRoutingTable(p string) *kbucket.RoutingTable {
	c.lk.RLock()
	defer c.lk.RUnlock()

	if idx, ok := c.peers[p]; ok {
		return c.rts[idx]
	}
	return nil
}

func (c *CompositeRoutingTable) Track(id peer.ID, proto string) {
	c.lk.Lock()
	defer c.lk.Unlock()

	// if we don't have a routing table for the protocol, we ignore the peer.
	if idx, ok := c.protos[proto]; ok {
		c.peers[string(id)] = idx
		c.peers[string(kbucket.ConvertPeerID(id))] = idx
	}
}

func (c *CompositeRoutingTable) Update(p peer.ID) (evicted peer.ID, err error) {
	if rt := c.getRoutingTable(string(p)); rt != nil {
		return rt.Update(p)
	} else {
		return "", nil
	}
}

func (c *CompositeRoutingTable) Remove(p peer.ID) {
	if rt := c.getRoutingTable(string(p)); rt != nil {
		rt.Remove(p)
	} else {
		return
	}

	c.lk.Lock()
	delete(c.peers, string(p))
	delete(c.peers, string(kbucket.ConvertPeerID(p)))
	c.lk.Unlock()
}

func (c *CompositeRoutingTable) Find(p peer.ID) peer.ID {
	if rt := c.getRoutingTable(string(p)); rt != nil {
		return rt.Find(p)
	} else {
		return ""
	}
}

func (c *CompositeRoutingTable) NearestPeer(id kbucket.ID) peer.ID {
	if rt := c.getRoutingTable(string(id)); rt != nil {
		return rt.NearestPeer(id)
	} else {
		return ""
	}
}

func (c *CompositeRoutingTable) NearestPeers(id kbucket.ID, count int) []peer.ID {
	// TODO: this implementation is a placeholder; we shoudl collect data from all routing tables to avoid
	//  segregating networks.
	return c.getRoutingTable(string(id)).NearestPeers(id, count)
}

func (c *CompositeRoutingTable) ListPeers() []peer.ID {
	ret := make([]peer.ID, 0, c.Size())
	for _, rt := range c.rts {
		ret = append(ret, rt.ListPeers()...)
	}
	return ret
}

func (c *CompositeRoutingTable) Size() (size int) {
	for _, rt := range c.rts {
		size += rt.Size()
	}
	return size
}

func (c *CompositeRoutingTable) Print() {
	fmt.Println("Printing a composite routing table")
	for proto, idx := range c.protos {
		fmt.Printf("--- Routing table for protocol %s ---\n", proto)
		c.rts[idx].Print()
	}
}
