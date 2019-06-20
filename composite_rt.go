package dht

import (
	"fmt"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/peerstore"
	kbucket "github.com/libp2p/go-libp2p-kbucket"
)

type CompositeRT struct {
	// protos maps protocols to slice indices.
	protos map[string]int

	lk sync.RWMutex
	// tracks membership of peers in routing tables.
	peers map[string]int

	// once initialized, this slice will never be written to, so no lock is needed.
	rts []*kbucket.RoutingTable
}

var _ RoutingTable = (*CompositeRT)(nil)

type CompositeRTConfig struct {
	Protocol   string
	BucketSize int
}

// TODO: collapse all these params into a struct.
func NewCompositeRT(localID peer.ID, latency time.Duration, metrics peerstore.Metrics,
	peerAdded func(peer.ID), peerRemoved func(peer.ID), config ...CompositeRTConfig) *CompositeRT {
	var (
		rts    = make([]*kbucket.RoutingTable, 0, len(config))
		protos = make(map[string]int, len(config))
	)

	for i, c := range config {
		rt := kbucket.NewRoutingTable(c.BucketSize, kbucket.ConvertPeerID(localID), latency, metrics)
		rt.PeerAdded = peerAdded
		rt.PeerRemoved = peerRemoved

		rts = append(rts, rt)
		protos[c.Protocol] = i
	}

	ret := &CompositeRT{
		peers:  make(map[string]int),
		rts:    rts,
		protos: protos,
	}
	return ret
}

func (c *CompositeRT) getRoutingTable(p string) *kbucket.RoutingTable {
	c.lk.RLock()
	defer c.lk.RUnlock()

	if idx, ok := c.peers[p]; ok {
		return c.rts[idx]
	}

	panic("tried to get routing table for untracked peer")
}

func (c *CompositeRT) Track(id peer.ID, proto string) {
	c.lk.Lock()
	defer c.lk.Unlock()

	if idx, ok := c.protos[proto]; ok {
		c.peers[string(id)] = idx
		c.peers[string(kbucket.ConvertPeerID(id))] = idx
	} else {
		panic("unrecognized protocol id")
	}
}

func (c *CompositeRT) Update(p peer.ID) (evicted peer.ID, err error) {
	return c.getRoutingTable(string(p)).Update(p)
}

func (c *CompositeRT) Remove(p peer.ID) {
	c.lk.Lock()
	delete(c.peers, string(p))
	delete(c.peers, string(kbucket.ConvertPeerID(p)))
	c.lk.Unlock()

	c.getRoutingTable(string(p)).Remove(p)
}

func (c *CompositeRT) Find(p peer.ID) peer.ID {
	return c.getRoutingTable(string(p)).Find(p)
}

func (c *CompositeRT) NearestPeer(id kbucket.ID) peer.ID {
	return c.getRoutingTable(string(id)).NearestPeer(id)
}

func (c *CompositeRT) NearestPeers(id kbucket.ID, count int) []peer.ID {
	// TODO: this implementation is a placeholder; we really need to collect data from all routing tables to avoid
	//  segregating networks.
	return c.getRoutingTable(string(id)).NearestPeers(id, count)
}

func (c *CompositeRT) ListPeers() []peer.ID {
	ret := make([]peer.ID, 0, c.Size())
	for _, rt := range c.rts {
		ret = append(ret, rt.ListPeers()...)
	}
	return ret
}

func (c *CompositeRT) Size() (size int) {
	for _, rt := range c.rts {
		size += rt.Size()
	}
	return size
}

func (c *CompositeRT) Print() {
	fmt.Println("Printing a composite routing table")
	for proto, idx := range c.protos {
		fmt.Printf("--- Routing table for protocol %s ---\n", proto)
		c.rts[idx].Print()
	}
}
