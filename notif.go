package dht

import (
	inet "github.com/libp2p/go-libp2p-net"
	ma "github.com/multiformats/go-multiaddr"
	mstream "github.com/multiformats/go-multistream"
)

// netNotifiee defines methods to be used with the IpfsDHT
type netNotifiee IpfsDHT

var dhtProtocols = []string{string(ProtocolDHT), string(ProtocolDHTOld)}

func (nn *netNotifiee) DHT() *IpfsDHT {
	return (*IpfsDHT)(nn)
}

func (nn *netNotifiee) Connected(n inet.Network, v inet.Conn) {
	dht := nn.DHT()
	select {
	case <-dht.Process().Closing():
		return
	default:
	}

	p := v.RemotePeer()
	protos, err := dht.peerstore.SupportsProtocols(p, dhtProtocols...)
	if err == nil && len(protos) != 0 {
		dht.plk.Lock()
		defer dht.plk.Unlock()
		if dht.host.Network().Connectedness(p) == inet.Connected {
			dht.Update(dht.Context(), p)
		}
		return
	}

	// Note: Unfortunately, the peerstore may not yet now that this peer is
	// a DHT server. So, if it didn't return a positive response above, test
	// manually.
	go nn.testConnection(v)
}

func (nn *netNotifiee) testConnection(v inet.Conn) {
	dht := nn.DHT()
	p := v.RemotePeer()

	// Forcibly use *this* connection. Otherwise, if we have two connections, we could:
	// 1. Test it twice.
	// 2. Have it closed from under us leaving the second (open) connection untested.
	s, err := v.NewStream()
	if err != nil {
		// Connection error
		return
	}
	defer s.Close()

	selected, err := mstream.SelectOneOf(dhtProtocols, s)
	if err != nil {
		// Doesn't support the protocol
		return
	}
	// Remember this choice (makes subsequent negotiations faster)
	dht.peerstore.AddProtocols(p, selected)

	dht.plk.Lock()
	defer dht.plk.Unlock()
	// Make sure we're still connected under the lock (race with disconnect)
	if dht.host.Network().Connectedness(p) == inet.Connected {
		dht.Update(dht.Context(), p)
	}
}

func (nn *netNotifiee) Disconnected(n inet.Network, v inet.Conn) {
	dht := nn.DHT()
	select {
	case <-dht.Process().Closing():
		return
	default:
	}

	p := v.RemotePeer()

	dht.plk.Lock()
	defer dht.plk.Unlock()
	if dht.host.Network().Connectedness(p) == inet.Connected {
		// We're still connected.
		return
	}

	dht.routingTable.Remove(p)

	dht.smlk.Lock()
	defer dht.smlk.Unlock()
	ms, ok := dht.strmap[p]
	if !ok {
		return
	}
	delete(dht.strmap, p)

	// Do this asynchronously as ms.lk can block for a while.
	go func() {
		ms.lk.Lock()
		defer ms.lk.Unlock()
		ms.invalidate()
	}()
}

func (nn *netNotifiee) OpenedStream(n inet.Network, v inet.Stream) {}
func (nn *netNotifiee) ClosedStream(n inet.Network, v inet.Stream) {}
func (nn *netNotifiee) Listen(n inet.Network, a ma.Multiaddr)      {}
func (nn *netNotifiee) ListenClose(n inet.Network, a ma.Multiaddr) {}
