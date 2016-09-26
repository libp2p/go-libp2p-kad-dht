package dht

import (
	inet "github.com/libp2p/go-libp2p/p2p/net"
	ma "github.com/multiformats/go-multiaddr"
	mstream "github.com/whyrusleeping/go-multistream"
)

// netNotifiee defines methods to be used with the IpfsDHT
type netNotifiee IpfsDHT

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

	// Note: We *could* just check the peerstore to see if the remote side supports the dht
	// protocol, but its not clear that that information will make it into the peerstore
	// by the time this notification is sent. So just to be very careful, we brute force this
	// and open a new stream
	s, err := dht.host.NewStream(dht.Context(), v.RemotePeer(), ProtocolDHT, ProtocolDHTOld)
	switch err {
	case nil:
		s.Close()
		// connected fine? full dht node
		dht.Update(dht.Context(), v.RemotePeer())
	case mstream.ErrNotSupported:
		// Client mode only, don't bother adding them to our routing table
	default:
		// real error? thats odd
		log.Errorf("checking dht client type: %#v", err)
	}
}

func (nn *netNotifiee) Disconnected(n inet.Network, v inet.Conn) {
	dht := nn.DHT()
	select {
	case <-dht.Process().Closing():
		return
	default:
	}
	dht.routingTable.Remove(v.RemotePeer())
}

func (nn *netNotifiee) OpenedStream(n inet.Network, v inet.Stream) {}
func (nn *netNotifiee) ClosedStream(n inet.Network, v inet.Stream) {}
func (nn *netNotifiee) Listen(n inet.Network, a ma.Multiaddr)      {}
func (nn *netNotifiee) ListenClose(n inet.Network, a ma.Multiaddr) {}
