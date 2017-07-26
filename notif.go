package dht

import (
	"context"
	"io"
	"time"

	inet "github.com/libp2p/go-libp2p-net"
	ma "github.com/multiformats/go-multiaddr"
	mstream "github.com/multiformats/go-multistream"
)

// TODO: There is a race condition here where we could process notifications
// out-of-order and incorrectly mark some peers as DHT nodes (or not DHT nodes).
// The correct fix for this is nasty so I'm not really sure it's worth it.
// Incorrectly recording or failing to record a DHT node in the routing table
// isn't a big issue.

const dhtCheckTimeout = 10 * time.Second

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

	go func() {

		// Note: We *could* just check the peerstore to see if the remote side supports the dht
		// protocol, but its not clear that that information will make it into the peerstore
		// by the time this notification is sent. So just to be very careful, we brute force this
		// and open a new stream

		// TODO: There's a race condition here where the connection may
		// not be open (and we may sit here trying to connect). I've
		// added a timeout but that's not really the correct fix.

		ctx, cancel := context.WithTimeout(dht.Context(), dhtCheckTimeout)
		defer cancel()
		s, err := dht.host.NewStream(ctx, v.RemotePeer(), ProtocolDHT, ProtocolDHTOld)

		switch err {
		case nil:
			s.Close()
			// connected fine? full dht node
			dht.Update(dht.Context(), v.RemotePeer())
		case mstream.ErrNotSupported:
			// Client mode only, don't bother adding them to our routing table
		case io.EOF:
			// This is kindof an error, but it happens someone often so make it a warning
			log.Warningf("checking dht client type: %s", err)
		default:
			// real error? thats odd
			log.Errorf("checking dht client type: %s", err)
		}
	}()
}

func (nn *netNotifiee) Disconnected(n inet.Network, v inet.Conn) {
	dht := nn.DHT()
	select {
	case <-dht.Process().Closing():
		return
	default:
	}
	go dht.routingTable.Remove(v.RemotePeer())
}

func (nn *netNotifiee) OpenedStream(n inet.Network, v inet.Stream) {}
func (nn *netNotifiee) ClosedStream(n inet.Network, v inet.Stream) {}
func (nn *netNotifiee) Listen(n inet.Network, a ma.Multiaddr)      {}
func (nn *netNotifiee) ListenClose(n inet.Network, a ma.Multiaddr) {}
