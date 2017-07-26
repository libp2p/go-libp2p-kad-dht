package dht

import (
	"context"
	"io"

	inet "github.com/libp2p/go-libp2p-net"
	ma "github.com/multiformats/go-multiaddr"
	mstream "github.com/multiformats/go-multistream"
)

// netNotifiee defines methods to be used with the IpfsDHT
type netNotifiee IpfsDHT

func (nn *netNotifiee) DHT() *IpfsDHT {
	return (*IpfsDHT)(nn)
}

type peerTracker struct {
	refcount int
	cancel   func()
}

func (nn *netNotifiee) Connected(n inet.Network, v inet.Conn) {
	dht := nn.DHT()
	select {
	case <-dht.Process().Closing():
		return
	default:
	}

	dht.plk.Lock()
	defer dht.plk.Unlock()

	conn, ok := nn.peers[v.RemotePeer()]
	if ok {
		conn.refcount += 1
		return
	}

	ctx, cancel := context.WithCancel(dht.Context())

	nn.peers[v.RemotePeer()] = &peerTracker{
		refcount: 1,
		cancel:   cancel,
	}

	go func() {

		// Note: We *could* just check the peerstore to see if the remote side supports the dht
		// protocol, but its not clear that that information will make it into the peerstore
		// by the time this notification is sent. So just to be very careful, we brute force this
		// and open a new stream

		for {
			s, err := dht.host.NewStream(ctx, v.RemotePeer(), ProtocolDHT, ProtocolDHTOld)

			// Canceled.
			if ctx.Err() != nil {
				return
			}

			switch err {
			case nil:
				s.Close()
				dht.plk.Lock()
				defer dht.plk.Unlock()

				// Check if canceled again under the lock.
				if ctx.Err() == nil {
					dht.Update(dht.Context(), v.RemotePeer())
				}
			case io.EOF:
				// Connection died but we may still have *an* open connection so try again.
				continue
			case mstream.ErrNotSupported:
				// Client mode only, don't bother adding them to our routing table
			default:
				// real error? thats odd
				log.Errorf("checking dht client type: %s", err)
			}
			return
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

	dht.plk.Lock()
	defer dht.plk.Unlock()

	conn, ok := nn.peers[v.RemotePeer()]
	if !ok {
		// Unmatched disconnects are fine. It just means that we were
		// already connected when we registered the listener.
		return
	}
	conn.refcount -= 1
	if conn.refcount == 0 {
		delete(nn.peers, v.RemotePeer())
		conn.cancel()
		dht.routingTable.Remove(v.RemotePeer())
	}
}

func (nn *netNotifiee) OpenedStream(n inet.Network, v inet.Stream) {}
func (nn *netNotifiee) ClosedStream(n inet.Network, v inet.Stream) {}
func (nn *netNotifiee) Listen(n inet.Network, a ma.Multiaddr)      {}
func (nn *netNotifiee) ListenClose(n inet.Network, a ma.Multiaddr) {}
