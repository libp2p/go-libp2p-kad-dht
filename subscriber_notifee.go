package dht

import (
	"github.com/jbenet/goprocess"
	"github.com/libp2p/go-libp2p-core/event"
	"github.com/libp2p/go-libp2p-core/network"

	"github.com/libp2p/go-eventbus"

	ma "github.com/multiformats/go-multiaddr"
)

// subscriberNotifee implements network.Notifee and also manages the subscriber to the event bus. We consume peer
// identification events to trigger inclusion in the routing table, and we consume Disconnected events to eject peers
// from it.
type subscriberNotifee IpfsDHT

func (nn *subscriberNotifee) DHT() *IpfsDHT {
	return (*IpfsDHT)(nn)
}

func (nn *subscriberNotifee) subscribe(proc goprocess.Process) {
	dht := nn.DHT()

	dht.host.Network().Notify(nn)
	defer dht.host.Network().StopNotify(nn)

	var err error
	evts := []interface{}{
		&event.EvtPeerIdentificationCompleted{},
	}

	// subscribe to the EvtPeerIdentificationCompleted event which notifies us every time a peer successfully completes identification
	sub, err := dht.host.EventBus().Subscribe(evts, eventbus.BufSize(256))
	if err != nil {
		logger.Errorf("dht not subscribed to peer identification events; things will fail; err: %s", err)
	}
	defer sub.Close()

	for {
		select {
		case evt, more := <-sub.Out():
			// we will not be getting any more events
			if !more {
				return
			}

			// something has gone really wrong if we get an event for another type
			ev, ok := evt.(event.EvtPeerIdentificationCompleted)
			if !ok {
				logger.Errorf("got wrong type from subscription: %T", ev)
				return
			}

			// if the peer supports the DHT protocol, add it to our RT and kick a refresh if needed
			protos, err := dht.peerstore.SupportsProtocols(ev.Peer, dht.protocolStrs()...)
			if err == nil && len(protos) != 0 {
				refresh := dht.routingTable.Size() <= minRTRefreshThreshold
				dht.Update(dht.ctx, ev.Peer)
				if refresh && dht.autoRefresh {
					select {
					case dht.triggerRtRefresh <- nil:
					default:
					}
				}
			}

		case <-proc.Closing():
			return
		}
	}
}

func (nn *subscriberNotifee) Disconnected(n network.Network, v network.Conn) {
	dht := nn.DHT()
	select {
	case <-dht.Process().Closing():
		return
	default:
	}

	p := v.RemotePeer()

	// Lock and check to see if we're still connected. We lock to make sure
	// we don't concurrently process a connect event.
	dht.plk.Lock()
	defer dht.plk.Unlock()
	if dht.host.Network().Connectedness(p) == network.Connected {
		// We're still connected.
		return
	}

	dht.routingTable.Remove(p)

	if dht.routingTable.Size() < minRTRefreshThreshold {
		// TODO: Actively bootstrap. For now, just try to add the currently connected peers.
		for _, p := range dht.host.Network().Peers() {
			// Don't bother probing, we do that on connect.
			protos, err := dht.peerstore.SupportsProtocols(p, dht.protocolStrs()...)
			if err == nil && len(protos) != 0 {
				dht.Update(dht.Context(), p)
			}
		}
	}

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

func (nn *subscriberNotifee) Connected(n network.Network, v network.Conn)      {}
func (nn *subscriberNotifee) OpenedStream(n network.Network, v network.Stream) {}
func (nn *subscriberNotifee) ClosedStream(n network.Network, v network.Stream) {}
func (nn *subscriberNotifee) Listen(n network.Network, a ma.Multiaddr)         {}
func (nn *subscriberNotifee) ListenClose(n network.Network, a ma.Multiaddr)    {}
