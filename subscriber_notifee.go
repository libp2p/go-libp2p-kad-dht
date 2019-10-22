package dht

import (
	"github.com/jbenet/goprocess"
	"github.com/libp2p/go-libp2p-core/event"
	"github.com/libp2p/go-libp2p-core/network"
	ma "github.com/multiformats/go-multiaddr"
)

// subscriberNotifee implements network.Notifee and also manages the subscriber to the event bus. We consume peer
// identification events to trigger inclusion in the routing table, and we consume Disconnected events to eject peers
// from it.
type subscriberNotifee IpfsDHT

func (nn *subscriberNotifee) DHT() *IpfsDHT {
	return (*IpfsDHT)(nn)
}

func (nn *subscriberNotifee) Process() goprocess.Process {
	dht := nn.DHT()

	proc := goprocess.Go(nn.subscribe)
	dht.host.Network().Notify(nn)
	proc.SetTeardown(func() error {
		dht.host.Network().StopNotify(nn)
		return nil
	})
	return proc
}

func (nn *subscriberNotifee) subscribe(proc goprocess.Process) {
	dht := nn.DHT()
	for {
		select {
		case evt, more := <-dht.subscriptions.evtPeerIdentification.Out():
			if !more {
				return
			}
			switch ev := evt.(type) {
			case event.EvtPeerIdentificationCompleted:
				protos, err := dht.peerstore.SupportsProtocols(ev.Peer, dht.protocolStrs()...)
				if err == nil && len(protos) != 0 {
					bootstrap := dht.routingTable.Size() <= minRTBootstrapThreshold
					dht.Update(dht.ctx, ev.Peer)
					if bootstrap {
						select {
						case dht.triggerBootstrap <- struct{}{}:
						default:
						}
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
