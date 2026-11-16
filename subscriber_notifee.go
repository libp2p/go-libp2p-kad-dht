package dht

import (
	"fmt"

	"github.com/libp2p/go-libp2p/core/event"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/host/eventbus"
)

func (dht *IpfsDHT) startNetworkSubscriber() error {
	bufSize := eventbus.BufSize(256)

	evts := []any{
		// register for event bus notifications of when peers successfully complete identification in order to update
		// the routing table
		new(event.EvtPeerIdentificationCompleted),

		// register for event bus protocol ID changes in order to update the routing table
		new(event.EvtPeerProtocolsUpdated),

		// register for event bus notifications for when our local address/addresses change so we can
		// advertise those to the network
		new(event.EvtLocalAddressesUpdated),

		// we want to know when we are disconnecting from other peers.
		new(event.EvtPeerConnectednessChanged),
	}

	// register for event bus local reachability changes in order to trigger switching between client and server modes.
	// AutoNAT v1 reports a node-wide EvtLocalReachabilityChanged, AutoNAT v2 reports per-address results through
	// EvtHostReachableAddrsChanged. Only register for events if the DHT is operating in ModeAuto.
	if dht.auto == ModeAuto || dht.auto == ModeAutoServer {
		evts = append(evts, new(event.EvtLocalReachabilityChanged), new(event.EvtHostReachableAddrsChanged))
	}

	subs, err := dht.host.EventBus().Subscribe(evts, bufSize)
	if err != nil {
		return fmt.Errorf("dht could not subscribe to eventbus events: %w", err)
	}

	dht.wg.Go(func() {
		defer subs.Close()

		// once AutoNAT v2 has reported, its per-address verdict is more stable than v1's node-wide flag, so v1 is ignored
		v2Seen := false
		for {
			select {
			case e, more := <-subs.Out():
				if !more {
					return
				}

				switch evt := e.(type) {
				case event.EvtLocalAddressesUpdated:
					// when our address changes, we should proactively tell our closest peers about it so
					// we become discoverable quickly. The Identify protocol will push a signed peer record
					// with our new address to all peers we are connected to. However, we might not necessarily be connected
					// to our closet peers & so in the true spirit of Zen, searching for ourself in the network really is the best way
					// to to forge connections with those matter.
					if dht.autoRefresh || dht.testAddressUpdateProcessing {
						dht.requestRefresh()
					}
				case event.EvtPeerProtocolsUpdated:
					handlePeerChangeEvent(dht, evt.Peer)
				case event.EvtPeerIdentificationCompleted:
					handlePeerChangeEvent(dht, evt.Peer)
				case event.EvtPeerConnectednessChanged:
					if evt.Connectedness != network.Connected {
						dht.msgSender.OnDisconnect(dht.ctx, evt.Peer)
					}
				case event.EvtLocalReachabilityChanged:
					if dht.auto != ModeAuto && dht.auto != ModeAutoServer {
						// something has gone really wrong if we get an event we did not subscribe to
						logger.Errorf("received LocalReachabilityChanged event that was not subscribed to")
					} else if v2Seen {
						logger.Debugw("ignoring AutoNAT v1 reachability event, AutoNAT v2 already reported", "reachability", evt.Reachability)
					} else {
						handleReachabilityChange(dht, evt.Reachability)
					}
				case event.EvtHostReachableAddrsChanged:
					if dht.auto != ModeAuto && dht.auto != ModeAutoServer {
						// something has gone really wrong if we get an event we did not subscribe to
						logger.Errorf("received HostReachableAddrsChanged event that was not subscribed to")
					} else {
						v2Seen = true
						handleReachabilityChange(dht, reachabilityFromAddrs(evt))
					}
				default:
					// something has gone really wrong if we get an event for another type
					logger.Errorf("got wrong type from subscription: %T", e)
				}
			case <-dht.ctx.Done():
				return
			}
		}
	})

	return nil
}

func handlePeerChangeEvent(dht *IpfsDHT, p peer.ID) {
	valid, err := dht.validRTPeer(p)
	if err != nil {
		logger.Errorf("could not check peerstore for protocol support: err: %s", err)
		return
	} else if valid {
		dht.peerFound(p)
	} else {
		dht.peerStoppedDHT(p)
	}
}

// reachabilityFromAddrs collapses AutoNAT v2's per-address results into the node-wide reachability the mode switch
// needs: any confirmed public address makes the node public, only unreachable ones make it private, and nothing
// confirmed yet leaves it unknown.
func reachabilityFromAddrs(e event.EvtHostReachableAddrsChanged) network.Reachability {
	switch {
	case len(e.Reachable) > 0:
		return network.ReachabilityPublic
	case len(e.Unreachable) > 0:
		return network.ReachabilityPrivate
	default:
		return network.ReachabilityUnknown
	}
}

func handleReachabilityChange(dht *IpfsDHT, r network.Reachability) {
	var target mode

	switch r {
	case network.ReachabilityPrivate:
		target = modeClient
	case network.ReachabilityUnknown:
		if dht.auto == ModeAutoServer {
			target = modeServer
		} else {
			target = modeClient
		}
	case network.ReachabilityPublic:
		target = modeServer
	}

	logger.Infow("reachability changed; performing dht mode switch", "reachability", r)

	err := dht.setMode(target)
	// NOTE: the mode will be printed out as a decimal.
	if err == nil {
		logger.Infow("switched DHT mode successfully", "mode", target)
	} else {
		logger.Errorw("switching DHT mode failed", "mode", target, "error", err)
	}
}

// validRTPeer returns true if the peer supports the DHT protocol and false otherwise. Supporting the DHT protocol means
// supporting the primary protocols, we do not want to add peers that are speaking obsolete secondary protocols to our
// routing table
func (dht *IpfsDHT) validRTPeer(p peer.ID) (bool, error) {
	b, err := dht.peerstore.FirstSupportedProtocol(p, dht.protocols...)
	if len(b) == 0 || err != nil {
		return false, err
	}

	return dht.routingTablePeerFilter == nil || dht.routingTablePeerFilter(dht, p), nil
}
