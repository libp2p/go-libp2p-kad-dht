package dht

import (
	"fmt"

	"github.com/libp2p/go-libp2p/core/event"
	"github.com/libp2p/go-libp2p/core/network"
)

// networkEventsSubscription registers a subscription on the libp2p event bus
// for several events. The DHT uses these events for various tasks like routing
// table or DHT mode updates.
func (d *DHT) networkEventsSubscription() (event.Subscription, error) {
	evts := []interface{}{
		// register for event bus notifications of when peers successfully
		// complete identification in order to update the routing table.
		new(event.EvtPeerIdentificationCompleted),

		// register for event bus protocol ID changes in order to update the
		// routing table. If a peer stops supporting the DHT protocol, we want
		// to remove it from the routing table.
		new(event.EvtPeerProtocolsUpdated),

		// register for event bus notifications for when our local
		// address/addresses change, so we can advertise those to the network
		new(event.EvtLocalAddressesUpdated),

		// we want to know when we are disconnecting from other peers.
		new(event.EvtPeerConnectednessChanged),
	}

	// register for event bus local reachability changes in order to trigger
	// switching between client and server modes. We only register for these
	// events if the DHT is operating in ModeOptAuto{Server,Client}.
	if d.cfg.Mode == ModeOptAutoServer || d.cfg.Mode == ModeOptAutoClient {
		evts = append(evts, new(event.EvtLocalReachabilityChanged))
	}

	return d.host.EventBus().Subscribe(evts)
}

// consumeNetworkEvents takes an event bus subscription and consumes all events
// emitted on that subscription. It calls out to various event handlers.
func (d *DHT) consumeNetworkEvents(sub event.Subscription) {
	for evt := range sub.Out() {
		switch evt := evt.(type) {
		case event.EvtLocalReachabilityChanged:
			d.onEvtLocalReachabilityChanged(evt)
		case event.EvtLocalAddressesUpdated:
		case event.EvtPeerProtocolsUpdated:
		case event.EvtPeerIdentificationCompleted:
		case event.EvtPeerConnectednessChanged:
		default:
			d.log.Warn("unknown libp2p event", "type", fmt.Sprintf("%T", evt))
		}
	}
}

// onEvtLocalReachabilityChanged handles reachability change events and sets
// the DHTs mode accordingly. We only subscribe to these events if the DHT
// operates in an automatic mode. This means we can directly change to
// client/server mode based on the reachability event and don't need to check
// if the configuration constrains us to a specific mode.
func (d *DHT) onEvtLocalReachabilityChanged(evt event.EvtLocalReachabilityChanged) {
	d.log.With("reachability", evt.Reachability.String()).Debug("handling reachability changed event")

	// set DHT mode based on new reachability
	switch evt.Reachability {
	case network.ReachabilityPrivate:
		d.setClientMode()
	case network.ReachabilityPublic:
		d.setServerMode()
	case network.ReachabilityUnknown:
		if d.cfg.Mode == ModeOptAutoClient {
			d.setClientMode()
		} else if d.cfg.Mode == ModeOptAutoServer {
			d.setServerMode()
		} else {
			// we should only be subscribed to EvtLocalReachabilityChanged events
			// if the DHT is configured to operate in any auto mode.
			d.log.With("mode", d.cfg.Mode).Warn("unexpected mode configuration")
		}
	default:
		d.log.With("reachability", evt.Reachability).Warn("unknown reachability type")
	}
}
