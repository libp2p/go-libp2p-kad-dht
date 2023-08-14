package dht

import (
	"fmt"

	"github.com/libp2p/go-libp2p/core/event"
	"github.com/libp2p/go-libp2p/core/network"
)

func (d *DHT) subscribeToNetworkEvents() error {
	evts := []interface{}{
	}

	// register for event bus local routability changes in order to trigger switching between client and server modes
	// only register for events if the DHT is operating in ModeAuto
	if d.cfg.Mode == ModeOptAutoServer || d.cfg.Mode == ModeOptAutoClient {
		evts = append(evts, new(event.EvtLocalReachabilityChanged))
	}

	sub, err := d.host.EventBus().Subscribe(evts)
	if err != nil {
		return fmt.Errorf("failed subscribing to eventbus: %w", err)
	}

	go func() {
		defer func() {
			if err := sub.Close(); err != nil {
				d.log.With("err", err).Warn("failed closing libp2p event subscription")
			}
		}()

		for evt := range sub.Out() {
			switch evt := evt.(type) {
			case event.EvtLocalReachabilityChanged:
				d.onEvtLocalReachabilityChanged(evt)
			default:
				d.log.Warn("unknown libp2p event", "type", fmt.Sprintf("%T", evt))
			}
		}
	}()

	return nil
}

// onEvtLocalReachabilityChanged handles reachability change events and sets
// the DHTs mode accordingly. We only subscribe to these events if the DHT
// operates in an automatic mode. This means we can directly change to
// client/server mode based on the reachability event and don't need to check
// if the configuration constrains us to a specific mode.
func (d *DHT) onEvtLocalReachabilityChanged(evt event.EvtLocalReachabilityChanged) {
	d.log.With("reachability", evt.Reachability.String()).
		Debug("handling reachability changed event")

	switch evt.Reachability {
	case network.ReachabilityPrivate:
		d.setClientMode()
	case network.ReachabilityUnknown:
		switch d.cfg.Mode {
		case ModeOptAutoClient:
			d.setClientMode()
		case ModeOptAutoServer:
			d.setServerMode()
		default:
			d.log.With("mode", d.cfg.Mode).Warn("unexpected mode configuration")
		}
	case network.ReachabilityPublic:
		d.setServerMode()
	default:
		d.log.With("reachability", evt.Reachability).Warn("unknown reachability type")
	}
}
