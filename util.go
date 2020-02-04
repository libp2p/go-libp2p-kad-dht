package dht

import (
	"github.com/libp2p/go-eventbus"
	"github.com/libp2p/go-libp2p-core/event"
)

// PoolSize is the number of nodes used for group find/set RPC calls
// Deprecated: No longer used
var PoolSize = 6

// KValue is the maximum number of requests to perform before returning failure.
// Deprecated: This value is still the default, but changing this parameter no longer does anything.
var KValue = 20

// AlphaValue is the concurrency factor for asynchronous requests.
// Deprecated: This value is still the default, but changing this parameter no longer does anything.
var AlphaValue = 3

// RoutabilityBasedModeSwitching uses LocalRoutabilty events sent over the DHT's EventBus to trigger switching between
// client and server modes.
func RoutabilityBasedModeSwitching(dht *IpfsDHT) {
	const modeBufSz = 256

	evts := []interface{}{
		&event.EvtLocalRoutabilityPublic{},
		&event.EvtLocalRoutabilityPrivate{},
		&event.EvtLocalRoutabilityUnknown{},
	}

	sub, err := dht.Host().EventBus().Subscribe(evts, eventbus.BufSize(modeBufSz))
	if err != nil {
		logger.Errorf("kaddht not subscribed to local routability events; dynamic mode switching will not work; err: %s", err)
	}
	defer sub.Close()

	for {
		select {
		case ev := <-sub.Out():
			var target DHTMode

			switch ev.(type) {
			case event.EvtLocalRoutabilityPrivate, event.EvtLocalRoutabilityUnknown:
				target = ModeClient
			case event.EvtLocalRoutabilityPublic:
				target = ModeServer
			}

			logger.Infof("processed event %T; performing kaddht mode switch", ev)

			err := dht.SetMode(target)
			// NOTE: the mode will be printed out as a decimal.
			if err == nil {
				logger.Infof("switched DHT mode successfully; new mode: %d", target)
			} else {
				logger.Warningf("switching DHT mode failed; new mode: %d, err: %s", target, err)
			}
		case <-dht.Context().Done():
			return
		}
	}
}
