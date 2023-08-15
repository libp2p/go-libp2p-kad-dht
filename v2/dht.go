package dht

import (
	"fmt"
	"sync"

	"github.com/libp2p/go-libp2p/core/event"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/plprobelab/go-kademlia/coord"
	"github.com/plprobelab/go-kademlia/kad"
	"github.com/plprobelab/go-kademlia/key"
	"github.com/plprobelab/go-kademlia/routing/triert"
	"golang.org/x/exp/slog"
)

// DHT is an implementation of Kademlia with S/Kademlia modifications.
// It is used to implement the base Routing module.
type DHT struct {
	// host holds a reference to the underlying libp2p host
	host host.Host

	// cfg holds a reference to the DHT configuration struct
	cfg *Config

	// mode indicates the current mode the DHT operates in. This can differ from
	// the desired mode if set to auto-client or auto-server. The desired mode
	// can be configured via the Config struct.
	mode   mode
	modeLk sync.RWMutex

	// kad is a reference to the go-kademlia coordinator
	kad *coord.Coordinator[key.Key256, ma.Multiaddr]

	// rt holds a reference to the routing table implementation. This can be
	// configured via the Config struct.
	rt kad.RoutingTable[key.Key256, kad.NodeID[key.Key256]]

	// log is a convenience accessor to the logging instance. It gets the value
	// of the logger field from the configuration.
	log *slog.Logger

	// sub holds a subscription to the libp2p event bus. The DHT subscribes to
	// these events in networkEventsSubscription and consumes them
	// asynchronously in consumeNetworkEvents.
	sub event.Subscription
}

// New constructs a new DHT for the given underlying host and with the given
// configuration. Use DefaultConfig() to construct a configuration.
func New(h host.Host, cfg *Config) (*DHT, error) {
	var err error

	// check if the configuration is valid
	if err = cfg.Validate(); err != nil {
		return nil, fmt.Errorf("validate DHT config: %w", err)
	}

	d := &DHT{
		host: h,
		cfg:  cfg,
		log:  cfg.Logger,
	}

	nid := nodeID(d.host.ID())

	// Use the configured routing table if it was provided
	if cfg.RoutingTable != nil {
		d.rt = cfg.RoutingTable
	} else {
		rtCfg := triert.DefaultConfig[key.Key256, kad.NodeID[key.Key256]]()
		d.rt, err = triert.New[key.Key256, kad.NodeID[key.Key256]](nid, rtCfg)
		if err != nil {
			return nil, fmt.Errorf("new trie routing table: %w", err)
		}
	}

	// instantiate a new Kademlia DHT coordinator.
	d.kad, err = coord.NewCoordinator[key.Key256, ma.Multiaddr](nid, nil, d.rt, cfg.Kademlia)
	if err != nil {
		return nil, fmt.Errorf("new coordinator: %w", err)
	}

	// determine mode to start in
	switch cfg.Mode {
	case ModeOptClient, ModeOptAutoClient:
		d.setClientMode()
	case ModeOptServer, ModeOptAutoServer:
		d.setServerMode()
	default:
		// should never happen because of the configuration validation above
		return nil, fmt.Errorf("invalid dht mode %s", cfg.Mode)
	}

	// create subscription to various network events
	d.sub, err = d.networkEventsSubscription()
	if err != nil {
		return nil, fmt.Errorf("failed subscribing to event bus: %w", err)
	}

	// consume these events asynchronously
	go d.consumeNetworkEvents(d.sub)

	return d, nil
}

// Close cleans up all resources associated with this DHT.
func (d *DHT) Close() error {
	if err := d.sub.Close(); err != nil {
		d.log.With("err", err).Debug("failed closing event bus subscription")
	}

	return nil
}

// setServerMode advertises (via libp2p identify updates) that we are able to
// respond to DHT queries for the configured protocol and sets the appropriate
// stream handler. This method is safe to call even if the DHT is already in
// server mode.
func (d *DHT) setServerMode() {
	d.modeLk.Lock()
	defer d.modeLk.Unlock()

	d.log.Info("Activating DHT server mode")

	d.mode = modeServer
	d.host.SetStreamHandler(d.cfg.ProtocolID, d.handleNewStream)
}

// setClientMode stops advertising (and rescinds advertisements via libp2p
// identify updates) that we are able to respond to DHT queries for the
// configured protocol and removes the registered stream handlers. We also kill
// all inbound streams that were utilizing the handled protocols. If we are
// already in client mode, this method is a no-op. This method is safe to call
// even if the DHT is already in client mode.
func (d *DHT) setClientMode() {
	d.modeLk.Lock()
	defer d.modeLk.Unlock()

	d.log.Info("Activating DHT client mode")

	d.mode = modeClient
	d.host.RemoveStreamHandler(d.cfg.ProtocolID)

	// kill all active inbound streams using the DHT protocol.
	for _, c := range d.host.Network().Conns() {
		for _, s := range c.GetStreams() {

			if s.Protocol() != d.cfg.ProtocolID {
				continue
			}

			if s.Stat().Direction != network.DirInbound {
				continue
			}

			if err := s.Reset(); err != nil {
				d.log.With("err", err).Debug("failed closing stream")
			}
		}
	}
}
