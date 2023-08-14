package dht

import (
	"fmt"

	"github.com/libp2p/go-libp2p/core/host"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/plprobelab/go-kademlia/coord"
	"github.com/plprobelab/go-kademlia/kad"
	"github.com/plprobelab/go-kademlia/key"
	"github.com/plprobelab/go-kademlia/routing/triert"
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

	if err := d.subscribeToNetworkEvents(); err != nil {
		return nil, err
	}

	return d, nil
}

// setServerMode advertises (via libp2p identify updates) that we are able to respond to DHT queries and sets the appropriate stream handlers.
// Note: We may support responding to queries with protocols aside from our primary ones in order to support
// interoperability with older versions of the DHT protocol.
func (d *DHT) setServerMode() {
	d.modeLk.Lock()
	defer d.modeLk.Unlock()

	if d.mode == modeServer {
		return
	}

	d.mode = modeServer
}

// moveToClientMode stops advertising (and rescinds advertisements via libp2p identify updates) that we are able to
// respond to DHT queries and removes the appropriate stream handlers. We also kill all inbound streams that were
// utilizing the handled protocols.
// Note: We may support responding to queries with protocols aside from our primary ones in order to support
// interoperability with older versions of the DHT protocol.
func (d *DHT) setClientMode() {
	d.modeLk.Lock()
	defer d.modeLk.Unlock()

	if d.mode == modeClient {
		return
	}

	d.mode = modeClient
}
