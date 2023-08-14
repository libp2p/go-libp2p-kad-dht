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
	host host.Host // host holds a reference to the underlying libp2p host
	cfg  *Config   // cfg holds a reference to the DHT configuration struct
	mode mode      // mode indicates the current mode the DHT operates in. This can differ from the desired mode if set to auto-client or auto-server.

	kad *coord.Coordinator[key.Key256, ma.Multiaddr] // the go-kademlia reference
	rt  kad.RoutingTable[key.Key256, kad.NodeID[key.Key256]]
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
		d.mode = modeClient
	case ModeOptServer, ModeOptAutoServer:
		d.mode = modeServer
	default:
		// should never happen because of the configuration validation above
		return nil, fmt.Errorf("invalid dht mode %s", cfg.Mode)
	}

	return d, nil
}
