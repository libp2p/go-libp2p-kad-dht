package dht

import (
	"fmt"
	"sync"

	"github.com/libp2p/go-libp2p/core/host"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/plprobelab/go-kademlia/coord"
	"github.com/plprobelab/go-kademlia/kad"
	"github.com/plprobelab/go-kademlia/key"
	"github.com/plprobelab/go-kademlia/query"
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
	mode mode

	// kad is a reference to the go-kademlia coordinator
	kad *coord.Coordinator[key.Key256, ma.Multiaddr]

	// rt holds a reference to the routing table implementation. This can be
	// configured via the Config struct.
	rt kad.RoutingTable[key.Key256, kad.NodeID[key.Key256]]

	// --- go-kademlia specific fields below ---

	// qSubs tracks a mapping of queries to their subscribers
	qSubs   map[query.QueryID]chan<- kad.Response[key.Key256, ma.Multiaddr]
	qSubsLk sync.RWMutex
	qSubCnt uint64
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
		host:  h,
		cfg:   cfg,
		qSubs: make(map[query.QueryID]chan<- kad.Response[key.Key256, ma.Multiaddr]),
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
