package dht

import (
	"fmt"

	"github.com/plprobelab/go-kademlia/coord"
	"github.com/plprobelab/go-kademlia/kad"
	"github.com/plprobelab/go-kademlia/key"
)

type (
	// ModeOpt describes in which mode this DHT process should operate in.
	// Possible options are client, server, and any variant that switches
	// between both automatically based on public reachability. The DHT receives
	// reachability updates from libp2p via the EvtLocalReachabilityChanged
	// event. A DHT that operates in client mode won't register a stream handler
	// for incoming requests and therefore won't store, e.g., any provider or
	// IPNS records. A DHT in server mode, on the other hand, does all of that.
	//
	// The `mode` type, on the other hand, captures the current state that the
	// DHT is in. This can either be client or server.
	ModeOpt string

	// mode describes in which mode the DHT currently operates. Because the ModeOpt
	// type has options that automatically switch between client and server mode
	// based on public connectivity, the DHT mode at any point in time can differ
	// from the desired mode. Therefore, we define this second mode type that
	// only has the two forms: client or server.
	mode string
)

const (
	// ModeOptClient configures the DHT to only operate in client mode
	// regardless of potential public reachability.
	ModeOptClient ModeOpt = "client"

	// ModeOptServer configures the DHT to always operate in server mode
	// regardless of potentially not being publicly reachable.
	ModeOptServer ModeOpt = "server"

	// ModeOptAutoClient configures the DHT to start operating in client mode
	// and if publicly reachability is detected to switch to server mode.
	ModeOptAutoClient ModeOpt = "auto-client"

	// ModeOptAutoServer configures the DHT to start operating in server mode,
	// and if it is detected that we don't have public reachability switch
	// to client mode.
	ModeOptAutoServer ModeOpt = "auto-server"

	// modeClient means that the DHT is currently operating in client mode.
	// For more information, check ModeOpt documentation.
	modeClient mode = "client"

	// modeServer means that the DHT is currently operating in server mode.
	// For more information, check ModeOpt documentation.
	modeServer mode = "server"
)

// Config contains all the configuration options for a DHT. Use DefaultConfig
// to build up your own configuration struct. The DHT constructor New uses the
// below method Validate to test for violations of configuration invariants.
type Config struct {
	// Mode defines if the DHT should operate as a server or client or switch
	// between both automatically (see ModeOpt).
	Mode ModeOpt

	// Kademlia holds the configuration of the underlying Kademlia implementation.
	Kademlia *coord.Config

	// RoutingTable holds a reference to the specific routing table
	// implementation that this DHT should use. If this field is nil, the
	// triert.TrieRT routing table will be used.
	RoutingTable kad.RoutingTable[key.Key256, kad.NodeID[key.Key256]]
}

// DefaultConfig returns a configuration struct that can be used as-is to
// instantiate a fully functional DHT client.
func DefaultConfig() *Config {
	return &Config{
		Mode:         ModeOptAutoClient,
		Kademlia:     coord.DefaultConfig(),
		RoutingTable: nil,
	}
}

// Validate validates the configuration struct it is called on. It returns
// an error if any configuration issue was detected and nil if this is
// a valid configuration.
func (c *Config) Validate() error {
	switch c.Mode {
	case ModeOptClient, ModeOptServer, ModeOptAutoClient, ModeOptAutoServer:
	default:
		return fmt.Errorf("invalid mode option: %s", c.Mode)
	}

	if c.Kademlia == nil {
		return fmt.Errorf("kademlia configuration must not be nil")
	}

	if err := c.Kademlia.Validate(); err != nil {
		return fmt.Errorf("invalid kademlia configuration: %w", err)
	}

	return nil
}
