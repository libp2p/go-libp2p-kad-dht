package dht

import (
	"fmt"
	"time"

	"github.com/ipfs/boxo/ipns"
	ds "github.com/ipfs/go-datastore"
	leveldb "github.com/ipfs/go-ds-leveldb"
	logging "github.com/ipfs/go-log/v2"
	record "github.com/libp2p/go-libp2p-record"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/plprobelab/go-kademlia/coord"
	"github.com/plprobelab/go-kademlia/kad"
	"github.com/plprobelab/go-kademlia/key"
	"github.com/plprobelab/go-kademlia/routing/triert"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap/exp/zapslog"
	"golang.org/x/exp/slog"
)

// ServiceName is used to scope incoming streams for the resource manager.
const ServiceName = "libp2p.DHT"

// tracer is an open telemetry tracing instance
var tracer = otel.Tracer("go-libp2p-kad-dht")

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

	// Datastore is an interface definition that gathers the datastore
	// requirements. The DHT requires the datastore to support batching and
	// transactions. Example datastores that implement both features are leveldb
	// and badger. leveldb can also be used in memory - this is used as the
	// default datastore.
	Datastore interface {
		ds.Datastore
		ds.BatchingFeature
		ds.TxnFeature
	}
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

	// BucketSize determines the number of closer peers to return
	BucketSize int

	// ProtocolID represents the DHT protocol we can query with and respond to.
	ProtocolID protocol.ID

	// RoutingTable holds a reference to the specific routing table
	// implementation that this DHT should use. If this field is nil, the
	// triert.TrieRT routing table will be used. This field will be nil
	// in the default configuration because a routing table requires information
	// about the local node.
	RoutingTable kad.RoutingTable[key.Key256, kad.NodeID[key.Key256]]

	// Datastore configures the DHT to use the specified datastore. The
	// datastore must support batching and transactions. Defaults to a leveldb
	// in-memory (temporary) map.
	Datastore Datastore

	// Validator
	Validator record.Validator

	// Logger can be used to configure a custom structured logger instance.
	// By default go.uber.org/zap is used (wrapped in ipfs/go-log).
	Logger *slog.Logger

	// MaxRecordAge is the default time that a record should last in the DHT.
	// This value is also known as the provider record expiration.
	MaxRecordAge time.Duration

	// TimeoutStreamIdle is the duration we're reading from a stream without
	// receiving before closing/resetting it. The timeout gets reset every time
	// we have successfully read a message from the stream.
	TimeoutStreamIdle time.Duration
}

// DefaultConfig returns a configuration struct that can be used as-is to
// instantiate a fully functional DHT client. All fields that are nil require
// some additional information to instantiate. The default values for these
// fields come from separate top-level methods prefixed with Default.
func DefaultConfig() *Config {
	return &Config{
		Mode:              ModeOptAutoClient,
		Kademlia:          coord.DefaultConfig(),
		BucketSize:        20,
		ProtocolID:        "/ipfs/kad/1.0.0",
		Datastore:         nil, // nil because the initialization of a datastore can fail. An in-memory leveldb datastore will be used if this field is nil.
		Validator:         nil, // nil because the default validator requires a peerstore.KeyBook.
		RoutingTable:      nil, // nil because a routing table requires information about the local node. triert.TrieRT will be used if this field is nil.
		Logger:            slog.New(zapslog.NewHandler(logging.Logger("dht").Desugar().Core())),
		MaxRecordAge:      48 * time.Hour, // empirically measured in: https://github.com/plprobelab/network-measurements/blob/master/results/rfm17-provider-record-liveness.md
		TimeoutStreamIdle: time.Minute,    // MAGIC: could be done dynamically
	}
}

// DefaultValidator returns a namespaced validator that can validate both public
// key (under the "pk" namespace) and IPNS records (under the "ipns" namespace).
// The validator can't be initialized in DefaultConfig because it requires
// access to a peerstore.KeyBook for the IPNS validator.
func DefaultValidator(kb peerstore.KeyBook) record.Validator {
	return record.NamespacedValidator{
		"pk":   record.PublicKeyValidator{},
		"ipns": ipns.Validator{KeyBook: kb},
	}
}

// DefaultRoutingTable returns a triert.TrieRT routing table. This routing table
// cannot be initialized in DefaultConfig because it requires information about
// the local peer.
func DefaultRoutingTable(nodeID kad.NodeID[key.Key256]) (kad.RoutingTable[key.Key256, kad.NodeID[key.Key256]], error) {
	rtCfg := triert.DefaultConfig[key.Key256, kad.NodeID[key.Key256]]()
	rt, err := triert.New[key.Key256, kad.NodeID[key.Key256]](nodeID, rtCfg)
	if err != nil {
		return nil, fmt.Errorf("new trie routing table: %w", err)
	}
	return rt, nil
}

// DefaultDatastore returns an in-memory leveldb datastore.
func DefaultDatastore() (Datastore, error) {
	return leveldb.NewDatastore("", nil)
}

// Validate validates the configuration struct it is called on. It returns
// an error if any configuration issue was detected and nil if this is
// a valid configuration.
func (c *Config) Validate() error {
	switch c.Mode {
	case ModeOptClient:
	case ModeOptServer:
	case ModeOptAutoClient:
	case ModeOptAutoServer:
	default:
		return fmt.Errorf("invalid mode option: %s", c.Mode)
	}

	if c.Kademlia == nil {
		return fmt.Errorf("kademlia configuration must not be nil")
	}

	if err := c.Kademlia.Validate(); err != nil {
		return fmt.Errorf("invalid kademlia configuration: %w", err)
	}

	if c.ProtocolID == "" {
		return fmt.Errorf("protocolID must not be empty")
	}

	if c.Logger == nil {
		return fmt.Errorf("logger must not be nil")
	}

	if c.MaxRecordAge <= 0 {
		return fmt.Errorf("max record age must be a positive duration")
	}

	if c.TimeoutStreamIdle <= 0 {
		return fmt.Errorf("stream idle timeout must be a positive duration")
	}

	return nil
}
