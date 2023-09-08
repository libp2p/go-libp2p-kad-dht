package dht

import (
	"fmt"
	"time"

	"github.com/benbjohnson/clock"
	ds "github.com/ipfs/go-datastore"
	leveldb "github.com/ipfs/go-ds-leveldb"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	ma "github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
	"github.com/plprobelab/go-kademlia/coord"
	"github.com/plprobelab/go-kademlia/kad"
	"github.com/plprobelab/go-kademlia/key"
	"github.com/plprobelab/go-kademlia/routing"
	"github.com/plprobelab/go-kademlia/routing/triert"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap/exp/zapslog"
	"golang.org/x/exp/slog"
)

// ServiceName is used to scope incoming streams for the resource manager.
const ServiceName = "libp2p.DHT"

const (
	// ProtocolAmino is the protocol identifier for the main Amino DHT network.
	// If the DHT is configured with this protocol, you must configure backends
	// for IPNS, Public Key, and provider records (ipns, pk, and providers
	// namespaces). Configuration validation will fail if backends are missing.
	ProtocolAmino protocol.ID = "/ipfs/kad/1.0.0"

	// ProtocolFilecoin is the protocol identifier for Filecoin mainnet. If this
	// protocol is configured, the DHT won't automatically add support for any
	// of the above record types.
	ProtocolFilecoin protocol.ID = "/fil/kad/testnetnet/kad/1.0.0"
)

type (
	// ModeOpt describes in which mode this [DHT] process should operate in.
	// Possible options are client, server, and any variant that switches
	// between both automatically based on public reachability. The [DHT] receives
	// reachability updates from libp2p via the EvtLocalReachabilityChanged
	// event. A [DHT] that operates in client mode won't register a stream handler
	// for incoming requests and therefore won't store, e.g., any provider or
	// IPNS records. A [DHT] in server mode, on the other hand, does all of that.
	//
	// The unexported "mode" type, on the other hand, captures the current state
	// that the [DHT] is in. This can either be client or server.
	ModeOpt string

	// mode describes in which mode the [DHT] currently operates. Because the [ModeOpt]
	// type has options that automatically switch between client and server mode
	// based on public connectivity, the [DHT] mode at any point in time can differ
	// from the desired mode. Therefore, we define this second mode type that
	// only has the two forms: client or server.
	mode string

	// Datastore is an interface definition that gathers the datastore
	// requirements. The [DHT] requires the datastore to support batching and
	// transactions. Example datastores that implement both features are leveldb
	// and badger. leveldb can also be used in memory - this is used as the
	// default datastore.
	Datastore interface {
		ds.Datastore
		ds.BatchingFeature
		ds.TxnFeature
	}

	AddressFilter func([]ma.Multiaddr) []ma.Multiaddr
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

	// modeClient means that the [DHT] is currently operating in client [mode].
	// For more information, check ModeOpt documentation.
	modeClient mode = "client"

	// modeServer means that the [DHT] is currently operating in server [mode].
	// For more information, check ModeOpt documentation.
	modeServer mode = "server"
)

// Config contains all the configuration options for a [DHT]. Use [DefaultConfig]
// to build up your own configuration struct. The [DHT] constructor [New] uses the
// below method [*Config.Validate] to test for violations of configuration invariants.
type Config struct {
	// Clock
	Clock clock.Clock

	// Mode defines if the DHT should operate as a server or client or switch
	// between both automatically (see ModeOpt).
	Mode ModeOpt

	// Kademlia holds the configuration of the underlying Kademlia implementation.
	Kademlia *coord.Config

	// BucketSize determines the number of closer peers to return
	BucketSize int

	// BootstrapPeers is the list of peers that should be used to bootstrap
	// into the DHT network.
	BootstrapPeers []peer.AddrInfo

	// ProtocolID represents the DHT [protocol] we can query with and respond to.
	//
	// [protocol]: https://docs.libp2p.io/concepts/fundamentals/protocols/
	ProtocolID protocol.ID

	// RoutingTable holds a reference to the specific routing table
	// implementation that this DHT should use. If this field is nil, the
	// [triert.TrieRT] routing table will be used. This field will be nil
	// in the default configuration because a routing table requires information
	// about the local node.
	RoutingTable routing.RoutingTableCpl[key.Key256, kad.NodeID[key.Key256]]

	// The Backends field holds a map of key namespaces to their corresponding
	// backend implementation. For example, if we received an IPNS record, the
	// key will have the form "/ipns/$binary_id". We will forward the handling
	// of this record to the corresponding backend behind the "ipns" key in this
	// map. A backend does record validation and handles the storage of the
	// record. If this map stays empty, it will be populated with the default
	// IPNS ([NewBackendIPNS]), PublicKey ([NewBackendPublicKey]), and
	// Providers ([NewBackendProvider]) backends.
	//
	// Backends that implement the [io.Closer] interface will get closed when
	// the DHT is closed.
	Backends map[string]Backend

	// Datastore will be used to construct the default backends. If this is nil,
	// an in-memory leveldb from [InMemoryDatastore] will be used for all
	// backends.
	// If you want to use individual datastores per backend, you will need to
	// construct them individually and register them with the above Backends
	// map. Note that if you configure the DHT to use [ProtocolAmino] it is
	// required to register backends for the ipns, pk, and providers namespaces.
	//
	// This datastore must be thread-safe.
	Datastore Datastore

	// Logger can be used to configure a custom structured logger instance.
	// By default go.uber.org/zap is used (wrapped in ipfs/go-log).
	Logger *slog.Logger

	// TimeoutStreamIdle is the duration we're reading from a stream without
	// receiving before closing/resetting it. The timeout gets reset every time
	// we have successfully read a message from the stream.
	TimeoutStreamIdle time.Duration

	// AddressFilter is used to filter the addresses we put into the peer store and
	// also fetch from the peer store and serve to other peers. It is mainly
	// used to filter out private addresses.
	AddressFilter AddressFilter

	// MeterProvider provides access to named Meter instances. It's used to,
	// e.g., expose prometheus metrics. Check out the [opentelemetry docs]:
	//
	// [opentelemetry docs]: https://opentelemetry.io/docs/specs/otel/metrics/api/#meterprovider
	MeterProvider metric.MeterProvider

	// TracerProvider provides Tracers that are used by instrumentation code to
	// trace computational workflows. Check out the [opentelemetry docs]:
	//
	// [opentelemetry docs]: https://opentelemetry.io/docs/concepts/signals/traces/#tracer-provider
	TracerProvider trace.TracerProvider
}

// DefaultConfig returns a configuration struct that can be used as-is to
// instantiate a fully functional [DHT] client. All fields that are nil require
// some additional information to instantiate. The default values for these
// fields come from separate top-level methods prefixed with Default.
func DefaultConfig() *Config {
	return &Config{
		Clock:             clock.New(),
		Mode:              ModeOptAutoClient,
		Kademlia:          coord.DefaultConfig(),
		BucketSize:        20, // MAGIC
		BootstrapPeers:    DefaultBootstrapPeers(),
		ProtocolID:        ProtocolAmino,
		RoutingTable:      nil,                  // nil because a routing table requires information about the local node. triert.TrieRT will be used if this field is nil.
		Backends:          map[string]Backend{}, // if empty and [ProtocolAmino] is used, it'll be populated with the ipns, pk and providers backends
		Datastore:         nil,
		Logger:            slog.New(zapslog.NewHandler(logging.Logger("dht").Desugar().Core())),
		TimeoutStreamIdle: time.Minute, // MAGIC
		AddressFilter:     AddrFilterPrivate,
		MeterProvider:     otel.GetMeterProvider(),
		TracerProvider:    otel.GetTracerProvider(),
	}
}

// DefaultRoutingTable returns a triert.TrieRT routing table. This routing table
// cannot be initialized in [DefaultConfig] because it requires information
// about the local peer.
func DefaultRoutingTable(nodeID kad.NodeID[key.Key256]) (routing.RoutingTableCpl[key.Key256, kad.NodeID[key.Key256]], error) {
	rtCfg := triert.DefaultConfig[key.Key256, kad.NodeID[key.Key256]]()
	rt, err := triert.New[key.Key256, kad.NodeID[key.Key256]](nodeID, rtCfg)
	if err != nil {
		return nil, fmt.Errorf("new trie routing table: %w", err)
	}
	return rt, nil
}

// InMemoryDatastore returns an in-memory leveldb datastore.
func InMemoryDatastore() (Datastore, error) {
	return leveldb.NewDatastore("", nil)
}

// Validate validates the configuration struct it is called on. It returns
// an error if any configuration issue was detected and nil if this is
// a valid configuration.
func (c *Config) Validate() error {
	if c.Clock == nil {
		return fmt.Errorf("clock must not be nil")
	}

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

	if c.BucketSize == 0 {
		return fmt.Errorf("bucket size must not be 0")
	}

	if len(c.BootstrapPeers) == 0 {
		return fmt.Errorf("no bootstrap peer")
	}

	if c.ProtocolID == "" {
		return fmt.Errorf("protocolID must not be empty")
	}

	if c.Logger == nil {
		return fmt.Errorf("logger must not be nil")
	}

	if c.TimeoutStreamIdle <= 0 {
		return fmt.Errorf("stream idle timeout must be a positive duration")
	}

	if c.ProtocolID == ProtocolAmino && len(c.Backends) != 0 {
		if len(c.Backends) != 3 {
			return fmt.Errorf("ipfs protocol requires exactly three backends")
		}

		if _, found := c.Backends[namespaceIPNS]; !found {
			return fmt.Errorf("ipfs protocol requires an IPNS backend")
		}

		if _, found := c.Backends[namespacePublicKey]; !found {
			return fmt.Errorf("ipfs protocol requires a public key backend")
		}

		if _, found := c.Backends[namespaceProviders]; !found {
			return fmt.Errorf("ipfs protocol requires a providers backend")
		}
	}

	if c.AddressFilter == nil {
		return fmt.Errorf("address filter must not be nil - use AddrFilterIdentity to disable filtering")
	}

	if c.MeterProvider == nil {
		return fmt.Errorf("opentelemetry meter provider must not be nil")
	}

	if c.TracerProvider == nil {
		return fmt.Errorf("opentelemetry tracer provider must not be nil")
	}

	return nil
}

// AddrFilterIdentity is an [AddressFilter] that does not apply any filtering
// and just returns that passed-in multi addresses without modification.
func AddrFilterIdentity(maddrs []ma.Multiaddr) []ma.Multiaddr {
	return maddrs
}

// AddrFilterPrivate filters out any multiaddresses that are private. It
// evaluates the [manet.IsPublicAddr] on each multiaddress, and if it returns
// true, the multiaddress will be in the result set.
func AddrFilterPrivate(maddrs []ma.Multiaddr) []ma.Multiaddr {
	return ma.FilterAddrs(maddrs, manet.IsPublicAddr)
}

// AddrFilterPublic filters out any multiaddresses that are public. It
// evaluates the [manet.IsIPLoopback] on each multiaddress, and if it returns
// true, the multiaddress will be in the result set.
func AddrFilterPublic(maddrs []ma.Multiaddr) []ma.Multiaddr {
	return ma.FilterAddrs(maddrs, func(maddr ma.Multiaddr) bool { return !manet.IsIPLoopback(maddr) })
}
