package dht

import (
	"crypto/sha256"
	"fmt"
	"io"
	"sync"

	"github.com/ipfs/go-datastore/trace"
	"github.com/libp2p/go-libp2p/core/event"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/plprobelab/go-kademlia/kad"
	"github.com/plprobelab/go-kademlia/key"
	"github.com/plprobelab/go-kademlia/routing"
	"golang.org/x/exp/slog"

	"github.com/libp2p/go-libp2p-kad-dht/v2/coord"
	"github.com/libp2p/go-libp2p-kad-dht/v2/kadt"
	"github.com/libp2p/go-libp2p-kad-dht/v2/tele"
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
	modeMu sync.RWMutex
	mode   mode

	// kad is a reference to the go-kademlia coordinator
	kad *coord.Coordinator

	// rt holds a reference to the routing table implementation. This can be
	// configured via the Config struct.
	rt routing.RoutingTableCpl[key.Key256, kad.NodeID[key.Key256]]

	// backends
	backends map[string]Backend

	// log is a convenience accessor to the logging instance. It gets the value
	// of the logger field from the configuration.
	log *slog.Logger

	// sub holds a subscription to the libp2p event bus. The DHT subscribes to
	// these events in networkEventsSubscription and consumes them
	// asynchronously in consumeNetworkEvents.
	sub event.Subscription

	// tele holds a reference to a telemetry struct
	tele *tele.Telemetry
}

// New constructs a new [DHT] for the given underlying host and with the given
// configuration. Use [DefaultConfig] to construct a configuration.
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

	nid := kadt.PeerID(d.host.ID())

	// Use the configured routing table if it was provided
	if cfg.RoutingTable != nil {
		d.rt = cfg.RoutingTable
	} else if d.rt, err = DefaultRoutingTable(nid); err != nil {
		return nil, fmt.Errorf("new trie routing table: %w", err)
	}

	// initialize a new telemetry struct
	d.tele, err = tele.New(cfg.MeterProvider, cfg.TracerProvider)
	if err != nil {
		return nil, fmt.Errorf("init telemetry: %w", err)
	}

	if len(cfg.Backends) != 0 {
		d.backends = cfg.Backends
	} else if cfg.ProtocolID == ProtocolAmino {

		var dstore Datastore
		if cfg.Datastore != nil {
			dstore = cfg.Datastore
		} else if dstore, err = InMemoryDatastore(); err != nil {
			return nil, fmt.Errorf("new default datastore: %w", err)
		}

		// wrap datastore in open telemetry tracing
		dstore = trace.New(dstore, d.tele.Tracer)

		pbeCfg, err := DefaultProviderBackendConfig()
		if err != nil {
			return nil, fmt.Errorf("default provider config: %w", err)
		}
		pbeCfg.Logger = cfg.Logger
		pbeCfg.AddressFilter = cfg.AddressFilter
		pbeCfg.Tele = d.tele
		pbeCfg.clk = d.cfg.Clock

		pbe, err := NewBackendProvider(h.Peerstore(), dstore, pbeCfg)
		if err != nil {
			return nil, fmt.Errorf("new provider backend: %w", err)
		}

		rbeCfg, err := DefaultRecordBackendConfig()
		if err != nil {
			return nil, fmt.Errorf("default provider config: %w", err)
		}
		rbeCfg.Logger = cfg.Logger
		rbeCfg.Tele = d.tele
		rbeCfg.clk = d.cfg.Clock

		ipnsBe, err := NewBackendIPNS(dstore, h.Peerstore(), rbeCfg)
		if err != nil {
			return nil, fmt.Errorf("new ipns backend: %w", err)
		}

		pkBe, err := NewBackendPublicKey(dstore, rbeCfg)
		if err != nil {
			return nil, fmt.Errorf("new public key backend: %w", err)
		}

		d.backends = map[string]Backend{
			"ipns":      ipnsBe,
			"pk":        pkBe,
			"providers": pbe,
		}
	}

	// wrap all backends with tracing
	for ns, be := range d.backends {
		d.backends[ns] = traceWrapBackend(ns, be, d.tele.Tracer)
	}

	// instantiate a new Kademlia DHT coordinator.
	d.kad, err = coord.NewCoordinator(d.host.ID(), &Router{host: h}, d.rt, nil)
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

	for ns, b := range d.backends {
		closer, ok := b.(io.Closer)
		if !ok {
			continue
		}

		if err := closer.Close(); err != nil {
			d.log.Warn("failed closing backend", "namespace", ns, "err", err.Error())
		}
	}

	// TODO: improve the following.
	// If the protocol is the IPFS kademlia protocol
	// and the user didn't provide a datastore implementation, we have initialized
	// an in-memory datastore and assigned it to all backends. In the following
	// we check if the conditions are met that we have initialized the datastore
	// and the get hold of a reference to that datastore by looking in our
	// backends map and casting one to one of our known providers.
	if d.cfg.ProtocolID == ProtocolAmino && d.cfg.Datastore == nil {
		if pbe, err := typedBackend[*ProvidersBackend](d, namespaceProviders); err == nil {
			if err := pbe.datastore.Close(); err != nil {
				d.log.Warn("failed closing in memory datastore", "err", err.Error())
			}
		}
	}

	// kill all active streams using the DHT protocol.
	for _, c := range d.host.Network().Conns() {
		for _, s := range c.GetStreams() {

			if s.Protocol() != d.cfg.ProtocolID {
				continue
			}

			if err := s.Reset(); err != nil {
				d.log.With("err", err).Debug("failed closing stream")
			}
		}
	}

	return nil
}

// setServerMode advertises (via libp2p identify updates) that we are able to
// respond to DHT queries for the configured protocol and sets the appropriate
// stream handler. This method is safe to call even if the DHT is already in
// server mode.
func (d *DHT) setServerMode() {
	d.modeMu.Lock()
	defer d.modeMu.Unlock()

	d.log.Info("Activating DHT server mode")

	d.mode = modeServer
	d.host.SetStreamHandler(d.cfg.ProtocolID, d.streamHandler)
}

// setClientMode stops advertising (and rescinds advertisements via libp2p
// identify updates) that we are able to respond to DHT queries for the
// configured protocol and removes the registered stream handlers. We also kill
// all inbound streams that were utilizing the handled protocols. If we are
// already in client mode, this method is a no-op. This method is safe to call
// even if the DHT is already in client mode.
func (d *DHT) setClientMode() {
	d.modeMu.Lock()
	defer d.modeMu.Unlock()

	d.log.Info("Activating DHT client mode")

	d.mode = modeClient
	d.host.RemoveStreamHandler(d.cfg.ProtocolID)

	// kill all active inbound streams using the DHT protocol. Note that if we
	// request something from a remote peer behind a NAT that succeeds with a
	// connection reversal, the connection would be inbound but the stream would
	// still be outbound and therefore not reset here.
	for _, c := range d.host.Network().Conns() {
		for _, s := range c.GetStreams() {

			if s.Protocol() != d.cfg.ProtocolID {
				continue
			}

			switch s.Stat().Direction {
			case network.DirUnknown:
			case network.DirInbound:
			case network.DirOutbound:
				// don't reset outbound connections because these are queries
				// that we have initiated.
				continue
			}

			if err := s.Reset(); err != nil {
				d.log.With("err", err).Debug("failed closing stream")
			}
		}
	}
}

// logErr is a helper method that uses the slogger of the DHT and writes a
// warning log line with the given message alongside the error. If the error
// is nil, this method is a no-op.
func (d *DHT) logErr(err error, msg string) {
	if err == nil {
		return
	}

	d.log.Warn(msg, "err", err.Error())
}

// newSHA256Key returns a [key.Key256] that conforms to the [kad.Key] interface by
// SHA256 hashing the given bytes and wrapping them in a [key.Key256].
func newSHA256Key(data []byte) key.Key256 {
	h := sha256.Sum256(data)
	return key.NewKey256(h[:])
}

// typedBackend returns the backend at the given namespace. It is casted to the
// provided type. If the namespace doesn't exist or the type cast failed, this
// function returns an error. Can't be a method on [DHT] because of the generic
// type constraint [0].
//
// This method is only used in tests and the [DHT.Close] method. It would be
// great if we wouldn't need this method.
//
// [0]: https://github.com/golang/go/issues/49085
func typedBackend[T Backend](d *DHT, namespace string) (T, error) {
	// check if backend was registered
	be, found := d.backends[namespace]
	if !found {
		return *new(T), fmt.Errorf("backend for namespace %s not found", namespace)
	}

	// try to cast to the desired type
	cbe, ok := be.(T) // casted backend
	if !ok {
		// that didn't work... check if the desired backend was wrapped
		// into a traced backend
		tbe, ok := be.(*tracedBackend)
		if !ok {
			return *new(T), fmt.Errorf("backend at namespace is no traced backend nor %T", *new(T))
		}

		cbe, ok := tbe.backend.(T)
		if !ok {
			return *new(T), fmt.Errorf("traced backend doesn't contain %T", *new(T))
		}

		return cbe, nil
	}

	return cbe, nil
}
