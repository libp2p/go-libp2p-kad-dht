package dht

import (
	"crypto/sha256"
	"fmt"
	"io"
	"sync"

	"github.com/iand/zikade/kademlia"
	"github.com/ipfs/go-datastore/trace"
	kadt "github.com/libp2p/go-libp2p-kad-dht/v2/kadt"
	"github.com/libp2p/go-libp2p/core/event"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/plprobelab/go-kademlia/kad"
	"github.com/plprobelab/go-kademlia/key"
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
	modeMu sync.RWMutex
	mode   mode

	// kad is a reference to the go-kademlia coordinator
	kad *kademlia.Dht[key.Key256, ma.Multiaddr]

	// rt holds a reference to the routing table implementation. This can be
	// configured via the Config struct.
	rt kad.RoutingTable[key.Key256, kad.NodeID[key.Key256]]

	// backends
	backends map[string]Backend

	// log is a convenience accessor to the logging instance. It gets the value
	// of the logger field from the configuration.
	log *slog.Logger

	// sub holds a subscription to the libp2p event bus. The DHT subscribes to
	// these events in networkEventsSubscription and consumes them
	// asynchronously in consumeNetworkEvents.
	sub event.Subscription
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

	if len(cfg.Backends) != 0 {
		d.backends = cfg.Backends
	} else if cfg.ProtocolID == ProtocolIPFS {

		var dstore Datastore
		if cfg.Datastore != nil {
			dstore = cfg.Datastore
		} else if dstore, err = InMemoryDatastore(); err != nil {
			return nil, fmt.Errorf("new default datastore: %w", err)
		}

		// wrap datastore in open telemetry tracing
		dstore = trace.New(dstore, tracer)

		pbeCfg := DefaultProviderBackendConfig()
		pbeCfg.Logger = cfg.Logger
		pbeCfg.AddressFilter = cfg.AddressFilter

		pbe, err := NewBackendProvider(h.Peerstore(), dstore, pbeCfg)
		if err != nil {
			return nil, fmt.Errorf("new provider backend: %w", err)
		}

		rbeCfg := DefaultRecordBackendConfig()
		rbeCfg.Logger = cfg.Logger

		d.backends = map[string]Backend{
			"ipns":      NewBackendIPNS(dstore, h.Peerstore(), rbeCfg),
			"pk":        NewBackendPublicKey(dstore, rbeCfg),
			"providers": pbe,
		}
	}

	// instantiate a new Kademlia DHT coordinator.
	d.kad, err = kademlia.NewDht[key.Key256, ma.Multiaddr](nid, &Router{host: h}, d.rt, nil)
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
	if d.cfg.ProtocolID == ProtocolIPFS && d.cfg.Datastore == nil {
		if b, found := d.backends[namespaceProviders]; found {
			if pbe, ok := b.(*ProvidersBackend); ok {
				if err := pbe.datastore.Close(); err != nil {
					d.log.Warn("failed closing in memory datastore", "err", err.Error())
				}
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

// newSHA256Key SHA256 hashes the given bytes and returns a new 256-bit key.
func newSHA256Key(data []byte) key.Key256 {
	h := sha256.Sum256(data)
	return key.NewKey256(h[:])
}
