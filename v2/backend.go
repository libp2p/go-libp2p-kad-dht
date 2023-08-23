package dht

import (
	"context"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/ipfs/boxo/ipns"
	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/autobatch"
	record "github.com/libp2p/go-libp2p-record"
	"github.com/libp2p/go-libp2p/core/peerstore"
)

// Default namespaces
const (
	namespaceIPNS      = "ipns"
	namespacePublicKey = "pk"
	namespaceProviders = "providers"
)

// A Backend implementation handles requests from other peers. Depending on the
// keys root namespace, we pass the request to the corresponding backend. For
// example, the root namespace for the key "/ipns/BINARY_ID" is "ipns." If
// we receive a PUT_VALUE request from another peer for the above key, we will
// pass the included record to the "ipns backend." This backend is responsible
// for validating the record and storing or discarding it. The same applies for,
// e.g., "/providers/..." keys which we will receive for ADD_PROVIDER and
// GET_PROVIDERS requests. The [ProvidersBackend] will take care of storing the
// records so that they can be retrieved efficiently via Fetch.
//
// To support additional record types, users would implement this Backend
// interface and register it for a custom namespace with the [DHT] [Config] by
// adding it to the [Config.Backend] map. Any PUT_VALUE/GET_VALUE requests would
// start to support the new record type. The requirement is though that all
// "any" types must be [*recpb.Record] types. The below interface cannot enforce
// that type because provider records are handled slightly differently. For
// example, with provider records, the return values are not assigned to the
// [pb.Message.Record] field but to the [pb.Message.ProviderPeers] field.
type Backend interface {
	// Store stores the given value at the give key (prefixed with the namespace
	// that this backend operates in). It returns the written record. The key
	// that will be handed into the Store won't contain the namespace prefix. For
	// example, if we receive a request for /ipns/BINARY_ID, key will be set to
	// BINARY_ID. The backend implementation is free to decide how to store the
	// data in the datastore. However, it makes sense to prefix the record with
	// the namespace that this Backend operates in. The written record that gets
	// returned from this method could have a different type than the value that
	// was passed into Store, or it could be enriched with additional information
	// like the timestamp when it was written.
	Store(ctx context.Context, key string, value any) (any, error)

	// Fetch returns the record for the given path or a [ds.ErrNotFound] if it
	// wasn't found or another error if any occurred.
	Fetch(ctx context.Context, key string) (any, error)
}

// NewBackendIPNS initializes a new backend for the "ipns" namespace that can
// store and fetch IPNS records from the given datastore. The stored and
// returned records must be of type [*recpb.Record]. The cfg parameter can be
// nil, in which case the [DefaultRecordBackendConfig] will be used.
func NewBackendIPNS(ds ds.TxnDatastore, kb peerstore.KeyBook, cfg *RecordBackendConfig) *RecordBackend {
	if cfg == nil {
		cfg = DefaultRecordBackendConfig()
	}

	return &RecordBackend{
		cfg:       cfg,
		log:       cfg.Logger,
		namespace: namespaceIPNS,
		datastore: ds,
		validator: ipns.Validator{KeyBook: kb},
	}
}

// NewBackendPublicKey initializes a new backend for the "pk" namespace that can
// store and fetch public key records from the given datastore. The stored and
// returned records must be of type [*recpb.Record]. The cfg parameter can be
// nil, in which case the [DefaultRecordBackendConfig] will be used.
func NewBackendPublicKey(ds ds.TxnDatastore, cfg *RecordBackendConfig) *RecordBackend {
	if cfg == nil {
		cfg = DefaultRecordBackendConfig()
	}

	return &RecordBackend{
		cfg:       cfg,
		log:       cfg.Logger,
		namespace: namespacePublicKey,
		datastore: ds,
		validator: record.PublicKeyValidator{},
	}
}

// NewBackendProvider initializes a new backend for the "providers" namespace
// that can store and fetch provider records from the given datastore. The
// values passed into Store must be of type [peer.AddrInfo]. The values returned
// from Fetch will be of type [providerSet] (unexported). The cfg parameter can
// be nil, in which case the [DefaultProviderBackendConfig] will be used.
func NewBackendProvider(pstore peerstore.Peerstore, dstore ds.Batching, cfg *ProviderBackendConfig) (*ProvidersBackend, error) {
	if cfg == nil {
		cfg = DefaultProviderBackendConfig()
	}

	cache, err := lru.New[string, providerSet](cfg.CacheSize)
	if err != nil {
		return nil, err
	}

	p := &ProvidersBackend{
		cfg:       cfg,
		log:       cfg.Logger,
		cache:     cache,
		namespace: namespaceProviders,
		peerstore: pstore,
		datastore: autobatch.NewAutoBatching(dstore, cfg.BatchSize),
	}

	return p, nil
}
