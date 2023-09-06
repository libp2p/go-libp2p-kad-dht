package dht

import (
	"context"
	"fmt"

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

// A Backend implementation handles requests for certain record types from other
// peers. A Backend always belongs to a certain namespace. In this case a
// namespace is equivalent to a type of record that this DHT supports. In the
// case of IPFS, the DHT supports the "ipns", "pk", and "providers" namespaces
// and therefore uses three different backends. Depending on the request's key
// the DHT invokes the corresponding backend Store and Fetch methods. A key
// has the structure "/$namespace/$path". The DHT parses uses the $namespace
// part to decide which Backend to use. The $path part is then passed to the
// Backend's Store and Fetch methods as the "key" parameter. Backends for
// different namespace may or may not operate on the same underlying datastore.
//
// To support additional record types, users would implement this Backend
// interface and register it for a custom namespace with the [DHT] [Config] by
// adding it to the [Config.Backend] map. Any PUT_VALUE/GET_VALUE requests would
// start to support the new record type. The requirement is though that all
// "any" types must be [*recpb.Record] types. The below interface cannot enforce
// that type because provider records are handled slightly differently. For
// example, with provider records, the return values are not assigned to the
// [pb.Message.Record] field but to the [pb.Message.ProviderPeers] field.
//
// This repository defines default Backends for the "ipns", "pk", and
// "providers" namespaces. They can be instantiated with [NewBackendIPNS],
// [NewBackendPublicKey], and [NewBackendProvider] respectively.
type Backend interface {
	// Store stores the given value such that it can be retrieved via Fetch
	// with the same key parameter. It returns the written record. The key
	// that will be handed into the Store won't contain the namespace prefix. For
	// example, if we receive a request for /ipns/$binary_id, key will be set to
	// $binary_id. The backend implementation is free to decide how to store the
	// data in the datastore. However, it makes sense to prefix the record with
	// the namespace that this Backend operates in.
	Store(ctx context.Context, key string, value any) (any, error)

	// Fetch returns the record for the given path or a [ds.ErrNotFound] if it
	// wasn't found or another error if any occurred.
	Fetch(ctx context.Context, key string) (any, error)
}

// NewBackendIPNS initializes a new backend for the "ipns" namespace that can
// store and fetch IPNS records from the given datastore. The stored and
// returned records must be of type [*recpb.Record]. The cfg parameter can be
// nil, in which case the [DefaultRecordBackendConfig] will be used.
func NewBackendIPNS(ds ds.TxnDatastore, kb peerstore.KeyBook, cfg *RecordBackendConfig) (be *RecordBackend, err error) {
	if cfg == nil {
		if cfg, err = DefaultRecordBackendConfig(); err != nil {
			return nil, fmt.Errorf("default ipns backend config: %w", err)
		}
	}

	return &RecordBackend{
		cfg:       cfg,
		log:       cfg.Logger,
		namespace: namespaceIPNS,
		datastore: ds,
		validator: ipns.Validator{KeyBook: kb},
	}, nil
}

// NewBackendPublicKey initializes a new backend for the "pk" namespace that can
// store and fetch public key records from the given datastore. The stored and
// returned records must be of type [*recpb.Record]. The cfg parameter can be
// nil, in which case the [DefaultRecordBackendConfig] will be used.
func NewBackendPublicKey(ds ds.TxnDatastore, cfg *RecordBackendConfig) (be *RecordBackend, err error) {
	if cfg == nil {
		if cfg, err = DefaultRecordBackendConfig(); err != nil {
			return nil, fmt.Errorf("default public key backend config: %w", err)
		}
	}

	return &RecordBackend{
		cfg:       cfg,
		log:       cfg.Logger,
		namespace: namespacePublicKey,
		datastore: ds,
		validator: record.PublicKeyValidator{},
	}, nil
}

// NewBackendProvider initializes a new backend for the "providers" namespace
// that can store and fetch provider records from the given datastore. The
// values passed into [ProvidersBackend.Store] must be of type [peer.AddrInfo].
// The values returned from [ProvidersBackend.Fetch] will be of type
// [*providerSet] (unexported). The cfg parameter can be nil, in which case the
// [DefaultProviderBackendConfig] will be used.
func NewBackendProvider(pstore peerstore.Peerstore, dstore ds.Batching, cfg *ProvidersBackendConfig) (be *ProvidersBackend, err error) {
	if cfg == nil {
		if cfg, err = DefaultProviderBackendConfig(); err != nil {
			return nil, fmt.Errorf("default provider backend config: %w", err)
		}
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
		addrBook:  pstore,
		datastore: autobatch.NewAutoBatching(dstore, cfg.BatchSize),
	}

	return p, nil
}
