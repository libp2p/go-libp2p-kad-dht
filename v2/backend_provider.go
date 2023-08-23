package dht

import (
	"context"
	"encoding/binary"
	"fmt"
	"path"
	"strings"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/autobatch"
	dsq "github.com/ipfs/go-datastore/query"
	record "github.com/libp2p/go-libp2p-record"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/multiformats/go-base32"
	"golang.org/x/exp/slog"
)

// ProvidersBackend implements the [Backend] interface and handles provider
// record requests for the "/providers/" namespace.
type ProvidersBackend struct {
	namespace string                          // the namespace string, usually set to namespaceProviders ("providers")
	cfg       *ProviderBackendConfig          // default is given by DefaultProviderBackendConfig
	log       *slog.Logger                    // convenience accessor of cfg.Logger
	cache     *lru.Cache[string, providerSet] // LRU cache for frequently requested records. TODO: is that really so effective? The cache size is quite low either.
	peerstore peerstore.Peerstore             // reference to the peer store to store and fetch peer multiaddresses from (we don't save them in the datastore)
	datastore *autobatch.Datastore            // the datastore where we save the peer IDs providing a certain multihash
	gcSkip    sync.Map                        // a sync map that marks records as to-be-skipped by the garbage collection process
}

var _ Backend = (*ProvidersBackend)(nil)

// ProviderBackendConfig is used to construct a [ProvidersBackend]. Use
// [DefaultProviderBackendConfig] to get a default configuration struct and then
// modify it to your liking.
type ProviderBackendConfig struct {
	ProvideValidity time.Duration // specifies for how long provider records are valid
	AddressTTL      time.Duration // specifies for how long we will keep around provider multi addresses in the peerstore. If such multiaddresses are present we send them alongside the peer ID to the requesting peer. This prevents the necessity for a second look for the multiaddresses on the requesting peers' side.
	BatchSize       int           // specifies how many provider record writes should be batched
	CacheSize       int           // specifies the LRU cache size
	Logger          *slog.Logger  // the logger to use
}

// DefaultProviderBackendConfig returns a default [ProvidersBackend]
// configuration. Use this as a starting point and modify it. If a nil
// configuration is passed to [NewBackendProvider], this default configuration
// here is used.
func DefaultProviderBackendConfig() *ProviderBackendConfig {
	return &ProviderBackendConfig{
		ProvideValidity: time.Hour * 48, // empirically measured in: https://github.com/plprobelab/network-measurements/blob/master/results/rfm17-provider-record-liveness.md
		AddressTTL:      24 * time.Hour,
		BatchSize:       256, // MAGIC
		CacheSize:       256, // MAGIC
		Logger:          slog.Default(),
	}
}

// Store implements the [Backend] interface. In the case of a [ProvidersBackend]
// this method accepts a [peer.AddrInfo] as a value and stores it in the
// configured datastore.
func (p *ProvidersBackend) Store(ctx context.Context, key string, value any) (any, error) {
	addrInfo, ok := value.(peer.AddrInfo)
	if !ok {
		return nil, fmt.Errorf("expected peer.AddrInfo value type, got: %T", value)
	}

	ns, suffix, err := record.SplitKey(key) // get namespace (prefix of the key)
	if err != nil {
		return nil, fmt.Errorf("invalid key %s: %w", key, err)
	}

	if p.namespace != ns {
		return nil, fmt.Errorf("expected namespace %s, got %s", p.namespace, ns)
	}

	rec := expiryRecord{
		expiry: time.Now(),
	}

	if provs, ok := p.cache.Get(key); ok {
		provs.setVal(addrInfo, rec.expiry)
	}

	p.peerstore.AddAddrs(addrInfo.ID, addrInfo.Addrs, p.cfg.AddressTTL)

	dsKey := newDatastoreKey(ns, suffix, string(addrInfo.ID))

	_, found := p.gcSkip.LoadOrStore(dsKey.String(), struct{}{})

	if err := p.datastore.Put(ctx, dsKey, rec.MarshalBinary()); err != nil {
		p.cache.Remove(key)

		// if we have just added the key to the gc skip list, delete it again
		// if we have added it in a previous Store invocation, keep it around
		if !found {
			p.gcSkip.Delete(dsKey.String())
		}

		return nil, fmt.Errorf("datastore put: %w", err)
	}

	return addrInfo, nil
}

// Fetch implements the [Backend] interface. In the case of a [ProvidersBackend]
// this method returns a [providerSet] (unexported) that contains all peer IDs
// and known multiaddresses for the given key. The key parameter should be of
// the form "/providers/$binary_multihash".
func (p *ProvidersBackend) Fetch(ctx context.Context, key string) (any, error) {
	ns, suffix, err := record.SplitKey(key) // get namespace (prefix of the key)
	if err != nil {
		return nil, fmt.Errorf("invalid key %s: %w", key, err)
	}

	if p.namespace != ns {
		return nil, fmt.Errorf("expected namespace %s, got %s", p.namespace, ns)
	}

	if cached, ok := p.cache.Get(key); ok {
		return cached, nil
	}

	qKey := newDatastoreKey(ns, suffix)
	q, err := p.datastore.Query(ctx, dsq.Query{Prefix: qKey.String()})
	if err != nil {
		return nil, err
	}

	defer func() {
		if err = q.Close(); err != nil {
			p.log.LogAttrs(ctx, slog.LevelWarn, "failed closing fetch query", slog.String("err", err.Error()))
		}
	}()

	now := time.Now()
	out := newProviderSet()

	for e := range q.Next() {
		if e.Error != nil {
			p.log.LogAttrs(ctx, slog.LevelWarn, "Fetch datastore entry contains error", slog.String("key", e.Key), slog.String("err", e.Error.Error()))
			continue
		}

		rec := expiryRecord{}
		if err = rec.UnmarshalBinary(e.Value); err != nil {
			p.log.LogAttrs(ctx, slog.LevelWarn, "Fetch provider record unmarshalling failed", slog.String("key", e.Key), slog.String("err", err.Error()))
			p.delete(ctx, ds.RawKey(e.Key))
			continue
		} else if now.Sub(rec.expiry) > p.cfg.ProvideValidity {
			// record is expired
			p.delete(ctx, ds.RawKey(e.Key))
			continue
		}

		idx := strings.LastIndex(e.Key, "/")
		binPeerID, err := base32.RawStdEncoding.DecodeString(e.Key[idx+1:])
		if err != nil {
			p.log.LogAttrs(ctx, slog.LevelWarn, "base32 key decoding error", slog.String("key", e.Key), slog.String("err", err.Error()))
			p.delete(ctx, ds.RawKey(e.Key))
			continue
		}

		addrInfo := p.peerstore.PeerInfo(peer.ID(binPeerID))

		out.setVal(addrInfo, rec.expiry)
	}

	if len(out.providers) > 0 {
		p.cache.Add(key, *out)
	}

	return out, nil
}

// CollectGarbage sweeps through the datastore and deletes all provider records
// that have expired. A record is expired if the
// [ProviderBackendConfig].ProvideValidity is exceeded.
func (p *ProvidersBackend) CollectGarbage(ctx context.Context) {
	// Faster to purge than garbage collecting
	p.cache.Purge()

	p.gcSkip = sync.Map{} // TODO: racy

	// Now, kick off a GC of the datastore.
	q, err := p.datastore.Query(ctx, dsq.Query{Prefix: p.namespace})
	if err != nil {
		p.log.LogAttrs(ctx, slog.LevelWarn, "provider record garbage collection query failed", slog.String("err", err.Error()))
		return
	}

	defer func() {
		if err = q.Close(); err != nil {
			p.log.LogAttrs(ctx, slog.LevelWarn, "failed closing garbage collection query", slog.String("err", err.Error()))
		}
	}()

	for e := range q.Next() {
		if e.Error != nil {
			p.log.LogAttrs(ctx, slog.LevelWarn, "Garbage collection datastore entry contains error", slog.String("key", e.Key), slog.String("err", e.Error.Error()))
			continue
		}

		if _, found := p.gcSkip.Load(e.Key); found {
			continue
		}

		rec := expiryRecord{}
		if err = rec.UnmarshalBinary(e.Value); err != nil {
			p.log.LogAttrs(ctx, slog.LevelWarn, "Garbage collection provider record unmarshalling failed", slog.String("key", e.Key), slog.String("err", err.Error()))
			p.delete(ctx, ds.RawKey(e.Key))
		} else if time.Now().Sub(rec.expiry) <= p.cfg.ProvideValidity {
			continue
		}

		// record expired -> garbage collect
		p.delete(ctx, ds.RawKey(e.Key))
	}
}

// delete is a convenience method to delete the record at the given datastore
// key. It doesn't return any error but logs it instead as a warning.
func (p *ProvidersBackend) delete(ctx context.Context, dsKey ds.Key) {
	if err := p.datastore.Delete(ctx, dsKey); err != nil {
		p.log.LogAttrs(ctx, slog.LevelWarn, "failed to remove provider record from disk", slog.String("key", dsKey.String()), slog.String("err", err.Error()))
	}
}

// expiryRecord is captures the information that gets written to the datastore
// for any provider record. This record doesn't include any peer IDs or
// multiaddresses because peer IDs are part of the key that this record gets
// stored under and multiaddresses are stored in the peerstore. This record
// just tracks the expiry time of the record. It implements binary marshalling
// and unmarshalling methods for easy (de)serialization into the datastore.
type expiryRecord struct {
	expiry time.Time
}

// MarshalBinary returns the byte slice that should be stored in the datastore.
// This method doesn't comply to the [encoding.BinaryMarshaler] interface
// because it doesn't return an error. We don't need the conformance here
// though.
func (e *expiryRecord) MarshalBinary() (data []byte) {
	buf := make([]byte, 16)
	n := binary.PutVarint(buf, e.expiry.UnixNano())
	return buf[:n]
}

// UnmarshalBinary is the inverse operation to the above MarshalBinary and is
// used to deserialize any blob of bytes that was previously stored in the
// datastore.
func (e *expiryRecord) UnmarshalBinary(data []byte) error {
	nsec, n := binary.Varint(data)
	if n == 0 {
		return fmt.Errorf("failed to parse time")
	}

	e.expiry = time.Unix(0, nsec)

	return nil
}

type providerSet struct {
	providers []peer.AddrInfo
	set       map[peer.ID]time.Time
}

func newProviderSet() *providerSet {
	return &providerSet{
		providers: []peer.AddrInfo{},
		set:       make(map[peer.ID]time.Time),
	}
}

func (ps *providerSet) setVal(addrInfo peer.AddrInfo, t time.Time) {
	_, found := ps.set[addrInfo.ID]
	if !found {
		ps.providers = append(ps.providers, addrInfo)
	}

	ps.set[addrInfo.ID] = t
}

func newDatastoreKey(namespace string, binStrs ...string) ds.Key {
	elems := make([]string, len(binStrs)+1)
	elems[0] = namespace
	for i, bin := range binStrs {
		elems[i+1] = base32.RawStdEncoding.EncodeToString([]byte(bin))
	}
	return ds.NewKey("/" + path.Join(elems...))
}
