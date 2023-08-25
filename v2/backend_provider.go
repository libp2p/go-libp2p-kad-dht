package dht

import (
	"context"
	"encoding/binary"
	"fmt"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/benbjohnson/clock"
	lru "github.com/hashicorp/golang-lru/v2"
	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/autobatch"
	dsq "github.com/ipfs/go-datastore/query"
	"github.com/libp2p/go-libp2p-kad-dht/v2/metrics"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/multiformats/go-base32"
	"go.opencensus.io/stats"
	"go.opencensus.io/tag"
	"golang.org/x/exp/slog"
)

// ProvidersBackend implements the [Backend] interface and handles provider
// record requests for the "/providers/" namespace.
type ProvidersBackend struct {
	// namespace holds the namespace string - usually
	// this is set to namespaceProviders ("providers")
	namespace string

	// cfg is set to DefaultProviderBackendConfig by default
	cfg *ProvidersBackendConfig

	// log is convenience accessor of cfg.Logger
	log *slog.Logger

	// cache is a LRU cache for frequently requested records. It is populated
	// when peers request a record and pruned during garbage collection.
	// TODO: is that really so effective? The cache size is quite low either.
	cache *lru.Cache[string, providerSet]

	// addrBook holds a reference to the peerstore's address book to store and
	// fetch peer multiaddresses from (we don't save them in the datastore).
	addrBook peerstore.AddrBook

	// datastore is where we save the peer IDs providing a certain multihash
	datastore *autobatch.Datastore

	// gcSkip is a sync map that marks records as to-be-skipped by the garbage
	// collection process. TODO: this is a sub-optimal pattern.
	gcSkip sync.Map

	// gcActive indicates whether the garbage collection loop is running
	gcCancelMu sync.RWMutex
	gcCancel   context.CancelFunc
	gcDone     chan struct{}
}

var _ Backend = (*ProvidersBackend)(nil)

// ProvidersBackendConfig is used to construct a [ProvidersBackend]. Use
// [DefaultProviderBackendConfig] to get a default configuration struct and then
// modify it to your liking.
type ProvidersBackendConfig struct {
	// clk is an unexported field that's used for testing time related methods
	clk clock.Clock

	// ProvideValidity specifies for how long provider records are valid
	ProvideValidity time.Duration

	// AddressTTL specifies for how long we will keep around provider multi
	// addresses in the peerstore's address book. If such multiaddresses are
	// present we send them alongside the peer ID to the requesting peer. This
	// prevents the necessity for a second look for the multiaddresses on the
	// requesting peers' side.
	AddressTTL time.Duration

	// BatchSize specifies how many provider record writes should be batched
	BatchSize int

	// CacheSize specifies the LRU cache size
	CacheSize int

	// GCInterval defines how frequently garbage collection should run
	GCInterval time.Duration

	// Logger is the logger to use
	Logger *slog.Logger

	// AddressFilter is a filter function that any addresses that we attempt to
	// store or fetch from the peerstore's address book need to pass through.
	// If you're manually configuring this backend, make sure to align the
	// filter with the one configured in [Config.AddressFilter].
	AddressFilter AddressFilter
}

// DefaultProviderBackendConfig returns a default [ProvidersBackend]
// configuration. Use this as a starting point and modify it. If a nil
// configuration is passed to [NewBackendProvider], this default configuration
// here is used.
func DefaultProviderBackendConfig() *ProvidersBackendConfig {
	return &ProvidersBackendConfig{
		clk:             clock.New(),
		ProvideValidity: 48 * time.Hour, // empirically measured in: https://github.com/plprobelab/network-measurements/blob/master/results/rfm17-provider-record-liveness.md
		AddressTTL:      24 * time.Hour, // MAGIC
		BatchSize:       256,            // MAGIC
		CacheSize:       256,            // MAGIC
		GCInterval:      time.Hour,      // MAGIC
		Logger:          slog.Default(),
		AddressFilter:   AddrFilterIdentity, // verify alignment with [Config.AddressFilter]
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

	rec := expiryRecord{
		expiry: p.cfg.clk.Now(),
	}

	cacheKey := newDatastoreKey(p.namespace, key).String()
	dsKey := newDatastoreKey(p.namespace, key, string(addrInfo.ID))
	if provs, ok := p.cache.Get(cacheKey); ok {
		provs.addProvider(addrInfo, rec.expiry)
	}

	filtered := p.cfg.AddressFilter(addrInfo.Addrs)
	p.addrBook.AddAddrs(addrInfo.ID, filtered, p.cfg.AddressTTL)

	_, found := p.gcSkip.LoadOrStore(dsKey.String(), struct{}{})

	if err := p.datastore.Put(ctx, dsKey, rec.MarshalBinary()); err != nil {
		p.cache.Remove(cacheKey)

		// if we have just added the key to the collectGarbage skip list, delete it again
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
	qKey := newDatastoreKey(p.namespace, key)

	if cached, ok := p.cache.Get(qKey.String()); ok {
		p.trackCacheQuery(ctx, true)
		return cached, nil
	}
	p.trackCacheQuery(ctx, false)

	q, err := p.datastore.Query(ctx, dsq.Query{Prefix: qKey.String()})
	if err != nil {
		return nil, err
	}

	defer func() {
		if err = q.Close(); err != nil {
			p.log.LogAttrs(ctx, slog.LevelWarn, "failed closing fetch query", slog.String("err", err.Error()))
		}
	}()

	now := p.cfg.clk.Now()
	out := &providerSet{
		providers: []peer.AddrInfo{},
		set:       make(map[peer.ID]time.Time),
	}

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

		maddrs := p.addrBook.Addrs(peer.ID(binPeerID))
		addrInfo := peer.AddrInfo{
			ID:    peer.ID(binPeerID),
			Addrs: p.cfg.AddressFilter(maddrs),
		}

		out.addProvider(addrInfo, rec.expiry)
	}

	if len(out.providers) > 0 {
		p.cache.Add(qKey.String(), *out)
	}

	return out, nil
}

// StartGarbageCollection starts the garbage collection loop. The garbage
// collection interval can be configured with [ProvidersBackendConfig.GCInterval].
// The garbage collection loop can only be started a single time. Use
// [StopGarbageCollection] to stop the garbage collection loop.
func (p *ProvidersBackend) StartGarbageCollection() {
	p.gcCancelMu.Lock()
	if p.gcCancel != nil {
		p.log.Info("Provider backend's garbage collection is already running")
		p.gcCancelMu.Unlock()
		return
	}
	defer p.gcCancelMu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	p.gcCancel = cancel
	p.gcDone = make(chan struct{})

	p.log.Info("Provider backend's started garbage collection schedule")

	go func() {
		defer close(p.gcDone)

		ticker := p.cfg.clk.Ticker(p.cfg.GCInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				p.collectGarbage(ctx)
			}
		}
	}()
}

// StopGarbageCollection stops the garbage collection loop started with
// [StartGarbageCollection]. If garbage collection is not running, this method
// is a no-op.
func (p *ProvidersBackend) StopGarbageCollection() {
	p.gcCancelMu.Lock()
	if p.gcCancel == nil {
		p.log.Info("Provider backend's garbage collection isn't running")
		p.gcCancelMu.Unlock()
		return
	}
	defer p.gcCancelMu.Unlock()

	p.gcCancel()
	<-p.gcDone
	p.gcDone = nil
	p.gcCancel = nil
	p.log.Info("Provider backend's garbage collection stopped")
}

// collectGarbage sweeps through the datastore and deletes all provider records
// that have expired. A record is expired if the
// [ProvidersBackendConfig].ProvideValidity is exceeded.
func (p *ProvidersBackend) collectGarbage(ctx context.Context) {
	p.log.Info("Provider backend starting garbage collection...")
	defer p.log.Info("Provider backend finished garbage collection!")

	// Faster to purge than garbage collecting
	p.cache.Purge()

	// erase map
	p.gcSkip.Range(func(key interface{}, value interface{}) bool {
		p.gcSkip.Delete(key)
		return true
	})

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
		now := p.cfg.clk.Now()
		if err = rec.UnmarshalBinary(e.Value); err != nil {
			p.log.LogAttrs(ctx, slog.LevelWarn, "Garbage collection provider record unmarshalling failed", slog.String("key", e.Key), slog.String("err", err.Error()))
			p.delete(ctx, ds.RawKey(e.Key))
		} else if now.Sub(rec.expiry) <= p.cfg.ProvideValidity {
			continue
		}

		// record expired -> garbage collect
		p.delete(ctx, ds.RawKey(e.Key))
	}
}

// trackCacheQuery updates the prometheus metrics about cache hit/miss performance
func (p *ProvidersBackend) trackCacheQuery(ctx context.Context, hit bool) {
	_ = stats.RecordWithTags(ctx,
		[]tag.Mutator{
			tag.Upsert(metrics.KeyCacheHit, strconv.FormatBool(hit)),
			tag.Upsert(metrics.KeyRecordType, "provider"),
		},
		metrics.LRUCache.M(1),
	)
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
// stored under and multiaddresses are stored in the addrBook. This record
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

// A providerSet is used to gather provider information in a single struct. It
// also makes sure that the user doesn't add any duplicate peers.
type providerSet struct {
	providers []peer.AddrInfo
	set       map[peer.ID]time.Time
}

// addProvider adds the given address information to the providerSet. If the
// provider already exists, only the time is updated.
func (ps *providerSet) addProvider(addrInfo peer.AddrInfo, t time.Time) {
	_, found := ps.set[addrInfo.ID]
	if !found {
		ps.providers = append(ps.providers, addrInfo)
	}

	ps.set[addrInfo.ID] = t
}

// newDatastoreKey assembles a datastore for the given namespace and set of
// binary strings. For example, the IPNS record keys have the format:
// "/ipns/$binary_id" (see [Routing Record]). To construct a datastore key this
// function base32-encodes the $binary_id (and any additional path components)
// and joins the parts together separated by forward slashes.
//
// [Routing Record]: https://specs.ipfs.tech/ipns/ipns-record/#routing-record
func newDatastoreKey(namespace string, binStrs ...string) ds.Key {
	elems := make([]string, len(binStrs)+1)
	elems[0] = namespace
	for i, bin := range binStrs {
		elems[i+1] = base32.RawStdEncoding.EncodeToString([]byte(bin))
	}
	return ds.NewKey("/" + path.Join(elems...))
}
