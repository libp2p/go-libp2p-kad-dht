package dht

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"path"
	"strings"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/ipfs/boxo/ipns"
	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/autobatch"
	dsq "github.com/ipfs/go-datastore/query"
	record "github.com/libp2p/go-libp2p-record"
	recpb "github.com/libp2p/go-libp2p-record/pb"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/multiformats/go-base32"
	"golang.org/x/exp/slog"
)

// Default namespaces
const (
	namespaceIPNS      = "ipns"
	namespacePublicKey = "pk"
	namespaceProviders = "providers"
)

type Backend interface {
	Store(ctx context.Context, key string, value any) (any, error)
	Fetch(ctx context.Context, key string) (any, error)
}

func NewValueBackend(namespace string, datastore ds.TxnDatastore, validator record.Validator) *RecordBackend {
	return &RecordBackend{
		namespace: namespace,
		datastore: datastore,
		validator: validator,
	}
}

func NewIPNSBackend(ds ds.TxnDatastore, kb peerstore.KeyBook, cfg *ValueBackendConfig) *RecordBackend {
	return &RecordBackend{
		cfg:       cfg,
		log:       cfg.Logger,
		namespace: namespaceIPNS,
		datastore: ds,
		validator: ipns.Validator{KeyBook: kb},
	}
}

func NewPublicKeyBackend(ds ds.TxnDatastore, cfg *ValueBackendConfig) *RecordBackend {
	if cfg == nil {
		cfg = DefaultValueBackendConfig()
	}

	return &RecordBackend{
		cfg:       cfg,
		log:       cfg.Logger,
		namespace: namespacePublicKey,
		datastore: ds,
		validator: record.PublicKeyValidator{},
	}
}

type RecordBackend struct {
	cfg       *ValueBackendConfig
	log       *slog.Logger
	namespace string
	datastore ds.TxnDatastore
	validator record.Validator
}

var _ Backend = (*RecordBackend)(nil)

type ValueBackendConfig struct {
	MaxRecordAge time.Duration
	Logger       *slog.Logger
}

func DefaultValueBackendConfig() *ValueBackendConfig {
	return &ValueBackendConfig{
		Logger:       slog.Default(),
		MaxRecordAge: 48 * time.Hour, // empirically measured in: https://github.com/plprobelab/network-measurements/blob/master/results/rfm17-provider-record-liveness.md
	}
}

func (v *RecordBackend) Store(ctx context.Context, key string, value any) (any, error) {
	rec, ok := value.(*recpb.Record)
	if !ok {
		return nil, fmt.Errorf("expected *recpb.Record value type, got: %T", value)
	}

	if key != string(rec.GetKey()) {
		return nil, fmt.Errorf("key doesn't match record key")
	}

	ns, suffix, err := record.SplitKey(key) // get namespace (prefix of the key)
	if err != nil {
		return nil, fmt.Errorf("invalid key %s: %w", key, err)
	}

	if v.namespace != ns {
		return nil, fmt.Errorf("expected namespace %s, got %s", v.namespace, ns)
	}

	dsKey := newDatastoreKey(v.namespace, suffix)

	if err := v.validator.Validate(string(rec.GetKey()), rec.GetValue()); err != nil {
		return nil, fmt.Errorf("put bad record: %w", err)
	}

	txn, err := v.datastore.NewTransaction(ctx, false)
	if err != nil {
		return nil, fmt.Errorf("new transaction: %w", err)
	}
	defer txn.Discard(ctx) // discard is a no-op if txn was committed beforehand

	shouldReplace, err := v.shouldReplaceExistingRecord(ctx, txn, dsKey, rec)
	if err != nil {
		return nil, fmt.Errorf("checking datastore for better record: %w", err)
	} else if !shouldReplace {
		return nil, fmt.Errorf("received worse record")
	}

	// avoid storing arbitrary data, so overwrite that field
	rec.TimeReceived = time.Now().UTC().Format(time.RFC3339Nano)

	data, err := rec.Marshal()
	if err != nil {
		return nil, fmt.Errorf("marshal incoming record: %w", err)
	}

	if err = txn.Put(ctx, dsKey, data); err != nil {
		return nil, fmt.Errorf("storing record in datastore: %w", err)
	}

	if err = txn.Commit(ctx); err != nil {
		return nil, fmt.Errorf("committing new record to datastore: %w", err)
	}

	return rec, nil
}

func (v *RecordBackend) Fetch(ctx context.Context, key string) (any, error) {
	ns, suffix, err := record.SplitKey(key) // get namespace (prefix of the key)
	if err != nil {
		return nil, fmt.Errorf("invalid key %s: %w", key, err)
	}

	if v.namespace != ns {
		return nil, fmt.Errorf("expected namespace %s, got %s", v.namespace, ns)
	}

	dsKey := newDatastoreKey(v.namespace, suffix)

	// fetch record from the datastore for the requested key
	buf, err := v.datastore.Get(ctx, dsKey)
	if err != nil {
		return nil, err
	}

	// we have found a record, parse it and do basic validation
	rec := &recpb.Record{}
	err = rec.Unmarshal(buf)
	if err != nil {
		// we have a corrupt record in the datastore -> delete it and pretend
		// that we don't know about it
		if err := v.datastore.Delete(ctx, dsKey); err != nil {
			v.log.LogAttrs(ctx, slog.LevelWarn, "Failed deleting corrupt record from datastore", slog.String("err", err.Error()))
		}

		return nil, nil
	}

	// validate that we don't serve stale records.
	receivedAt, err := time.Parse(time.RFC3339Nano, rec.GetTimeReceived())
	if err != nil || time.Since(receivedAt) > v.cfg.MaxRecordAge {
		errStr := ""
		if err != nil {
			errStr = err.Error()
		}

		v.log.LogAttrs(ctx, slog.LevelWarn, "Invalid received timestamp on stored record", slog.String("err", errStr), slog.Duration("age", time.Since(receivedAt)))
		if err = v.datastore.Delete(ctx, dsKey); err != nil {
			v.log.LogAttrs(ctx, slog.LevelWarn, "Failed deleting bad record from datastore", slog.String("err", err.Error()))
		}
		return nil, nil
	}

	// We don't do any additional validation beyond checking the above
	// timestamp. We put the burden of validating the record on the requester as
	// checking a record may be computationally expensive.

	return rec, nil
}

// shouldReplaceExistingRecord returns true if the given record should replace any
// existing one in the local datastore. It queries the datastore, unmarshalls
// the record, validates it, and compares it to the incoming record. If the
// incoming one is "better" (e.g., just newer), this function returns true.
// If unmarshalling or validation fails, this function (alongside an error) also
// returns true because the existing record should be replaced.
func (v *RecordBackend) shouldReplaceExistingRecord(ctx context.Context, txn ds.Read, dsKey ds.Key, newRec *recpb.Record) (bool, error) {
	ctx, span := tracer.Start(ctx, "DHT.shouldReplaceExistingRecord")
	defer span.End()

	existingBytes, err := txn.Get(ctx, dsKey)
	if errors.Is(err, ds.ErrNotFound) {
		return true, nil
	} else if err != nil {
		return false, fmt.Errorf("getting record from datastore: %w", err)
	}

	existingRec := &recpb.Record{}
	if err := existingRec.Unmarshal(existingBytes); err != nil {
		return true, nil
	}

	if err := v.validator.Validate(string(existingRec.GetKey()), existingRec.GetValue()); err != nil {
		return true, nil
	}

	records := [][]byte{newRec.GetValue(), existingRec.GetValue()}
	i, err := v.validator.Select(string(newRec.GetKey()), records)
	if err != nil {
		return false, fmt.Errorf("record selection: %w", err)
	} else if i != 0 {
		return false, nil
	}

	return true, nil
}

type ProviderBackend struct {
	namespace string
	cfg       *ProviderBackendConfig
	log       *slog.Logger
	cache     *lru.Cache[string, providerSet]
	peerstore peerstore.Peerstore
	datastore *autobatch.Datastore
	gcSkip    sync.Map
}

type ProviderBackendConfig struct {
	ProvideValidity time.Duration
	AddressTTL      time.Duration
	BatchSize       int
	CacheSize       int
	Logger          *slog.Logger
}

func DefaultProviderBackendConfig() *ProviderBackendConfig {
	return &ProviderBackendConfig{
		ProvideValidity: time.Hour * 48,
		AddressTTL:      24 * time.Hour,
		BatchSize:       256, // MAGIC
		CacheSize:       256, // MAGIC
		Logger:          slog.Default(),
	}
}

func NewProviderBackend(pstore peerstore.Peerstore, dstore ds.Batching, cfg *ProviderBackendConfig) (*ProviderBackend, error) {
	if cfg == nil {
		cfg = DefaultProviderBackendConfig()
	}

	cache, err := lru.New[string, providerSet](cfg.CacheSize)
	if err != nil {
		return nil, err
	}

	p := &ProviderBackend{
		cfg:       cfg,
		log:       cfg.Logger,
		cache:     cache,
		namespace: namespaceProviders,
		peerstore: pstore,
		datastore: autobatch.NewAutoBatching(dstore, cfg.BatchSize),
	}

	return p, nil
}

var _ Backend = (*ProviderBackend)(nil)

func (p *ProviderBackend) Store(ctx context.Context, key string, value any) (any, error) {
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

func (p *ProviderBackend) Fetch(ctx context.Context, key string) (any, error) {
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

func (p *ProviderBackend) CollectGarbage(ctx context.Context) {
	// Faster to purge than garbage collecting
	p.cache.Purge()

	p.gcSkip = sync.Map{}

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

func (p *ProviderBackend) delete(ctx context.Context, dsKey ds.Key) {
	if err := p.datastore.Delete(ctx, dsKey); err != nil {
		p.log.LogAttrs(ctx, slog.LevelWarn, "failed to remove provider record from disk", slog.String("key", dsKey.String()), slog.String("err", err.Error()))
	}
}

type expiryRecord struct {
	expiry time.Time
}

func (e *expiryRecord) MarshalBinary() (data []byte) {
	buf := make([]byte, 16)
	n := binary.PutVarint(buf, e.expiry.UnixNano())
	return buf[:n]
}

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
