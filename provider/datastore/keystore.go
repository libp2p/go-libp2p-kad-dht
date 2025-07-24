package datastore

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	query "github.com/ipfs/go-datastore/query"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p-kad-dht/amino"
	"github.com/libp2p/go-libp2p-kad-dht/provider/internal/keyspace"
	mh "github.com/multiformats/go-multihash"

	"github.com/probe-lab/go-libdht/kad/key"
	"github.com/probe-lab/go-libdht/kad/key/bitstr"
)

var logger = logging.Logger("dht/SweepingReprovider/KeyStore")

type KeyChanFunc = func(context.Context) (<-chan cid.Cid, error) // TODO: update to get mh.Multihash instead of cid.Cid

// KeyStore maintains a collection of multihash keys, grouping them in a
// datastore by their first `prefixLen` bits.
//
// Optionally, you can supply a garbage-collection callback that returns the
// complete set of keys that should remain in the store. On a configurable
// interval, the KeyStore will:
//  1. Wipe its entire contents.
//  2. Re-populate itself using only the keys provided by the callback.
//
// This automatic refresh keeps the KeyStore in sync with your source of truth
// even if it’s difficult to know exactly when to call Remove() manually.
type KeyStore struct {
	done      chan struct{}
	closeOnce sync.Once

	wg sync.WaitGroup
	lk sync.Mutex

	ds        ds.Batching
	base      ds.Key
	prefixLen int

	gcFunc      KeyChanFunc // optional function to get keys for garbage collection
	gcInterval  time.Duration
	gcBatchSize int
}

var ErrKeyStoreClosed = fmt.Errorf("keystore is closed")

type keyStoreCfg struct {
	base       string
	prefixBits int

	gcFunc      KeyChanFunc
	gcInterval  time.Duration
	gcBatchSize int
}

// KeyStoreOption configures KeyStore behaviour.
type KeyStoreOption func(*keyStoreCfg) error

const (
	DefaultKeyStorePrefixBits  = 10
	DefaultKeyStoreBasePrefix  = "/reprovider/mhs"
	DefaultKeyStoreGCInterval  = 2 * amino.DefaultReprovideInterval
	DefaultKeyStoreGCBatchSize = 1 << 14
)

var KeyStoreDefaultCfg = func(cfg *keyStoreCfg) error {
	cfg.prefixBits = DefaultKeyStorePrefixBits
	cfg.base = DefaultKeyStoreBasePrefix
	cfg.gcInterval = DefaultKeyStoreGCInterval
	cfg.gcBatchSize = DefaultKeyStoreGCBatchSize
	return nil
}

// WithPrefixBits sets the bit-length used to group multihashes when persisting
// them. The value must be positive and at most 256 bits.
func WithPrefixBits(n int) KeyStoreOption {
	return func(cfg *keyStoreCfg) error {
		if n <= 0 || n > 256 {
			return fmt.Errorf("invalid prefix length %d", n)
		}
		cfg.prefixBits = n
		return nil
	}
}

// WithDatastorePrefix sets the datastore prefix under which multihashes are
// stored.
func WithDatastorePrefix(base string) KeyStoreOption {
	return func(cfg *keyStoreCfg) error {
		if base == "" {
			return fmt.Errorf("datastore prefix cannot be empty")
		}
		cfg.base = base
		return nil
	}
}

// WithGCFunc sets the function periodically used to garbage collect keys that
// shouldn't be provided anymore. During the garbage collection process, the
// store is entirely purged, and is repopulated from the keys supplied by the
// KeyChanFunc.
func WithGCFunc(gcFunc KeyChanFunc) KeyStoreOption {
	return func(cfg *keyStoreCfg) error {
		cfg.gcFunc = gcFunc
		return nil
	}
}

// WithGCInterval defines the interval at which the KeyStore is garbage
// collected. During the garbage collection process, the store is entirely
// purged, and is repopulated from the keys supplied by the gcFunc.
func WithGCInterval(interval time.Duration) KeyStoreOption {
	return func(cfg *keyStoreCfg) error {
		if interval <= 0 {
			return fmt.Errorf("invalid garbage collection interval %s", interval)
		}
		cfg.gcInterval = interval
		return nil
	}
}

// WithGCBatchSize defines the number of keys per batch when repopulating the
// KeyStore using the gcFunc. The gcFunc returns a chan cid.Cid, and the
// repopulation process reads batches of gcBatchSize keys before writing them
// to the KeyStore.
func WithGCBatchSize(size int) KeyStoreOption {
	return func(cfg *keyStoreCfg) error {
		if size <= 0 {
			return fmt.Errorf("invalid garbage collection batch size %d", size)
		}
		cfg.gcBatchSize = size
		return nil
	}
}

// NewKeyStore creates a new KeyStore backed by the provided datastore.
func NewKeyStore(d ds.Batching, opts ...KeyStoreOption) (*KeyStore, error) {
	var cfg keyStoreCfg
	opts = append([]KeyStoreOption{KeyStoreDefaultCfg}, opts...)
	for i, o := range opts {
		if err := o(&cfg); err != nil {
			return nil, fmt.Errorf("KeyStore option %d failed: %w", i, err)
		}
	}
	keyStore := KeyStore{
		ds:        d,
		base:      ds.NewKey(cfg.base),
		prefixLen: cfg.prefixBits,

		gcFunc:      cfg.gcFunc,
		gcInterval:  cfg.gcInterval,
		gcBatchSize: cfg.gcBatchSize,
	}
	if cfg.gcFunc != nil && cfg.gcInterval > 0 {
		go keyStore.runGC()
	}
	return &keyStore, nil
}

func (s *KeyStore) Close() error {
	s.closeOnce.Do(func() { close(s.done) })
	s.wg.Wait()
	return nil
}

func (s *KeyStore) closed() bool {
	select {
	case <-s.done:
		return true
	default:
		return false
	}
}

// runGC periodically runs garbage collection.
//
// Garbage collection consists in totally purging the KeyStore, and
// repopulating it with the keys supplied by the GC function. It basically
// resets the state of the KeyStore to match the state returned by the GC
// function.
func (s *KeyStore) runGC() {
	s.wg.Add(1)
	defer s.wg.Done()
	ticker := time.NewTicker(s.gcInterval)
	for {
		select {
		case <-s.done:
			return
		case <-ticker.C:
			keysChan, err := s.gcFunc(context.Background())
			if err != nil {
				logger.Errorf("garbage collection failed: %v", err)
				continue
			}
			err = s.ResetCids(context.Background(), keysChan)
			if err != nil {
				logger.Errorf("reset failed: %v", err)
			}
		}
	}
}

// ResetCids purges the KeyStore and repopulates it with the provided cids.
func (s *KeyStore) ResetCids(ctx context.Context, keysChan <-chan cid.Cid) error {
	if s.closed() {
		return ErrKeyStoreClosed
	}
	err := s.Empty(ctx)
	if err != nil {
		return fmt.Errorf("KeyStore empty failed during reset: %w", err)
	}
	keys := make([]mh.Multihash, s.gcBatchSize)
	i := 0
	for c := range keysChan {
		keys[i] = c.Hash()
		i++
		if i == s.gcBatchSize {
			_, err = s.Put(ctx, keys...)
			if err != nil {
				return fmt.Errorf("KeyStore put failed during reset: %w", err)
			}
			i = 0
			keys = keys[:0]
		}
	}
	_, err = s.Put(ctx, keys...)
	if err != nil {
		return fmt.Errorf("KeyStore put failed during reset: %w", err)
	}
	return nil
}

func (s *KeyStore) dsKey(prefix bitstr.Key) ds.Key {
	return s.base.ChildString(string(prefix))
}

// putLocked stores the provided keys while assuming s.lk is already held.
func (s *KeyStore) putLocked(ctx context.Context, keys ...mh.Multihash) ([]mh.Multihash, error) {
	groups := make(map[bitstr.Key][]mh.Multihash)
	for _, h := range keys {
		k := keyspace.MhToBit256(h)
		bs := bitstr.Key(key.BitString(k)[:s.prefixLen])
		groups[bs] = append(groups[bs], h)
	}

	newKeys := make([]mh.Multihash, 0, len(keys))
	for prefix, hs := range groups {
		dsKey := s.dsKey(prefix)
		var stored []mh.Multihash
		data, err := s.ds.Get(ctx, dsKey)
		if err != nil && err != ds.ErrNotFound {
			return nil, err
		}
		if err == nil {
			if err := json.Unmarshal(data, &stored); err != nil {
				return nil, err
			}
		}

		set := make(map[string]struct{}, len(stored))
		for _, h := range stored {
			set[string(h)] = struct{}{}
		}

		for _, h := range hs {
			if _, ok := set[string(h)]; !ok {
				stored = append(stored, h)
				set[string(h)] = struct{}{}
				newKeys = append(newKeys, h)
			}
		}

		buf, err := json.Marshal(stored)
		if err != nil {
			return nil, err
		}
		if err := s.ds.Put(ctx, dsKey, buf); err != nil {
			return nil, err
		}
	}
	return newKeys, nil
}

// Put stores the provided keys in the underlying datastore, grouping them by
// the first prefixLen bits. It returns only the keys that were not previously
// persisted in the datastore (i.e., newly added keys).
func (s *KeyStore) Put(ctx context.Context, keys ...mh.Multihash) ([]mh.Multihash, error) {
	if s.closed() {
		return nil, ErrKeyStoreClosed
	}
	if len(keys) == 0 {
		return nil, nil
	}
	s.lk.Lock()
	defer s.lk.Unlock()

	return s.putLocked(ctx, keys...)
}

// Get returns all keys whose bit256 representation matches the provided
// prefix.
func (s *KeyStore) Get(ctx context.Context, prefix bitstr.Key) ([]mh.Multihash, error) {
	if s.closed() {
		return nil, ErrKeyStoreClosed
	}
	s.lk.Lock()
	defer s.lk.Unlock()

	result := make([]mh.Multihash, 0)
	uniq := make(map[string]struct{})

	if len(prefix) >= s.prefixLen {
		dsKey := s.dsKey(bitstr.Key(prefix[:s.prefixLen]))
		data, err := s.ds.Get(ctx, dsKey)
		if err != nil {
			if err == ds.ErrNotFound {
				return nil, nil
			}
			return nil, err
		}
		var stored []mh.Multihash
		if err := json.Unmarshal(data, &stored); err != nil {
			return nil, err
		}
		for _, h := range stored {
			bs := bitstr.Key(key.BitString(keyspace.MhToBit256(h)))
			if len(bs) >= len(prefix) && bs[:len(prefix)] == prefix {
				if _, ok := uniq[string(h)]; !ok {
					uniq[string(h)] = struct{}{}
					result = append(result, h)
				}
			}
		}
		return result, nil
	}

	remaining := s.prefixLen - len(prefix)
	limit := 1 << remaining
	for i := range limit {
		suffix := fmt.Sprintf("%0*b", remaining, i)
		dsKey := s.dsKey(prefix + bitstr.Key(suffix))
		data, err := s.ds.Get(ctx, dsKey)
		if err != nil {
			if err == ds.ErrNotFound {
				continue
			}
			return nil, err
		}
		var stored []mh.Multihash
		if err := json.Unmarshal(data, &stored); err != nil {
			return nil, err
		}
		for _, h := range stored {
			if _, ok := uniq[string(h)]; !ok {
				uniq[string(h)] = struct{}{}
				result = append(result, h)
			}
		}
	}
	return result, nil
}

// emptyLocked deletes all entries under the datastore prefix, assuming s.lk is
// already held.
func (s *KeyStore) emptyLocked(ctx context.Context) error {
	res, err := s.ds.Query(ctx, query.Query{Prefix: s.base.String()})
	if err != nil {
		return err
	}
	defer res.Close()

	for r := range res.Next() {
		if r.Error != nil {
			return r.Error
		}
		if err := s.ds.Delete(ctx, ds.NewKey(r.Key)); err != nil {
			return err
		}
	}
	return nil
}

// Empty deletes all entries under the datastore prefix.
func (s *KeyStore) Empty(ctx context.Context) error {
	if s.closed() {
		return ErrKeyStoreClosed
	}
	s.lk.Lock()
	defer s.lk.Unlock()

	return s.emptyLocked(ctx)
}

// Delete removes the given keys from datastore.
func (s *KeyStore) Delete(ctx context.Context, keys ...mh.Multihash) error {
	if len(keys) == 0 {
		return nil
	}
	s.lk.Lock()
	defer s.lk.Unlock()

	groups := make(map[bitstr.Key][]mh.Multihash)
	for _, h := range keys {
		bs := bitstr.Key(key.BitString(keyspace.MhToBit256(h)))
		p := bitstr.Key(bs[:s.prefixLen])
		groups[p] = append(groups[p], h)
	}

	for prefix, toDel := range groups {
		dsKey := s.dsKey(prefix)
		data, err := s.ds.Get(ctx, dsKey)
		if err != nil {
			if err == ds.ErrNotFound {
				continue
			}
			return err
		}

		var stored []mh.Multihash
		if err := json.Unmarshal(data, &stored); err != nil {
			return err
		}

		rmSet := make(map[string]struct{}, len(toDel))
		for _, h := range toDel {
			rmSet[string(h)] = struct{}{}
		}

		remaining := stored[:0]
		changed := false
		for _, h := range stored {
			if _, ok := rmSet[string(h)]; ok {
				changed = true
				continue
			}
			remaining = append(remaining, h)
		}
		if !changed {
			continue
		}
		if len(remaining) == 0 {
			if err := s.ds.Delete(ctx, dsKey); err != nil && err != ds.ErrNotFound {
				return err
			}
			continue
		}
		buf, err := json.Marshal(remaining)
		if err != nil {
			return err
		}
		if err := s.ds.Put(ctx, dsKey, buf); err != nil {
			return err
		}
	}
	return nil
}
