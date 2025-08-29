package datastore

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	query "github.com/ipfs/go-datastore/query"
	"github.com/libp2p/go-libp2p-kad-dht/provider/internal/keyspace"
	mh "github.com/multiformats/go-multihash"
	"github.com/probe-lab/go-libdht/kad/key"
	"github.com/probe-lab/go-libdht/kad/key/bit256"
	"github.com/probe-lab/go-libdht/kad/key/bitstr"
)

type KeyStore3 struct {
	done      chan struct{}
	closeOnce sync.Once

	wg sync.WaitGroup
	lk sync.Mutex

	ds   ds.Batching
	base ds.Key

	// TODO: remove everything GC
	gcFunc      KeyChanFunc // optional function to get keys for garbage collection
	gcFuncLk    sync.Mutex
	gcInterval  time.Duration
	gcBatchSize int // TODO: rename resetBatchSize
}

// TODO: probably remove
func (s *KeyStore3) SetGCFunc(gcFunc KeyChanFunc) {
	if gcFunc == nil {
		return
	}
	s.gcFuncLk.Lock()
	defer s.gcFuncLk.Unlock()

	startGC := s.gcFunc == nil && s.gcInterval > 0
	s.gcFunc = gcFunc

	if startGC {
		s.wg.Add(1)
		go s.runGC()
	}
}

// NewKeyStore3 creates a new KeyStore backed by the provided datastore.
func NewKeyStore3(d ds.Batching, opts ...KeyStoreOption) (*KeyStore3, error) {
	var cfg keyStoreCfg
	opts = append([]KeyStoreOption{KeyStoreDefaultCfg}, opts...)
	for i, o := range opts {
		if err := o(&cfg); err != nil {
			return nil, fmt.Errorf("KeyStore option %d failed: %w", i, err)
		}
	}
	keyStore := KeyStore3{
		done: make(chan struct{}),
		ds:   d,
		base: ds.NewKey(cfg.base),

		gcFunc:      cfg.gcFunc,
		gcInterval:  cfg.gcInterval,
		gcBatchSize: cfg.gcBatchSize,
	}
	if cfg.gcFunc != nil && cfg.gcInterval > 0 {
		keyStore.wg.Add(1)
		go keyStore.runGC()
	}
	return &keyStore, nil
}

func (s *KeyStore3) Close() error {
	s.closeOnce.Do(func() { close(s.done) })
	s.wg.Wait()
	return nil
}

func (s *KeyStore3) closed() bool {
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
//
// TODO: probably remove
func (s *KeyStore3) runGC() {
	defer s.wg.Done()
	ticker := time.NewTicker(s.gcInterval)
	for {
		select {
		case <-s.done:
			return
		case <-ticker.C:
			s.gcFuncLk.Lock()
			keysChan, err := s.gcFunc(context.Background())
			s.gcFuncLk.Unlock()
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
func (s *KeyStore3) ResetCids(ctx context.Context, keysChan <-chan cid.Cid) error {
	if s.closed() {
		return ErrKeyStoreClosed
	}
	err := s.Empty(ctx)
	if err != nil {
		return fmt.Errorf("KeyStore empty failed during reset: %w", err)
	}
	keys := make([]mh.Multihash, 0, s.gcBatchSize)
	for c := range keysChan {
		keys = append(keys, c.Hash())
		if len(keys) == s.gcBatchSize {
			_, err = s.Put(ctx, keys...)
			if err != nil {
				return fmt.Errorf("KeyStore put failed during reset: %w", err)
			}
			keys = keys[:0]
		}
	}
	_, err = s.Put(ctx, keys...)
	if err != nil {
		return fmt.Errorf("KeyStore put failed during reset: %w", err)
	}
	return nil
}

// dsKey turns a full key (256 bit) into the corresponding datastore key under
// supplied base key.
//
// Example:
// * Input: k="11110000...", base="/provider/keystore"
// * Output: "/provider/keystore/f/0/..."
func (s *KeyStore3) fullDsKey(k bit256.Key) ds.Key {
	b := strings.Builder{}
	for _, r := range key.HexString(k) {
		b.WriteRune('/')
		b.WriteRune(r)
	}
	return s.base.ChildString(b.String())
}

func (s *KeyStore3) prefixDsKey(k bitstr.Key) ds.Key {
	b := strings.Builder{}
	for _, r := range key.HexString(k[:len(k)-len(k)%4]) {
		b.WriteRune('/')
		b.WriteRune(r)
	}
	return s.base.ChildString(b.String())
}

var hexToBits = map[rune]bitstr.Key{
	'0': "0000", '1': "0001", '2': "0010", '3': "0011",
	'4': "0100", '5': "0101", '6': "0110", '7': "0111",
	'8': "1000", '9': "1001", 'a': "1010", 'b': "1011",
	'c': "1100", 'd': "1101", 'e': "1110", 'f': "1111",
}

type pair struct {
	k ds.Key
	h mh.Multihash
}

// putLocked stores the provided keys while assuming s.lk is already held, and
// returns the keys that weren't present already in the keystore.
func (s *KeyStore3) putLocked(ctx context.Context, keys ...mh.Multihash) ([]mh.Multihash, error) {
	seen := make(map[bit256.Key]struct{}, len(keys))
	toPut := make([]pair, 0, len(keys))

	for _, h := range keys {
		k := keyspace.MhToBit256(h)
		if _, ok := seen[k]; ok {
			continue
		}
		seen[k] = struct{}{}
		dsKey := s.fullDsKey(k)
		ok, err := s.ds.Has(ctx, dsKey)
		if err != nil {
			return nil, err
		}
		if !ok {
			toPut = append(toPut, pair{k: dsKey, h: h})
		}
	}
	clear(seen)
	if len(toPut) == 0 {
		// Nothing to do
		return nil, nil
	}

	b, err := s.ds.Batch(ctx)
	if err != nil {
		return nil, err
	}
	for _, p := range toPut {
		if err := b.Put(ctx, p.k, p.h); err != nil {
			return nil, err
		}
	}
	if err := b.Commit(ctx); err != nil {
		return nil, err
	}

	newKeys := make([]mh.Multihash, len(toPut))
	for i, p := range toPut {
		newKeys[i] = p.h
	}
	return newKeys, nil
}

// Put stores the provided keys in the underlying datastore, grouping them by
// the first prefixLen bits. It returns only the keys that were not previously
// persisted in the datastore (i.e., newly added keys).
func (s *KeyStore3) Put(ctx context.Context, keys ...mh.Multihash) ([]mh.Multihash, error) {
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
func (s *KeyStore3) Get(ctx context.Context, prefix bitstr.Key) ([]mh.Multihash, error) {
	if s.closed() {
		return nil, ErrKeyStoreClosed
	}
	s.lk.Lock()
	defer s.lk.Unlock()

	dsKey := s.prefixDsKey(prefix).String()
	res, err := s.ds.Query(ctx, query.Query{Prefix: dsKey})
	if err != nil {
		return nil, err
	}
	mod := len(prefix) % 4
	out := make([]mh.Multihash, 0)
	for r := range res.Next() {
		if r.Error != nil {
			return nil, r.Error
		}
		// Depending on prefix length, filter out non matching keys
		if mod != 0 {
			c := rune(r.Key[len(dsKey)+1]) // skip the '/'
			b := hexToBits[c][:mod]
			target := prefix[len(prefix)-mod:]
			if b != target {
				// Last character from the result doesn't match the latest prefix bits
				continue
			}
		}
		out = append(out, mh.Multihash(r.Value))
	}

	return out, nil
}

// ContainsPrefix reports whether the KeyStore currently holds at least one
// multihash whose kademlia identifier (bit256.Key) starts with the provided
// bit-prefix.
func (s *KeyStore3) ContainsPrefix(ctx context.Context, prefix bitstr.Key) (bool, error) {
	if s.closed() {
		return false, ErrKeyStoreClosed
	}
	s.lk.Lock()
	defer s.lk.Unlock()

	dsKey := s.prefixDsKey(prefix).String()
	mod := len(prefix) % 4
	q := query.Query{Prefix: dsKey}
	if mod == 0 {
		// Exact match on hex character, only one possible match
		q.Limit = 1
	}
	res, err := s.ds.Query(ctx, q)
	if err != nil {
		return false, err
	}
	for r := range res.Next() {
		if r.Error != nil {
			return false, r.Error
		}
		if mod == 0 {
			return true, nil
		}
		c := rune(r.Key[len(dsKey)+1]) // skip the '/'
		b := hexToBits[c][:mod]
		target := prefix[len(prefix)-mod:]
		if b == target {
			// Last character from the result matches the latest prefix bits
			return true, nil
		}
	}
	return false, nil
}

// emptyLocked deletes all entries under the datastore prefix, assuming s.lk is
// already held.
func (s *KeyStore3) emptyLocked(ctx context.Context) error {
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
func (s *KeyStore3) Empty(ctx context.Context) error {
	if s.closed() {
		return ErrKeyStoreClosed
	}
	s.lk.Lock()
	defer s.lk.Unlock()

	return s.emptyLocked(ctx)
}

// Delete removes the given keys from datastore.
func (s *KeyStore3) Delete(ctx context.Context, keys ...mh.Multihash) error {
	if len(keys) == 0 {
		return nil
	}
	s.lk.Lock()
	defer s.lk.Unlock()

	b, err := s.ds.Batch(ctx)
	if err != nil {
		return err
	}
	for _, h := range keys {
		dsKey := s.fullDsKey(keyspace.MhToBit256(h))
		err := b.Delete(ctx, dsKey)
		if err != nil {
			return err
		}
	}
	return b.Commit(ctx)
}
