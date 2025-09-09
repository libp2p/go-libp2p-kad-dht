package datastore

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	query "github.com/ipfs/go-datastore/query"
	"github.com/libp2p/go-libp2p-kad-dht/provider/internal/keyspace"
	mh "github.com/multiformats/go-multihash"

	"github.com/probe-lab/go-libdht/kad"
	"github.com/probe-lab/go-libdht/kad/key/bit256"
	"github.com/probe-lab/go-libdht/kad/key/bitstr"
)

// KeyStore provides thread-safe storage and retrieval of multihashes, indexed
// by their kademlia 256-bit identifier.
type KeyStore interface {
	Put(context.Context, ...mh.Multihash) ([]mh.Multihash, error)
	Get(context.Context, bitstr.Key) ([]mh.Multihash, error)
	ContainsPrefix(context.Context, bitstr.Key) (bool, error)
	Delete(context.Context, ...mh.Multihash) error
	Empty(context.Context) error
	Size(context.Context) (int, error)
}

// keyStore indexes multihashes by their kademlia identifier.
type keyStore struct {
	lk sync.Mutex

	ds         ds.Batching
	base       ds.Key
	prefixBits int
	batchSize  int
}

type keyStoreCfg struct {
	base       string
	prefixBits int
	batchSize  int
}

// KeyStoreOption configures KeyStore behaviour.
type KeyStoreOption func(*keyStoreCfg) error

const (
	DefaultKeyStoreBasePrefix = "/provider/keystore"
	DefaultKeyStoreBatchSize  = 1 << 14
	DefaultPrefixBits         = 16
)

var KeyStoreDefaultCfg = func(cfg *keyStoreCfg) error {
	cfg.base = DefaultKeyStoreBasePrefix
	cfg.prefixBits = DefaultPrefixBits
	cfg.batchSize = DefaultKeyStoreBatchSize
	return nil
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

// WithPrefixBits sets how many bits from binary keys become individual path
// components in datastore keys. Higher values create deeper hierarchies but
// enable more granular prefix queries.
//
// Must be a multiple of 8 between 0 and 256 (inclusive) to align with byte
// boundaries.
func WithPrefixBits(prefixBits int) KeyStoreOption {
	return func(cfg *keyStoreCfg) error {
		if prefixBits < 0 || prefixBits > 256 || prefixBits%8 != 0 {
			return fmt.Errorf("invalid prefix bits %d, must be a non-negative multiple of 8 less or equal to 256", prefixBits)
		}
		cfg.prefixBits = prefixBits
		return nil
	}
}

// WithBatchSize defines the maximal number of keys per batch when reading or
// writing to the datastore. It is typically used in Empty() and ResetCids().
func WithBatchSize(size int) KeyStoreOption {
	return func(cfg *keyStoreCfg) error {
		if size <= 0 {
			return fmt.Errorf("invalid batch size %d", size)
		}
		cfg.batchSize = size
		return nil
	}
}

// NewKeyStore creates a new KeyStore backed by the provided datastore.
func NewKeyStore(d ds.Batching, opts ...KeyStoreOption) (KeyStore, error) {
	var cfg keyStoreCfg
	opts = append([]KeyStoreOption{KeyStoreDefaultCfg}, opts...)
	for i, o := range opts {
		if err := o(&cfg); err != nil {
			return nil, fmt.Errorf("KeyStore option %d failed: %w", i, err)
		}
	}
	return &keyStore{
		ds:         d,
		base:       ds.NewKey(cfg.base),
		prefixBits: cfg.prefixBits,
		batchSize:  cfg.batchSize,
	}, nil
}

// dsKey returns the datastore key for the provided binary key.
//
// The function creates a hierarchical datastore key by expanding bits into
// path components, starting with the supplied `base` prefix, followed by
// individual bits (`0` or `1`) separated by `/`, and optionally a
// base64URL-encoded suffix.
//
// Full keys (256-bit):
// The first `prefixBits` bits become individual path components, and the
// remaining bytes (after prefixBits/8) are base64URL encoded as the final
// component. Example: "/base/prefix/0/0/0/0/1/1/1/1/AAAA...A=="
//
// Prefix keys (<256-bit):
// If the key is shorter than 256-bits, only the available bits (up to
// `prefixBits`) become path components. No base64URL suffix is added. This
// creates a prefix that can be used in datastore queries to find all matching
// full keys.
//
// If the prefix is longer than `prefixBits`, only the first `prefixBits` bits
// are used, allowing the returned key to serve as a query prefix for the
// datastore.
func dsKey[K kad.Key[K]](k K, prefixBits int, base ds.Key) ds.Key {
	b := strings.Builder{}
	l := k.BitLen()
	for i := range min(prefixBits, l) {
		b.WriteRune(rune('0' + k.Bit(i)))
		b.WriteRune('/')
	}
	if l == keyspace.KeyLen {
		b.WriteString(base64.URLEncoding.EncodeToString(keyspace.KeyToBytes(k)[prefixBits/8:]))
	}
	return base.ChildString(b.String())
}

// decodeKey reconstructs a 256-bit binary key from a hierarchical datastore key string.
//
// This function reverses the process of dsKey, converting a datastore key back into
// its original binary representation by parsing the individual bit components and
// base64URL-encoded suffix.
//
// The input datastore key format is expected to be:
// "base/bit0/bit1/.../bitN/base64url_suffix"
//
// Returns the reconstructed 256-bit key or an error if base64URL decoding fails.
func (s *keyStore) decodeKey(dsk string) (bit256.Key, error) {
	dsk = dsk[len(s.base.String()):] // remove leading prefix
	bs := make([]byte, 32)
	// Extract individual bits from odd positions (skip '/' separators)
	for i := range s.prefixBits {
		if dsk[2*i+1] == '1' {
			bs[i/8] |= byte(1) << (7 - i%8)
		}
	}
	// Decode base64URL suffix and append to remaining bytes
	decoded, err := base64.URLEncoding.DecodeString(dsk[2*(s.prefixBits)+1:])
	if err != nil {
		return bit256.Key{}, err
	}
	copy(bs[s.prefixBits/8:], decoded)
	return bit256.NewKey(bs), nil
}

type pair struct {
	k ds.Key
	h mh.Multihash
}

// putLocked stores the provided keys while assuming s.lk is already held, and
// returns the keys that weren't present already in the keystore.
func (s *keyStore) putLocked(ctx context.Context, keys ...mh.Multihash) ([]mh.Multihash, error) {
	seen := make(map[bit256.Key]struct{}, len(keys))
	toPut := make([]pair, 0, len(keys))

	for _, h := range keys {
		k := keyspace.MhToBit256(h)
		if _, ok := seen[k]; ok {
			continue
		}
		seen[k] = struct{}{}
		dsk := dsKey(k, s.prefixBits, s.base)
		ok, err := s.ds.Has(ctx, dsk)
		if err != nil {
			return nil, err
		}
		if !ok {
			toPut = append(toPut, pair{k: dsk, h: h})
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
func (s *keyStore) Put(ctx context.Context, keys ...mh.Multihash) ([]mh.Multihash, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	s.lk.Lock()
	defer s.lk.Unlock()

	return s.putLocked(ctx, keys...)
}

// Get returns all keys whose bit256 representation matches the provided
// prefix.
func (s *keyStore) Get(ctx context.Context, prefix bitstr.Key) ([]mh.Multihash, error) {
	s.lk.Lock()
	defer s.lk.Unlock()

	dsk := dsKey(prefix, s.prefixBits, s.base).String()
	res, err := s.ds.Query(ctx, query.Query{Prefix: dsk})
	if err != nil {
		return nil, err
	}
	defer res.Close()

	longPrefix := prefix.BitLen() > s.prefixBits
	out := make([]mh.Multihash, 0)
	for r := range res.Next() {
		if r.Error != nil {
			return nil, r.Error
		}
		// Depending on prefix length, filter out non matching keys
		if longPrefix {
			k, err := s.decodeKey(r.Key)
			if err != nil {
				return nil, err
			}
			if !keyspace.IsPrefix(prefix, k) {
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
func (s *keyStore) ContainsPrefix(ctx context.Context, prefix bitstr.Key) (bool, error) {
	s.lk.Lock()
	defer s.lk.Unlock()

	dsk := dsKey(prefix, s.prefixBits, s.base).String()
	longPrefix := prefix.BitLen() > s.prefixBits
	q := query.Query{Prefix: dsk, KeysOnly: true}
	if !longPrefix {
		// Exact match on hex character, only one possible match
		q.Limit = 1
	}
	res, err := s.ds.Query(ctx, q)
	if err != nil {
		return false, err
	}
	defer res.Close()

	for r := range res.Next() {
		if r.Error != nil {
			return false, r.Error
		}
		if !longPrefix {
			return true, nil
		}
		k, err := s.decodeKey(r.Key)
		if err != nil {
			return false, err
		}
		if keyspace.IsPrefix(prefix, k) {
			return true, nil
		}
	}
	return false, nil
}

// emptyLocked deletes all entries under the datastore prefix, assuming s.lk is
// already held.
func emptyLocked(ctx context.Context, d ds.Batching, base ds.Key, batchSize int) error {
	res, err := d.Query(ctx, query.Query{Prefix: base.String(), KeysOnly: true})
	if err != nil {
		return err
	}
	defer res.Close()

	delBatch := func(keys []ds.Key) error {
		if len(keys) == 0 {
			return nil
		}
		b, err := d.Batch(ctx)
		if err != nil {
			return nil
		}
		for _, k := range keys {
			if err := b.Delete(ctx, k); err != nil {
				return err
			}
		}
		return b.Commit(ctx)
	}
	keys := make([]ds.Key, 0, batchSize)
	for r := range res.Next() {
		if r.Error != nil {
			return r.Error
		}
		keys = append(keys, ds.NewKey(r.Key))
		if len(keys) == cap(keys) {
			if err := delBatch(keys); err != nil {
				return err
			}
			keys = keys[:0]
		}
	}
	return delBatch(keys) // delete remaining keys
}

// Empty deletes all entries under the datastore prefix.
func (s *keyStore) Empty(ctx context.Context) error {
	s.lk.Lock()
	defer s.lk.Unlock()

	return emptyLocked(ctx, s.ds, s.base, s.batchSize)
}

// Delete removes the given keys from datastore.
func (s *keyStore) Delete(ctx context.Context, keys ...mh.Multihash) error {
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
		dsk := dsKey(keyspace.MhToBit256(h), s.prefixBits, s.base)
		err := b.Delete(ctx, dsk)
		if err != nil {
			return err
		}
	}
	return b.Commit(ctx)
}

// Size returns the number of keys currently stored in the KeyStore.
//
// The size is obtained by iterating over all keys in the underlying
// datastore, so it may be expensive for large stores.
func (s *keyStore) Size(ctx context.Context) (size int, err error) {
	s.lk.Lock()
	defer s.lk.Unlock()

	q := query.Query{Prefix: s.base.String(), KeysOnly: true}
	res, err := s.ds.Query(ctx, q)
	if err != nil {
		return
	}
	defer res.Close()

	for r := range res.Next() {
		if r.Error != nil {
			err = r.Error
			return
		}
		size++
	}
	return
}

var ErrResetInProgress = errors.New("reset already in progress")

const (
	ready uint32 = iota
	inProgress
	cleanup
)

// ResettableKeyStore is a KeyStore implementation that supports atomic reset
// operations. It maintains two alternate bases in the underlying datastore and
// can swap between them to provide atomic replacement of all stored keys.
//
// The reset operation allows replacing all stored multihashes with a new set
// without interrupting concurrent read/write operations. During a reset, new
// writes are duplicated to both the current and alternate storage bases to
// maintain consistency.
type ResettableKeyStore struct {
	keyStore

	altBase ds.Key
	// resetInProgress tracks the state of the reset operation:
	//   - 0: no reset in progress, ready for new reset
	//   - 1: reset in progress, writing to alternate base
	//   - 2: post reset cleanup
	resetState atomic.Uint32
}

var _ KeyStore = (*ResettableKeyStore)(nil)

// NewResettableKeyStore creates a new ResettableKeyStore backed by the
// provided datastore. It automatically adds "/0" and "/1" suffixes to the
// configured base prefix to create two alternate storage locations for atomic
// reset operations.
func NewResettableKeyStore(d ds.Batching, opts ...KeyStoreOption) (*ResettableKeyStore, error) {
	var cfg keyStoreCfg
	opts = append([]KeyStoreOption{KeyStoreDefaultCfg}, opts...)
	for i, o := range opts {
		if err := o(&cfg); err != nil {
			return nil, fmt.Errorf("KeyStore option %d failed: %w", i, err)
		}
	}
	return &ResettableKeyStore{
		keyStore: keyStore{
			ds:         d,
			base:       ds.NewKey(cfg.base).ChildString("0"),
			prefixBits: cfg.prefixBits,
			batchSize:  cfg.batchSize,
		},
		altBase: ds.NewKey(cfg.base).ChildString("1"),
	}, nil
}

func batchPut(ctx context.Context, d ds.Batching, base ds.Key, prefixBits int, keys []mh.Multihash) error {
	b, err := d.Batch(ctx)
	if err != nil {
		return err
	}
	for _, h := range keys {
		dsk := dsKey(keyspace.MhToBit256(h), prefixBits, base)
		if err := b.Put(ctx, dsk, h); err != nil {
			return err
		}
	}
	return b.Commit(ctx)
}

// ResetCids atomically replaces all stored keys with the CIDs received from
// keysChan. The operation is thread-safe and non-blocking for concurrent reads
// and writes.
//
// During the reset:
//   - New keys from keysChan are written to an alternate storage location
//   - Concurrent Put operations are duplicated to both current and alternate
//     locations
//   - Once all keys are processed, storage locations are atomically swapped
//   - The old storage location is cleaned up
//
// Returns ErrResetInProgress if another reset operation is already running.
// The operation can be cancelled via context, which will clean up partial
// state.
func (s *ResettableKeyStore) ResetCids(ctx context.Context, keysChan <-chan cid.Cid) error {
	if !s.resetState.CompareAndSwap(ready, inProgress) {
		return ErrResetInProgress
	}
	defer s.resetState.Store(ready)

	// Make sure the alternate base is empty
	if err := emptyLocked(ctx, s.ds, s.altBase, s.batchSize); err != nil {
		return fmt.Errorf("ResettableKeyStore empty failed during reset: %w", err)
	}

	// Read all the keys from the channel and put them in batch at the alternate
	// base
	keys := make([]mh.Multihash, 0, s.batchSize)
loop:
	for {
		select {
		case <-ctx.Done():
			// Context cancelled, abort reset and clean up alternate base
			s.resetState.Store(cleanup)
			if err := emptyLocked(ctx, s.ds, s.altBase, s.batchSize); err != nil {
				return fmt.Errorf("ResettableKeyStore empty failed during reset: %w", err)
			}
			return nil
		case c, ok := <-keysChan:
			if !ok {
				break loop
			}
			keys = append(keys, c.Hash())
			if len(keys) == cap(keys) {
				if err := batchPut(ctx, s.ds, s.altBase, s.prefixBits, keys); err != nil {
					return fmt.Errorf("ResettableKeyStore put failed during reset: %w", err)
				}
				keys = keys[:0]
			}
		}
	}
	// Put final batch
	if err := batchPut(ctx, s.ds, s.altBase, s.prefixBits, keys); err != nil {
		return fmt.Errorf("ResettableKeyStore put failed during reset: %w", err)
	}

	// Swap the bases. The base in use is now the one to which we just wrote.
	s.lk.Lock()
	oldBase := s.base
	s.base = s.altBase
	s.resetState.Store(cleanup)
	s.lk.Unlock()
	s.altBase = oldBase

	// Clean up the old base
	if err := emptyLocked(ctx, s.ds, oldBase, s.batchSize); err != nil {
		return fmt.Errorf("ResettableKeyStore empty failed during reset: %w", err)
	}

	return nil
}

// Put stores the provided keys in the underlying datastore, with special
// handling during reset operations. When a reset is in progress, keys are
// written to both the current storage location and the alternate location
// being prepared for the reset.
//
// This ensures that keys written during a reset operation remain available
// after the reset completes. Returns only the keys that were not previously
// persisted.
func (s *ResettableKeyStore) Put(ctx context.Context, keys ...mh.Multihash) ([]mh.Multihash, error) {
	if s.resetState.Load() == inProgress {
		// Reset is in progress, write to alternate base in addition to current
		// base.
		batchPut(ctx, s.ds, s.altBase, s.prefixBits, keys)
	}
	return s.keyStore.Put(ctx, keys...)
}
