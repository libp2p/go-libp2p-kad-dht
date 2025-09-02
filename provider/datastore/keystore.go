package datastore

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	query "github.com/ipfs/go-datastore/query"
	"github.com/libp2p/go-libp2p-kad-dht/provider/internal/keyspace"
	mh "github.com/multiformats/go-multihash"

	"github.com/probe-lab/go-libdht/kad/key"
	"github.com/probe-lab/go-libdht/kad/key/bit256"
	"github.com/probe-lab/go-libdht/kad/key/bitstr"
)

type KeyChanFunc = func(context.Context) (<-chan cid.Cid, error) // TODO: update to get mh.Multihash instead of cid.Cid

// KeyStore indexes multihashes by their kademlia identifier.
type KeyStore struct {
	lk sync.Mutex

	ds        ds.Batching
	base      ds.Key
	batchSize int
}

type keyStoreCfg struct {
	base      string
	batchSize int
}

// KeyStoreOption configures KeyStore behaviour.
type KeyStoreOption func(*keyStoreCfg) error

const (
	DefaultKeyStoreBasePrefix = "/provider/keystore"
	DefaultKeyStoreBatchSize  = 1 << 14
)

var KeyStoreDefaultCfg = func(cfg *keyStoreCfg) error {
	cfg.base = DefaultKeyStoreBasePrefix
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
func NewKeyStore(d ds.Batching, opts ...KeyStoreOption) (*KeyStore, error) {
	var cfg keyStoreCfg
	opts = append([]KeyStoreOption{KeyStoreDefaultCfg}, opts...)
	for i, o := range opts {
		if err := o(&cfg); err != nil {
			return nil, fmt.Errorf("KeyStore option %d failed: %w", i, err)
		}
	}
	return &KeyStore{
		ds:        d,
		base:      ds.NewKey(cfg.base),
		batchSize: cfg.batchSize,
	}, nil
}

// ResetCids purges the KeyStore and repopulates it with the provided cids'
// multihashes.
func (s *KeyStore) ResetCids(ctx context.Context, keysChan <-chan cid.Cid) error {
	s.lk.Lock()
	defer s.lk.Unlock()

	err := s.emptyLocked(ctx)
	if err != nil {
		return fmt.Errorf("KeyStore empty failed during reset: %w", err)
	}
	keys := make([]mh.Multihash, 0, s.batchSize)
	for c := range keysChan {
		keys = append(keys, c.Hash())
		if len(keys) == cap(keys) {
			_, err = s.putLocked(ctx, keys...)
			if err != nil {
				return fmt.Errorf("KeyStore put failed during reset: %w", err)
			}
			keys = keys[:0]
		}
	}
	_, err = s.putLocked(ctx, keys...)
	if err != nil {
		return fmt.Errorf("KeyStore put failed during reset: %w", err)
	}
	return nil
}

// fullDsKey turns a full key (256 bit) into the corresponding datastore key
// under supplied base key.
//
// Example:
// * Input: k="11110000...", base="/provider/keystore"
// * Output: "/provider/keystore/f/0/..."
func (s *KeyStore) fullDsKey(k bit256.Key) ds.Key {
	b := strings.Builder{}
	for _, r := range key.HexString(k) {
		b.WriteRune('/')
		b.WriteRune(r)
	}
	return s.base.ChildString(b.String())
}

// fullDsKey turns a bitstring key prefix into the corresponding datastore key
// under supplied base key.
func (s *KeyStore) prefixDsKey(k bitstr.Key) ds.Key {
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
func (s *KeyStore) putLocked(ctx context.Context, keys ...mh.Multihash) ([]mh.Multihash, error) {
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
func (s *KeyStore) Put(ctx context.Context, keys ...mh.Multihash) ([]mh.Multihash, error) {
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
	s.lk.Lock()
	defer s.lk.Unlock()

	dsKey := s.prefixDsKey(prefix).String()
	res, err := s.ds.Query(ctx, query.Query{Prefix: dsKey})
	if err != nil {
		return nil, err
	}
	defer res.Close()

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
func (s *KeyStore) ContainsPrefix(ctx context.Context, prefix bitstr.Key) (bool, error) {
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
	defer res.Close()

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
func (s *KeyStore) emptyLocked(ctx context.Context) error {
	res, err := s.ds.Query(ctx, query.Query{Prefix: s.base.String(), KeysOnly: true})
	if err != nil {
		return err
	}
	defer res.Close()

	delBatch := func(keys []ds.Key) error {
		if len(keys) == 0 {
			return nil
		}
		b, err := s.ds.Batch(ctx)
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
	keys := make([]ds.Key, 0, s.batchSize)
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
func (s *KeyStore) Empty(ctx context.Context) error {
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

// Size returns the number of keys currently stored in the KeyStore.
//
// The size is obtained by iterating over all keys in the underlying
// datastore, so it may be expensive for large stores.
func (s *KeyStore) Size(ctx context.Context) (size int, err error) {
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
