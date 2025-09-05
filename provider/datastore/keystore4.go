package datastore

import (
	"context"
	"encoding/base64"
	"fmt"
	"strings"
	"sync"

	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	query "github.com/ipfs/go-datastore/query"
	"github.com/libp2p/go-libp2p-kad-dht/provider/internal/keyspace"
	mh "github.com/multiformats/go-multihash"
	"github.com/probe-lab/go-libdht/kad"
	"github.com/probe-lab/go-libdht/kad/key/bit256"
	"github.com/probe-lab/go-libdht/kad/key/bitstr"
)

type KeyStore4 struct {
	lk sync.Mutex

	ds         ds.Batching
	base       ds.Key
	prefixBits int

	gcBatchSize int // TODO: rename resetBatchSize
}

// NewKeyStore4 creates a new KeyStore backed by the provided datastore.
func NewKeyStore4(d ds.Batching, opts ...KeyStoreOption) (*KeyStore4, error) {
	var cfg keyStoreCfg
	opts = append([]KeyStoreOption{KeyStoreDefaultCfg}, opts...)
	for i, o := range opts {
		if err := o(&cfg); err != nil {
			return nil, fmt.Errorf("KeyStore option %d failed: %w", i, err)
		}
	}
	if cfg.prefixBits%8 != 0 {
		return nil, fmt.Errorf("KeyStore4 requires prefixBits to be a multiple of 8")
	}
	return &KeyStore4{
		ds:         d,
		base:       ds.NewKey(cfg.base),
		prefixBits: cfg.prefixBits,

		gcBatchSize: cfg.gcBatchSize,
	}, nil
}

// ResetCids purges the KeyStore and repopulates it with the provided cids.
func (s *KeyStore4) ResetCids(ctx context.Context, keysChan <-chan cid.Cid) error {
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

func (s *KeyStore4) decodeKey(dsk string) (bit256.Key, error) {
	dsk = dsk[len(s.base.String()):] // remove leading prefix
	bs := make([]byte, 32)
	for i := range s.prefixBits {
		if dsk[2*i+1] == '1' {
			bs[i/8] |= byte(1) << (7 - i%8)
		}
	}
	decoded, err := base64.URLEncoding.DecodeString(dsk[2*(s.prefixBits)+1:])
	if err != nil {
		return bit256.Key{}, err
	}
	copy(bs[s.prefixBits/8:], decoded)
	return bit256.NewKey(bs), nil
}

// putLocked stores the provided keys while assuming s.lk is already held, and
// returns the keys that weren't present already in the keystore.
func (s *KeyStore4) putLocked(ctx context.Context, keys ...mh.Multihash) ([]mh.Multihash, error) {
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
func (s *KeyStore4) Put(ctx context.Context, keys ...mh.Multihash) ([]mh.Multihash, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	s.lk.Lock()
	defer s.lk.Unlock()

	return s.putLocked(ctx, keys...)
}

// Get returns all keys whose bit256 representation matches the provided
// prefix.
func (s *KeyStore4) Get(ctx context.Context, prefix bitstr.Key) ([]mh.Multihash, error) {
	s.lk.Lock()
	defer s.lk.Unlock()

	dsk := dsKey(prefix, s.prefixBits, s.base).String()
	res, err := s.ds.Query(ctx, query.Query{Prefix: dsk})
	if err != nil {
		return nil, err
	}
	longPrefix := prefix.BitLen() > s.prefixBits
	out := make([]mh.Multihash, 0)
	for r := range res.Next() {
		if r.Error != nil {
			return nil, r.Error
		}
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
func (s *KeyStore4) ContainsPrefix(ctx context.Context, prefix bitstr.Key) (bool, error) {
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
func (s *KeyStore4) emptyLocked(ctx context.Context) error {
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
func (s *KeyStore4) Empty(ctx context.Context) error {
	s.lk.Lock()
	defer s.lk.Unlock()

	return s.emptyLocked(ctx)
}

// Delete removes the given keys from datastore.
func (s *KeyStore4) Delete(ctx context.Context, keys ...mh.Multihash) error {
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
