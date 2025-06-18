package reprovider

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	ds "github.com/ipfs/go-datastore"
	query "github.com/ipfs/go-datastore/query"
	mh "github.com/multiformats/go-multihash"

	"github.com/probe-lab/go-libdht/kad/key"
	"github.com/probe-lab/go-libdht/kad/key/bitstr"
)

// MHStore stores multihashes grouped by their first prefixLen bits in a
// datastore.
type MHStore struct {
	ds        ds.Batching
	lk        sync.Mutex
	prefixLen int
	base      ds.Key
}

type mhStoreCfg struct {
	prefixLen int
	base      string
}

// MHStoreOption configures MHStore behaviour.
type MHStoreOption func(*mhStoreCfg) error

const (
	DefaultMHStorePrefixLen  = 10
	DefaultMHStoreBasePrefix = "/reprovider/mhs"
)

var MHStoreDefaultCfg = func(cfg *mhStoreCfg) error {
	cfg.prefixLen = DefaultMHStorePrefixLen
	cfg.base = DefaultMHStoreBasePrefix
	return nil
}

// WithPrefixLen sets the bit-length used to group multihashes when persisting
// them. The value must be positive and at most 256 bits.
func WithPrefixLen(n int) MHStoreOption {
	return func(cfg *mhStoreCfg) error {
		if n <= 0 || n > 256 {
			return fmt.Errorf("invalid prefix length %d", n)
		}
		cfg.prefixLen = n
		return nil
	}
}

// WithDatastorePrefix sets the datastore prefix under which multihashes are
// stored.
func WithDatastorePrefix(base string) MHStoreOption {
	return func(cfg *mhStoreCfg) error {
		if base == "" {
			return fmt.Errorf("datastore prefix cannot be empty")
		}
		cfg.base = base
		return nil
	}
}

// NewMHStore creates a new MHStore backed by the provided datastore.
func NewMHStore(d ds.Batching, opts ...MHStoreOption) (*MHStore, error) {
	var cfg mhStoreCfg
	opts = append([]MHStoreOption{MHStoreDefaultCfg}, opts...)
	for i, o := range opts {
		if err := o(&cfg); err != nil {
			return nil, fmt.Errorf("MHStore option %d failed: %w", i, err)
		}
	}
	return &MHStore{
		ds:        d,
		prefixLen: cfg.prefixLen,
		base:      ds.NewKey(cfg.base),
	}, nil
}

func (s *MHStore) dsKey(prefix bitstr.Key) ds.Key {
	return s.base.ChildString(string(prefix))
}

// putLocked stores the provided multihashes while assuming s.lk is already
// held.
func (s *MHStore) putLocked(ctx context.Context, mhs ...mh.Multihash) ([]mh.Multihash, error) {
	groups := make(map[bitstr.Key][]mh.Multihash)
	newMhs := make([]mh.Multihash, 0, len(mhs))
	for _, h := range mhs {
		k := mhToBit256(h)
		bs := bitstr.Key(key.BitString(k)[:s.prefixLen])
		groups[bs] = append(groups[bs], h)
	}

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
				newMhs = append(newMhs, h)
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
	return newMhs, nil
}

// Put stores the provided multihashes in the underlying datastore, grouping them
// by the first prefixLen bits. It returns only the multihashes that were not
// previously persisted in the datastore (i.e., newly added multihashes).
func (s *MHStore) Put(ctx context.Context, mhs ...mh.Multihash) ([]mh.Multihash, error) {
	if len(mhs) == 0 {
		return nil, nil
	}
	s.lk.Lock()
	defer s.lk.Unlock()

	return s.putLocked(ctx, mhs...)
}

// Get returns all multihashes whose bit256 representation matches the provided
// prefix.
func (s *MHStore) Get(ctx context.Context, prefix bitstr.Key) ([]mh.Multihash, error) {
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
			bs := bitstr.Key(key.BitString(mhToBit256(h)))
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

// Reset deletes all entries under the given datastore prefix and stores the
// provided hashes. Returns the deduplicated multihashes that have been
// persisted.
func (s *MHStore) Reset(ctx context.Context, mhs ...mh.Multihash) ([]mh.Multihash, error) {
	s.lk.Lock()
	defer s.lk.Unlock()

	res, err := s.ds.Query(ctx, query.Query{Prefix: s.base.String()})
	if err != nil {
		return nil, err
	}
	defer res.Close()

	for r := range res.Next() {
		if r.Error != nil {
			return nil, r.Error
		}
		if err := s.ds.Delete(ctx, ds.NewKey(r.Key)); err != nil {
			return nil, err
		}
	}

	if len(mhs) == 0 {
		return nil, nil
	}
	return s.putLocked(ctx, mhs...)
}

// Delete removes the given multihashes from datastore.
func (s *MHStore) Delete(ctx context.Context, mhs ...mh.Multihash) error {
	if len(mhs) == 0 {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	s.lk.Lock()
	defer s.lk.Unlock()

	groups := make(map[bitstr.Key][]mh.Multihash)
	for _, h := range mhs {
		bs := bitstr.Key(key.BitString(mhToBit256(h)))
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
