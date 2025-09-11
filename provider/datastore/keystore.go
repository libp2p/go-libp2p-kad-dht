package datastore

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"

	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/namespace"
	"github.com/ipfs/go-datastore/query"
	"github.com/libp2p/go-libp2p-kad-dht/provider/internal/keyspace"
	mh "github.com/multiformats/go-multihash"

	"github.com/probe-lab/go-libdht/kad"
	"github.com/probe-lab/go-libdht/kad/key/bit256"
	"github.com/probe-lab/go-libdht/kad/key/bitstr"
)

var ErrKeyStoreClosed = errors.New("keystore is closed")

// KeyStore provides thread-safe storage and retrieval of multihashes, indexed
// by their kademlia 256-bit identifier.
type KeyStore interface {
	Put(context.Context, ...mh.Multihash) ([]mh.Multihash, error)
	Get(context.Context, bitstr.Key) ([]mh.Multihash, error)
	ContainsPrefix(context.Context, bitstr.Key) (bool, error)
	Delete(context.Context, ...mh.Multihash) error
	Empty(context.Context) error
	Size(context.Context) (int, error)
	Close() error
}

// operation types for the worker goroutine
type opType uint8

const (
	opPut opType = iota
	opGet
	opContainsPrefix
	opDelete
	opEmpty
	opSize
	lastOp
)

// operation request sent to worker goroutine
type operation struct {
	op       opType
	ctx      context.Context
	keys     []mh.Multihash
	prefix   bitstr.Key
	response chan<- operationResponse
}

// response from worker goroutine
type operationResponse struct {
	multihashes []mh.Multihash
	found       bool
	size        int
	err         error
}

// keyStore indexes multihashes by their kademlia identifier.
type keyStore struct {
	ds         ds.Batching
	prefixBits int
	batchSize  int

	// worker goroutine communication
	requests chan operation
	close    chan struct{}
	done     chan struct{}
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

	ks := &keyStore{
		ds:         namespace.Wrap(d, ds.NewKey(cfg.base)),
		prefixBits: cfg.prefixBits,
		batchSize:  cfg.batchSize,
		requests:   make(chan operation),
		close:      make(chan struct{}),
		done:       make(chan struct{}),
	}

	// start worker goroutine
	go ks.worker()

	return ks, nil
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
func dsKey[K kad.Key[K]](k K, prefixBits int) ds.Key {
	b := strings.Builder{}
	l := k.BitLen()
	for i := range min(prefixBits, l) {
		b.WriteRune(rune('0' + k.Bit(i)))
		b.WriteRune('/')
	}
	if l == keyspace.KeyLen {
		b.WriteString(base64.URLEncoding.EncodeToString(keyspace.KeyToBytes(k)[prefixBits/8:]))
	}
	return ds.NewKey(b.String())
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

// worker processes operations sequentially in a single goroutine
func (s *keyStore) worker() {
	defer close(s.done)

	for {
		select {
		case <-s.close:
			return
		case op := <-s.requests:
			switch op.op {
			case opPut:
				newKeys, err := s.put(op.ctx, op.keys)
				op.response <- operationResponse{multihashes: newKeys, err: err}

			case opGet:
				keys, err := s.get(op.ctx, op.prefix)
				op.response <- operationResponse{multihashes: keys, err: err}

			case opContainsPrefix:
				found, err := s.containsPrefix(op.ctx, op.prefix)
				op.response <- operationResponse{found: found, err: err}

			case opDelete:
				err := s.delete(op.ctx, op.keys)
				op.response <- operationResponse{err: err}

			case opEmpty:
				err := empty(op.ctx, s.ds, s.batchSize)
				op.response <- operationResponse{err: err}

			case opSize:
				size, err := s.size(op.ctx)
				op.response <- operationResponse{size: size, err: err}

			default:
				op.response <- operationResponse{err: fmt.Errorf("unknown operation %d", op.op)}
			}
		}
	}
}

// put stores the provided keys while assuming s.lk is already held, and
// returns the keys that weren't present already in the keystore.
func (s *keyStore) put(ctx context.Context, keys []mh.Multihash) ([]mh.Multihash, error) {
	seen := make(map[bit256.Key]struct{}, len(keys))
	toPut := make([]pair, 0, len(keys))

	for _, h := range keys {
		k := keyspace.MhToBit256(h)
		if _, ok := seen[k]; ok {
			continue
		}
		seen[k] = struct{}{}
		dsk := dsKey(k, s.prefixBits)
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

// get returns all keys whose bit256 representation matches the provided
// prefix.
func (s *keyStore) get(ctx context.Context, prefix bitstr.Key) ([]mh.Multihash, error) {
	dsk := dsKey(prefix, s.prefixBits).String()
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

// containsPrefix reports whether the KeyStore currently holds at least one
// multihash whose kademlia identifier (bit256.Key) starts with the provided
// bit-prefix.
func (s *keyStore) containsPrefix(ctx context.Context, prefix bitstr.Key) (bool, error) {
	dsk := dsKey(prefix, s.prefixBits).String()
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

// empty deletes all entries under the datastore prefix, assuming s.lk is
// already held.
func empty(ctx context.Context, d ds.Batching, batchSize int) error {
	res, err := d.Query(ctx, query.Query{KeysOnly: true})
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

// delete removes the given keys from datastore.
func (s *keyStore) delete(ctx context.Context, keys []mh.Multihash) error {
	b, err := s.ds.Batch(ctx)
	if err != nil {
		return err
	}
	for _, h := range keys {
		dsk := dsKey(keyspace.MhToBit256(h), s.prefixBits)
		err := b.Delete(ctx, dsk)
		if err != nil {
			return err
		}
	}
	return b.Commit(ctx)
}

// size returns the number of keys currently stored in the KeyStore.
func (s *keyStore) size(ctx context.Context) (size int, err error) {
	q := query.Query{KeysOnly: true}
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

// Put stores the provided keys in the underlying datastore, grouping them by
// the first prefixLen bits. It returns only the keys that were not previously
// persisted in the datastore (i.e., newly added keys).
func (s *keyStore) Put(ctx context.Context, keys ...mh.Multihash) ([]mh.Multihash, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	response := make(chan operationResponse, 1)
	select {
	case s.requests <- operation{
		op:       opPut,
		ctx:      ctx,
		keys:     keys,
		response: response,
	}:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-s.close:
		return nil, ErrKeyStoreClosed
	}

	select {
	case resp := <-response:
		return resp.multihashes, resp.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Get returns all keys whose bit256 representation matches the provided
// prefix.
func (s *keyStore) Get(ctx context.Context, prefix bitstr.Key) ([]mh.Multihash, error) {
	response := make(chan operationResponse, 1)
	select {
	case s.requests <- operation{
		op:       opGet,
		ctx:      ctx,
		prefix:   prefix,
		response: response,
	}:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-s.close:
		return nil, ErrKeyStoreClosed
	}

	select {
	case resp := <-response:
		return resp.multihashes, resp.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// ContainsPrefix reports whether the KeyStore currently holds at least one
// multihash whose kademlia identifier (bit256.Key) starts with the provided
// bit-prefix.
func (s *keyStore) ContainsPrefix(ctx context.Context, prefix bitstr.Key) (bool, error) {
	response := make(chan operationResponse, 1)
	select {
	case s.requests <- operation{
		op:       opContainsPrefix,
		ctx:      ctx,
		prefix:   prefix,
		response: response,
	}:
	case <-ctx.Done():
		return false, ctx.Err()
	case <-s.close:
		return false, ErrKeyStoreClosed
	}

	select {
	case resp := <-response:
		return resp.found, resp.err
	case <-ctx.Done():
		return false, ctx.Err()
	}
}

// Empty deletes all entries under the datastore prefix.
func (s *keyStore) Empty(ctx context.Context) error {
	response := make(chan operationResponse, 1)
	select {
	case s.requests <- operation{
		op:       opEmpty,
		ctx:      ctx,
		response: response,
	}:
	case <-ctx.Done():
		return ctx.Err()
	case <-s.close:
		return ErrKeyStoreClosed
	}

	select {
	case resp := <-response:
		return resp.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Delete removes the given keys from datastore.
func (s *keyStore) Delete(ctx context.Context, keys ...mh.Multihash) error {
	if len(keys) == 0 {
		return nil
	}
	response := make(chan operationResponse, 1)
	select {
	case s.requests <- operation{
		op:       opDelete,
		ctx:      ctx,
		keys:     keys,
		response: response,
	}:
	case <-ctx.Done():
		return ctx.Err()
	case <-s.close:
		return ErrKeyStoreClosed
	}

	select {
	case resp := <-response:
		return resp.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Size returns the number of keys currently stored in the KeyStore.
//
// The size is obtained by iterating over all keys in the underlying
// datastore, so it may be expensive for large stores.
func (s *keyStore) Size(ctx context.Context) (int, error) {
	response := make(chan operationResponse, 1)
	select {
	case s.requests <- operation{
		op:       opSize,
		ctx:      ctx,
		response: response,
	}:
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-s.close:
		return 0, ErrKeyStoreClosed
	}

	select {
	case resp := <-response:
		return resp.size, resp.err
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

// Close shuts down the worker goroutine and releases resources.
func (s *keyStore) Close() error {
	select {
	case <-s.close:
		// Already closed
		return nil
	default:
		close(s.close)
		<-s.done
	}
	return nil
}
