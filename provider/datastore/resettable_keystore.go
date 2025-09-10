package datastore

import (
	"context"
	"errors"
	"fmt"

	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	"github.com/libp2p/go-libp2p-kad-dht/provider/internal/keyspace"
	mh "github.com/multiformats/go-multihash"
)

var ErrResetInProgress = errors.New("reset already in progress")

const (
	opStart opType = iota + lastOp + 1
	opCleanup
	opAltPut
)

type resetOp struct {
	op       opType
	success  bool
	response chan<- error
}

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

	altBase         ds.Key
	resetInProgress bool
	resetSync       chan []mh.Multihash // passes keys from worker to reset go routine
	resetOps        chan resetOp        // reset operations that must be run in main go routine
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

	rks := &ResettableKeyStore{
		keyStore: keyStore{
			ds:         d,
			base:       ds.NewKey(cfg.base).ChildString("0"),
			prefixBits: cfg.prefixBits,
			batchSize:  cfg.batchSize,
			requests:   make(chan operation),
			close:      make(chan struct{}),
			done:       make(chan struct{}),
		},
		altBase:   ds.NewKey(cfg.base).ChildString("1"),
		resetOps:  make(chan resetOp),
		resetSync: make(chan []mh.Multihash, 128), // buffered to avoid blocking
	}

	// start worker goroutine
	go rks.worker()

	return rks, nil
}

// worker processes operations sequentially in a single goroutine for ResettableKeyStore
func (s *ResettableKeyStore) worker() {
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
				err := empty(op.ctx, s.ds, s.base, s.batchSize)
				op.response <- operationResponse{err: err}

			case opSize:
				size, err := s.size(op.ctx)
				op.response <- operationResponse{size: size, err: err}

			case opAltPut:
				err := s.altPut(op.ctx, op.keys)
				op.response <- operationResponse{err: err}

			}
		case op := <-s.resetOps:
			s.handleResetOp(op)
		}
	}
}

// resettablePutLocked handles put operations for ResettableKeyStore, with special
// handling during reset operations.
func (s *ResettableKeyStore) put(ctx context.Context, keys []mh.Multihash) ([]mh.Multihash, error) {
	if s.resetInProgress {
		// Reset is in progress, write to alternate base in addition to current base
		s.resetSync <- keys
	}
	return s.keyStore.put(ctx, keys)
}

// altPut writes the given multihashes to the alternate base in the datastore.
func (s *ResettableKeyStore) altPut(ctx context.Context, keys []mh.Multihash) error {
	b, err := s.ds.Batch(ctx)
	if err != nil {
		return err
	}
	for _, h := range keys {
		dsk := dsKey(keyspace.MhToBit256(h), s.prefixBits, s.altBase)
		if err := b.Put(ctx, dsk, h); err != nil {
			return err
		}
	}
	return b.Commit(ctx)
}

// handleResetOp processes reset operations that need to happen synchronously.
func (s *ResettableKeyStore) handleResetOp(op resetOp) {
	if op.op == opStart {
		if s.resetInProgress {
			op.response <- ErrResetInProgress
			return
		}
		if err := empty(context.Background(), s.ds, s.altBase, s.batchSize); err != nil {
			op.response <- err
			return
		}
		s.resetInProgress = true
		op.response <- nil
		return
	}

	// Cleanup operation
	if op.success {
		// Swap the keystore prefix bases.
		oldBase := s.base
		s.base = s.altBase
		s.altBase = oldBase
	}
	// Drain resetSync
drain:
	for {
		select {
		case <-s.resetSync:
		default:
			break drain
		}
	}
	// Empty the unused base prefix
	s.resetInProgress = false
	op.response <- empty(context.Background(), s.ds, s.altBase, s.batchSize)
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
	if keysChan == nil {
		return nil
	}

	opsChan := make(chan error)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.done:
		return ErrKeyStoreClosed
	case s.resetOps <- resetOp{op: opStart, response: opsChan}:
		select {
		case err := <-opsChan:
			if err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	var success bool

	defer func() {
		// Cleanup before returning on success and failure
		select {
		case s.resetOps <- resetOp{op: opCleanup, success: success, response: opsChan}:
			<-opsChan
		case <-s.done:
			// Safe not to go through the worker since we are done, and we need to
			// cleanup
			empty(context.Background(), s.ds, s.altBase, s.batchSize)
		}
	}()

	rsp := make(chan operationResponse)
	batchPut := func(ctx context.Context, keys []mh.Multihash) error {
		select {
		case <-s.done:
			return ErrKeyStoreClosed
		case <-ctx.Done():
			return ctx.Err()
		case s.requests <- operation{op: opAltPut, ctx: ctx, keys: keys, response: rsp}:
			return (<-rsp).err
		}
	}

	keys := make([]mh.Multihash, 0)

	processNewKeys := func(newKeys ...mh.Multihash) error {
		keys = append(keys, newKeys...)
		if len(keys) >= s.batchSize {
			if err := batchPut(ctx, keys); err != nil {
				return err
			}
			keys = keys[:0]
		}
		return nil
	}

	// Read all the keys from the channel and put them in batch at the alternate base
loop:
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.done:
			return ErrKeyStoreClosed
		case mhs := <-s.resetSync:
			if err := processNewKeys(mhs...); err != nil {
				return err
			}
		case c, ok := <-keysChan:
			if !ok {
				break loop
			}
			if err := processNewKeys(c.Hash()); err != nil {
				return err
			}
		}
	}
	// Put final batch
	if err := batchPut(ctx, keys); err != nil {
		return err
	}
	success = true

	return nil
}
