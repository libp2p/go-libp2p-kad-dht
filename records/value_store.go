package records

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	ds "github.com/ipfs/go-datastore"
	"github.com/libp2p/go-libp2p-kad-dht/internal"
	record "github.com/libp2p/go-libp2p-record"
	recpb "github.com/libp2p/go-libp2p-record/pb"
	"github.com/multiformats/go-base32"
	"google.golang.org/protobuf/proto"
)

// ErrOldRecord is returned by ValueStore.Put when the stored record is at least
// as desirable as the one being written, per the Validator's Select.
var ErrOldRecord = errors.New("old record")

// ValueStore persists DHT value records (such as /pk and /ipns) in a
// datastore. It owns the validate / select / time-stamping policy that used to
// be inlined across the DHT value handlers: records are validated and selected
// against on write, validated and age-checked on read, and stamped with the
// time they were received. Invalid or expired records are discarded on access.
//
// A ValueStore is safe for concurrent use. Writes to the same key are
// serialised so that a concurrent Put cannot overwrite a better record.
type ValueStore struct {
	ds           ds.Datastore
	validator    record.Validator
	maxRecordAge time.Duration
	putLocks     [256]sync.Mutex
}

// NewValueStore returns a ValueStore backed by dstore. It uses validator to
// validate and select records, and treats records older than maxRecordAge as
// absent. A non-positive maxRecordAge disables age expiry.
func NewValueStore(dstore ds.Datastore, validator record.Validator, maxRecordAge time.Duration) *ValueStore {
	return &ValueStore{
		ds:           dstore,
		validator:    validator,
		maxRecordAge: maxRecordAge,
	}
}

// Get returns the record stored under key. It returns (nil, nil) when there is
// no record, or when the stored record is corrupt, fails validation, or is
// older than maxRecordAge; such records are deleted as a side effect. A
// non-nil error indicates a datastore failure.
func (v *ValueStore) Get(ctx context.Context, key string) (*recpb.Record, error) {
	dskey := valueDsKey(key)
	buf, err := v.ds.Get(ctx, dskey)
	if err == ds.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	rec := new(recpb.Record)
	if err := proto.Unmarshal(buf, rec); err != nil {
		v.discardIfUnchanged(ctx, key, dskey, buf)
		return nil, nil
	}

	// The stored record must belong to the key it is filed under.
	if string(rec.GetKey()) != key {
		v.discardIfUnchanged(ctx, key, dskey, buf)
		return nil, nil
	}

	if err := v.validator.Validate(key, rec.GetValue()); err != nil {
		v.discardIfUnchanged(ctx, key, dskey, buf)
		return nil, nil
	}

	if v.expired(rec) {
		v.discardIfUnchanged(ctx, key, dskey, buf)
		return nil, nil
	}

	return rec, nil
}

// Put validates rec, selects it against any record already stored under key,
// stamps it with the current receive time, and stores it. It returns
// ErrOldRecord when the existing record is at least as desirable as rec.
// Concurrent Puts for the same key are serialised. rec is not modified.
func (v *ValueStore) Put(ctx context.Context, key string, rec *recpb.Record) error {
	if err := v.validator.Validate(key, rec.GetValue()); err != nil {
		return fmt.Errorf("validating record: %w", err)
	}

	lock := &v.putLocks[lockIndex(key)]
	lock.Lock()
	defer lock.Unlock()

	dskey := valueDsKey(key)
	existing, err := v.existingForSelect(ctx, dskey)
	if err != nil {
		return err
	}
	if existing != nil {
		best, err := v.validator.Select(key, [][]byte{rec.GetValue(), existing.GetValue()})
		if err != nil {
			return fmt.Errorf("selecting record: %w", err)
		}
		if best != 0 {
			return ErrOldRecord
		}
	}

	// Stamp a clone so we never mutate the caller's record, which may be shared
	// across goroutines (e.g. a record being written locally and sent to peers).
	stored := proto.Clone(rec).(*recpb.Record)
	stored.TimeReceived = internal.FormatRFC3339(time.Now())

	data, err := proto.Marshal(stored)
	if err != nil {
		return err
	}
	return v.ds.Put(ctx, dskey, data)
}

// existingForSelect reads the record currently stored at dskey for comparison
// during Put. Unlike Get it does not age-check or delete: a missing, corrupt,
// or invalid record is reported as absent so the incoming record replaces it.
func (v *ValueStore) existingForSelect(ctx context.Context, dskey ds.Key) (*recpb.Record, error) {
	buf, err := v.ds.Get(ctx, dskey)
	if err == ds.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	rec := new(recpb.Record)
	if err := proto.Unmarshal(buf, rec); err != nil {
		return nil, nil
	}
	if err := v.validator.Validate(string(rec.GetKey()), rec.GetValue()); err != nil {
		return nil, nil
	}
	return rec, nil
}

// expired reports whether rec is older than maxRecordAge, or carries no valid
// receive time. A non-positive maxRecordAge disables age expiry entirely.
func (v *ValueStore) expired(rec *recpb.Record) bool {
	if v.maxRecordAge <= 0 {
		return false
	}
	recvtime, err := internal.ParseRFC3339(rec.GetTimeReceived())
	if err != nil {
		return true
	}
	return time.Since(recvtime) > v.maxRecordAge
}

// discardIfUnchanged best-effort deletes a bad record, but only while it still
// holds the exact bytes the reader saw. It takes the key's Put lock and
// re-reads, so it never clobbers a good record written by a concurrent Put
// between the read and the delete. Failures are logged, not returned.
func (v *ValueStore) discardIfUnchanged(ctx context.Context, key string, dskey ds.Key, seen []byte) {
	lock := &v.putLocks[lockIndex(key)]
	lock.Lock()
	defer lock.Unlock()

	current, err := v.ds.Get(ctx, dskey)
	if err != nil {
		return // already gone, or a datastore error we cannot act on
	}
	if !bytes.Equal(current, seen) {
		return // replaced by a concurrent Put; keep the newer record
	}
	if err := v.ds.Delete(ctx, dskey); err != nil {
		log.Errorw("failed to delete bad record from datastore", "key", dskey, "error", err)
	}
}

// valueDsKey encodes a record key as its datastore key:
// "/" + <namespace> + "/" + base32(fullkey). Value records are stored under
// their record namespace verbatim ("/pk/…", "/ipns/…"), which is readable and,
// because record namespaces are distinct, keeps a shared datastore
// collision-free. providerNamespace is reserved, so a value record that
// (non-standardly) uses it is base32-encoded to keep it out of the "/providers/"
// subtree. base32-encoding the full key keeps the record key recoverable and the
// ValueStore.Get key check valid. A malformed key (no namespace) lands at root.
func valueDsKey(key string) ds.Key {
	ns, _, _ := record.SplitKey(key)
	if ns == providerNamespace {
		ns = base32.RawStdEncoding.EncodeToString([]byte(ns))
	}
	return ds.NewKey("/" + ns + "/" + base32.RawStdEncoding.EncodeToString([]byte(key)))
}

// lockIndex maps a key to one of the striped Put locks by its last byte.
func lockIndex(key string) byte {
	if len(key) == 0 {
		return 0
	}
	return key[len(key)-1]
}
