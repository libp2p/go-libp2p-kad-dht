package dht

import (
	"context"
	"errors"
	"fmt"
	"time"

	ds "github.com/ipfs/go-datastore"
	record "github.com/libp2p/go-libp2p-record"
	recpb "github.com/libp2p/go-libp2p-record/pb"
	"golang.org/x/exp/slog"
)

type RecordBackend struct {
	cfg       *RecordBackendConfig
	log       *slog.Logger
	namespace string
	datastore ds.TxnDatastore
	validator record.Validator
}

var _ Backend = (*RecordBackend)(nil)

type RecordBackendConfig struct {
	MaxRecordAge time.Duration
	Logger       *slog.Logger
}

func DefaultRecordBackendConfig() *RecordBackendConfig {
	return &RecordBackendConfig{
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
