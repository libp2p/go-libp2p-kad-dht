package dht

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/benbjohnson/clock"
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
	clk          clock.Clock
	MaxRecordAge time.Duration
	Logger       *slog.Logger
	Tele         *Telemetry
}

func DefaultRecordBackendConfig() (*RecordBackendConfig, error) {
	telemetry, err := NewWithGlobalProviders()
	if err != nil {
		return nil, fmt.Errorf("new telemetry: %w", err)
	}

	return &RecordBackendConfig{
		clk:          clock.New(),
		Logger:       slog.Default(),
		Tele:         telemetry,
		MaxRecordAge: 48 * time.Hour, // empirically measured in: https://github.com/plprobelab/network-measurements/blob/master/results/rfm17-provider-record-liveness.md
	}, nil
}

func (r *RecordBackend) Store(ctx context.Context, key string, value any) (any, error) {
	rec, ok := value.(*recpb.Record)
	if !ok {
		return nil, fmt.Errorf("expected *recpb.Record value type, got: %T", value)
	}

	if err := r.validator.Validate(r.routingKey(key), rec.GetValue()); err != nil {
		return nil, fmt.Errorf("put bad record: %w", err)
	}

	txn, err := r.datastore.NewTransaction(ctx, false)
	if err != nil {
		return nil, fmt.Errorf("new transaction: %w", err)
	}
	defer txn.Discard(ctx) // discard is a no-op if txn was committed beforehand

	dsKey := newDatastoreKey(r.namespace, key)
	shouldReplace, err := r.shouldReplaceExistingRecord(ctx, txn, dsKey, rec.GetValue())
	if err != nil {
		return nil, fmt.Errorf("checking datastore for better record: %w", err)
	} else if !shouldReplace {
		return nil, fmt.Errorf("received worse record")
	}

	// avoid storing arbitrary data, so overwrite that field
	rec.TimeReceived = r.cfg.clk.Now().UTC().Format(time.RFC3339Nano)

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

func (r *RecordBackend) Fetch(ctx context.Context, key string) (any, error) {
	dsKey := newDatastoreKey(r.namespace, key)

	// fetch record from the datastore for the requested key
	buf, err := r.datastore.Get(ctx, dsKey)
	if err != nil {
		return nil, err
	}

	// we have found a record, parse it and do basic validation
	rec := &recpb.Record{}
	err = rec.Unmarshal(buf)
	if err != nil {
		// we have a corrupt record in the datastore -> delete it and pretend
		// that we don't know about it
		if err := r.datastore.Delete(ctx, dsKey); err != nil {
			r.log.LogAttrs(ctx, slog.LevelWarn, "Failed deleting corrupt record from datastore", slog.String("err", err.Error()))
		}

		return nil, nil
	}

	// validate that we don't serve stale records.
	receivedAt, err := time.Parse(time.RFC3339Nano, rec.GetTimeReceived())
	if err != nil || r.cfg.clk.Since(receivedAt) > r.cfg.MaxRecordAge {
		errStr := ""
		if err != nil {
			errStr = err.Error()
		}

		r.log.LogAttrs(ctx, slog.LevelWarn, "Invalid received timestamp on stored record", slog.String("err", errStr), slog.Duration("age", time.Since(receivedAt)))
		if err = r.datastore.Delete(ctx, dsKey); err != nil {
			r.log.LogAttrs(ctx, slog.LevelWarn, "Failed deleting bad record from datastore", slog.String("err", err.Error()))
		}
		return nil, nil
	}

	// We don't do any additional validation beyond checking the above
	// timestamp. We put the burden of validating the record on the requester as
	// checking a record may be computationally expensive.

	return rec, nil
}

func (r *RecordBackend) Validate(ctx context.Context, key string, values ...any) (int, error) {
	k := newRoutingKey(r.namespace, key)

	// short circuit if it's just a single value
	if len(values) == 1 {
		data, ok := values[0].([]byte)
		if !ok {
			return -1, fmt.Errorf("value not byte slice")
		}

		if err := r.validator.Validate(k, data); err != nil {
			return -1, err
		}

		return 0, nil
	}

	// In case there are invalid values in the slice, we still want to return
	// the index in the original list of values. The Select method below will
	// return the index of the "best" value in the slice of valid values. This
	// slice can have a different length and therefore that method will return
	// an index that doesn't match the values slice that's passed into this
	// method. origIdx stores the original index
	origIdx := map[int]int{}
	validValues := [][]byte{}
	for i, value := range values {
		data, ok := value.([]byte)
		if !ok {
			continue
		}

		if err := r.validator.Validate(k, data); err != nil {
			continue
		}

		origIdx[len(validValues)] = i
		validValues = append(validValues, data)
	}

	if len(validValues) == 0 {
		return -1, fmt.Errorf("no valid values")
	}

	sel, err := r.validator.Select(k, validValues)
	if err != nil {
		return -1, err
	}

	return origIdx[sel], nil
}

// shouldReplaceExistingRecord returns true if the given record should replace any
// existing one in the local datastore. It queries the datastore, unmarshalls
// the record, validates it, and compares it to the incoming record. If the
// incoming one is "better" (e.g., just newer), this function returns true.
// If unmarshalling or validation fails, this function (alongside an error) also
// returns true because the existing record should be replaced.
func (r *RecordBackend) shouldReplaceExistingRecord(ctx context.Context, txn ds.Read, dsKey ds.Key, value []byte) (bool, error) {
	ctx, span := r.cfg.Tele.Tracer.Start(ctx, "RecordBackend.shouldReplaceExistingRecord")
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

	if err := r.validator.Validate(string(existingRec.GetKey()), existingRec.GetValue()); err != nil {
		return true, nil
	}

	records := [][]byte{value, existingRec.GetValue()}
	i, err := r.validator.Select(dsKey.String(), records)
	if err != nil {
		return false, fmt.Errorf("record selection: %w", err)
	} else if i != 0 {
		return false, nil
	}

	return true, nil
}

// routingKey returns the routing key for the given key by prefixing it with
// the namespace.
func (r *RecordBackend) routingKey(key string) string {
	return fmt.Sprintf("/%s/%s", r.namespace, key)
}
