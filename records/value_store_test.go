package records

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/libp2p/go-libp2p-kad-dht/internal"
	record "github.com/libp2p/go-libp2p-record"
	recpb "github.com/libp2p/go-libp2p-record/pb"
	"github.com/multiformats/go-base32"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// lenValidator accepts any value not prefixed with "bad" and treats the longest
// value as the most desirable, so tests can drive Select deterministically.
type lenValidator struct{}

func (lenValidator) Validate(_ string, value []byte) error {
	if bytes.HasPrefix(value, []byte("bad")) {
		return errors.New("invalid value")
	}
	return nil
}

func (lenValidator) Select(_ string, values [][]byte) (int, error) {
	best := 0
	for i, v := range values {
		if len(v) > len(values[best]) {
			best = i
		}
	}
	return best, nil
}

func newTestStore(maxAge time.Duration) (*ValueStore, ds.Datastore) {
	dstore := dssync.MutexWrap(ds.NewMapDatastore())
	return NewValueStore(dstore, lenValidator{}, maxAge), dstore
}

func mustPut(t *testing.T, s *ValueStore, key, value string) {
	t.Helper()
	require.NoErrorf(t, s.Put(t.Context(), key, record.MakePutRecord(key, []byte(value))),
		"put %q=%q", key, value)
}

func TestValueStorePutGetRoundTrip(t *testing.T) {
	s, _ := newTestStore(time.Hour)
	mustPut(t, s, "/pk/abc", "hello")

	rec, err := s.Get(t.Context(), "/pk/abc")
	require.NoError(t, err)
	require.NotNil(t, rec)
	require.Equal(t, []byte("hello"), rec.GetValue())
	require.Equal(t, []byte("/pk/abc"), rec.GetKey())
	require.NotEmptyf(t, rec.GetTimeReceived(), "Put must stamp a receive time")
}

func TestValueStoreGetMissing(t *testing.T) {
	s, _ := newTestStore(time.Hour)
	rec, err := s.Get(t.Context(), "/pk/missing")
	require.NoError(t, err)
	require.Nil(t, rec)
}

// Put must stamp the server's own receive time and never persist or mutate the
// caller-supplied one (the old cleanRecord guarantee, now enforced by Put).
func TestValueStorePutStampsReceiveTime(t *testing.T) {
	s, dstore := newTestStore(time.Hour)

	rec := record.MakePutRecord("/pk/abc", []byte("hello"))
	rec.TimeReceived = "attacker-supplied"
	require.NoError(t, s.Put(t.Context(), "/pk/abc", rec))

	require.Equalf(t, "attacker-supplied", rec.TimeReceived,
		"Put must not mutate the caller's record")

	stored := readRaw(t, dstore, "/pk/abc")
	require.NotEqual(t, "attacker-supplied", stored.GetTimeReceived())
	_, err := internal.ParseRFC3339(stored.GetTimeReceived())
	require.NoErrorf(t, err, "stored receive time must be a valid RFC3339 stamp")
}

func TestValueStorePutValidation(t *testing.T) {
	s, dstore := newTestStore(time.Hour)

	err := s.Put(t.Context(), "/pk/abc", record.MakePutRecord("/pk/abc", []byte("bad-value")))
	require.Error(t, err)

	_, err = dstore.Get(t.Context(), valueDsKey("/pk/abc"))
	require.ErrorIs(t, err, ds.ErrNotFound, "invalid record must not be stored")
}

func TestValueStorePutSelect(t *testing.T) {
	tests := []struct {
		name     string
		first    string
		second   string
		wantErr  error
		wantHeld string
	}{
		{"better replaces worse", "aa", "bbbb", nil, "bbbb"},
		{"worse rejected", "bbbb", "c", ErrOldRecord, "bbbb"},
		{"equal replaces", "aa", "cc", nil, "cc"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, _ := newTestStore(time.Hour)
			mustPut(t, s, "/pk/k", tt.first)

			err := s.Put(t.Context(), "/pk/k", record.MakePutRecord("/pk/k", []byte(tt.second)))
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}

			rec, err := s.Get(t.Context(), "/pk/k")
			require.NoError(t, err)
			require.NotNil(t, rec)
			require.Equal(t, []byte(tt.wantHeld), rec.GetValue())
		})
	}
}

func TestValueStoreGetDiscardsBadRecords(t *testing.T) {
	tests := []struct {
		name  string
		key   string
		write func(t *testing.T, dstore ds.Datastore, key string)
	}{
		{
			name: "corrupt bytes",
			key:  "/pk/corrupt",
			write: func(t *testing.T, dstore ds.Datastore, key string) {
				require.NoError(t, dstore.Put(t.Context(), valueDsKey(key), []byte("not a protobuf")))
			},
		},
		{
			name: "fails validation",
			key:  "/pk/invalid",
			write: func(t *testing.T, dstore ds.Datastore, key string) {
				writeRaw(t, dstore, key, record.MakePutRecord(key, []byte("bad-value")))
			},
		},
		{
			name: "key mismatch",
			key:  "/pk/mismatch",
			write: func(t *testing.T, dstore ds.Datastore, key string) {
				// A valid record filed under the wrong datastore location.
				rec := record.MakePutRecord("/pk/other", []byte("value"))
				rec.TimeReceived = internal.FormatRFC3339(time.Now())
				data, err := proto.Marshal(rec)
				require.NoError(t, err)
				require.NoError(t, dstore.Put(t.Context(), valueDsKey(key), data))
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, dstore := newTestStore(time.Hour)
			tt.write(t, dstore, tt.key)

			rec, err := s.Get(t.Context(), tt.key)
			require.NoError(t, err)
			require.Nil(t, rec)

			_, err = dstore.Get(t.Context(), valueDsKey(tt.key))
			require.ErrorIsf(t, err, ds.ErrNotFound, "bad record must be discarded on read")
		})
	}
}

func TestValueStoreGetExpiry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const maxAge = time.Hour
		s, dstore := newTestStore(maxAge)
		mustPut(t, s, "/pk/k", "hello")

		// Still fresh well within maxAge.
		time.Sleep(maxAge / 2)
		rec, err := s.Get(t.Context(), "/pk/k")
		require.NoError(t, err)
		require.NotNilf(t, rec, "record within maxAge must be served")

		// Now past maxAge.
		time.Sleep(maxAge)
		rec, err = s.Get(t.Context(), "/pk/k")
		require.NoError(t, err)
		require.Nilf(t, rec, "expired record must not be served")

		_, err = dstore.Get(t.Context(), valueDsKey("/pk/k"))
		require.ErrorIsf(t, err, ds.ErrNotFound, "expired record must be discarded")
	})
}

// A non-positive maxRecordAge (as a hand-built FullRT config carries) must
// disable age expiry rather than treat every record as instantly expired.
func TestValueStoreNoExpiryWhenAgeZero(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, dstore := newTestStore(0)
		mustPut(t, s, "/pk/k", "hello")

		time.Sleep(1000 * time.Hour)
		rec, err := s.Get(t.Context(), "/pk/k")
		require.NoError(t, err)
		require.NotNilf(t, rec, "with maxRecordAge<=0 records must never expire")

		_, err = dstore.Get(t.Context(), valueDsKey("/pk/k"))
		require.NoErrorf(t, err, "record must not be discarded when age expiry is disabled")
	})
}

// discardIfUnchanged must delete a bad record only while it still holds the
// exact bytes the reader saw, so it cannot clobber a concurrent Put's record.
func TestValueStoreDiscardIfUnchanged(t *testing.T) {
	s, dstore := newTestStore(time.Hour)
	dskey := valueDsKey("/pk/k")
	require.NoError(t, dstore.Put(t.Context(), dskey, []byte("garbage")))

	// Bytes differ from what the reader saw (a Put replaced them): keep them.
	s.discardIfUnchanged(t.Context(), "/pk/k", dskey, []byte("stale"))
	got, err := dstore.Get(t.Context(), dskey)
	require.NoError(t, err)
	require.Equal(t, []byte("garbage"), got)

	// Bytes match what the reader saw: delete them.
	s.discardIfUnchanged(t.Context(), "/pk/k", dskey, []byte("garbage"))
	_, err = dstore.Get(t.Context(), dskey)
	require.ErrorIs(t, err, ds.ErrNotFound)
}

func TestValueStorePutConcurrent(t *testing.T) {
	s, _ := newTestStore(time.Hour)
	const key = "/pk/race"

	// Concurrent writers of varying-length values plus concurrent readers. With
	// the longest value being "best", the store must converge on it regardless
	// of ordering, with no data race.
	var wg sync.WaitGroup
	for i := 1; i <= 16; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			value := bytes.Repeat([]byte("x"), n)
			_ = s.Put(context.Background(), key, record.MakePutRecord(key, value))
			_, _ = s.Get(context.Background(), key)
		}(i)
	}
	wg.Wait()

	rec, err := s.Get(t.Context(), key)
	require.NoError(t, err)
	require.NotNil(t, rec)
	require.Lenf(t, rec.GetValue(), 16, "the longest (best) value must win")
}

// A Put must overwrite an existing record that is itself corrupt or invalid,
// rather than letting Select block on it.
func TestValueStorePutOverwritesBadExisting(t *testing.T) {
	tests := []struct {
		name     string
		writeBad func(t *testing.T, dstore ds.Datastore, key string)
	}{
		{"corrupt existing", func(t *testing.T, dstore ds.Datastore, key string) {
			require.NoError(t, dstore.Put(t.Context(), valueDsKey(key), []byte("garbage")))
		}},
		{"invalid existing", func(t *testing.T, dstore ds.Datastore, key string) {
			writeRaw(t, dstore, key, record.MakePutRecord(key, []byte("bad-existing")))
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, dstore := newTestStore(time.Hour)
			tt.writeBad(t, dstore, "/pk/k")

			require.NoError(t, s.Put(t.Context(), "/pk/k", record.MakePutRecord("/pk/k", []byte("fresh"))))

			rec, err := s.Get(t.Context(), "/pk/k")
			require.NoError(t, err)
			require.NotNil(t, rec)
			require.Equal(t, []byte("fresh"), rec.GetValue())
		})
	}
}

// A record with no valid receive time is treated as expired.
func TestValueStoreGetNoReceiveTime(t *testing.T) {
	s, dstore := newTestStore(time.Hour)
	rec := record.MakePutRecord("/pk/k", []byte("hello")) // TimeReceived left empty
	data, err := proto.Marshal(rec)
	require.NoError(t, err)
	require.NoError(t, dstore.Put(t.Context(), valueDsKey("/pk/k"), data))

	got, err := s.Get(t.Context(), "/pk/k")
	require.NoError(t, err)
	require.Nilf(t, got, "record without a receive time must be treated as expired")

	_, err = dstore.Get(t.Context(), valueDsKey("/pk/k"))
	require.ErrorIs(t, err, ds.ErrNotFound)
}

func TestValueStoreEmptyKey(t *testing.T) {
	s, _ := newTestStore(time.Hour)
	require.NoError(t, s.Put(t.Context(), "", record.MakePutRecord("", []byte("v"))))
	rec, err := s.Get(t.Context(), "")
	require.NoError(t, err)
	require.NotNil(t, rec)
	require.Equal(t, []byte("v"), rec.GetValue())
}

// errInjected is returned by failingDatastore for the operations it is told to fail.
var errInjected = errors.New("injected datastore failure")

type failingDatastore struct {
	ds.Datastore
	failGet, failPut, failDelete bool
}

func (f *failingDatastore) Get(ctx context.Context, key ds.Key) ([]byte, error) {
	if f.failGet {
		return nil, errInjected
	}
	return f.Datastore.Get(ctx, key)
}

func (f *failingDatastore) Put(ctx context.Context, key ds.Key, value []byte) error {
	if f.failPut {
		return errInjected
	}
	return f.Datastore.Put(ctx, key, value)
}

func (f *failingDatastore) Delete(ctx context.Context, key ds.Key) error {
	if f.failDelete {
		return errInjected
	}
	return f.Datastore.Delete(ctx, key)
}

func TestValueStoreDatastoreErrors(t *testing.T) {
	tests := []struct {
		name string
		fail func(f *failingDatastore)
		op   func(t *testing.T, s *ValueStore) error
	}{
		{
			name: "get propagates read error",
			fail: func(f *failingDatastore) { f.failGet = true },
			op: func(t *testing.T, s *ValueStore) error {
				_, err := s.Get(t.Context(), "/pk/k")
				return err
			},
		},
		{
			name: "put propagates existing-read error",
			fail: func(f *failingDatastore) { f.failGet = true },
			op: func(t *testing.T, s *ValueStore) error {
				return s.Put(t.Context(), "/pk/k", record.MakePutRecord("/pk/k", []byte("v")))
			},
		},
		{
			name: "put propagates write error",
			fail: func(f *failingDatastore) { f.failPut = true },
			op: func(t *testing.T, s *ValueStore) error {
				return s.Put(t.Context(), "/pk/k", record.MakePutRecord("/pk/k", []byte("v")))
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dstore := &failingDatastore{Datastore: dssync.MutexWrap(ds.NewMapDatastore())}
			tt.fail(dstore)
			s := NewValueStore(dstore, lenValidator{}, time.Hour)
			require.ErrorIs(t, tt.op(t, s), errInjected)
		})
	}
}

// A read that finds a bad record still returns "not found" even when the
// best-effort delete of that record fails.
func TestValueStoreDiscardToleratesDeleteFailure(t *testing.T) {
	inner := dssync.MutexWrap(ds.NewMapDatastore())
	require.NoError(t, inner.Put(t.Context(), valueDsKey("/pk/k"), []byte("garbage")))
	dstore := &failingDatastore{Datastore: inner, failDelete: true}

	s := NewValueStore(dstore, lenValidator{}, time.Hour)
	rec, err := s.Get(t.Context(), "/pk/k")
	require.NoError(t, err)
	require.Nil(t, rec)
}

type selectErrValidator struct{ lenValidator }

func (selectErrValidator) Select(string, [][]byte) (int, error) {
	return 0, errors.New("select boom")
}

// A Select failure while comparing against the existing record is surfaced.
func TestValueStorePutSelectError(t *testing.T) {
	dstore := dssync.MutexWrap(ds.NewMapDatastore())
	s := NewValueStore(dstore, selectErrValidator{}, time.Hour)

	require.NoError(t, s.Put(t.Context(), "/pk/k", record.MakePutRecord("/pk/k", []byte("first"))))

	err := s.Put(t.Context(), "/pk/k", record.MakePutRecord("/pk/k", []byte("second")))
	require.ErrorContains(t, err, "selecting record")
}

func readRaw(t *testing.T, dstore ds.Datastore, key string) *recpb.Record {
	t.Helper()
	buf, err := dstore.Get(t.Context(), valueDsKey(key))
	require.NoError(t, err)
	rec := new(recpb.Record)
	require.NoError(t, proto.Unmarshal(buf, rec))
	return rec
}

func writeRaw(t *testing.T, dstore ds.Datastore, key string, rec *recpb.Record) {
	t.Helper()
	rec.TimeReceived = internal.FormatRFC3339(time.Now())
	data, err := proto.Marshal(rec)
	require.NoError(t, err)
	require.NoError(t, dstore.Put(t.Context(), valueDsKey(key), data))
}

// TestValueDsKeyNamespaced checks value records are keyed under their record
// namespace verbatim ("/pk/…", "/ipns/…"), and never collide with the reserved
// provider namespace.
func TestValueDsKeyNamespaced(t *testing.T) {
	for key, wantNS := range map[string]string{
		"/pk/abc":      "pk",
		"/ipns/abc":    "ipns",
		"/dnslink/abc": "dnslink",
	} {
		got := valueDsKey(key).String()
		wantEnc := base32.RawStdEncoding.EncodeToString([]byte(key))
		require.Equalf(t, "/"+wantNS+"/"+wantEnc, got, "valueDsKey(%q)", key)
		require.Falsef(t, strings.HasPrefix(got, ProvidersKeyPrefix),
			"value key %q collides with the provider namespace", got)
	}

	// A value record that (non-standardly) uses the reserved provider namespace
	// is base32-encoded, so it stays out of the /providers/ subtree.
	reserved := valueDsKey("/" + providerNamespace + "/x").String()
	require.False(t, strings.HasPrefix(reserved, ProvidersKeyPrefix))
}

const (
	gcMaxAge     = time.Hour
	gcInterval   = 10 * time.Minute
	pastMaxAge   = gcMaxAge + gcInterval // enough real time for a sweep to fire after expiry
	withinMaxAge = gcMaxAge / 2
)

// hasKey reports whether dstore holds an entry at the datastore key.
func hasKey(t *testing.T, dstore ds.Datastore, key ds.Key) bool {
	t.Helper()
	has, err := dstore.Has(t.Context(), key)
	require.NoError(t, err)
	return has
}

// TestValueStoreGCSweepsExpired checks the background sweep proactively deletes
// an expired value record without anyone reading it first.
func TestValueStoreGCSweepsExpired(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, dstore := newTestStore(gcMaxAge)
		mustPut(t, s, "/pk/k", "hello")

		s.StartGC(t.Context(), gcInterval)
		defer func() { require.NoError(t, s.Close()) }()

		time.Sleep(pastMaxAge)
		synctest.Wait()

		require.Falsef(t, hasKey(t, dstore, valueDsKey("/pk/k")),
			"expired record must be swept without a prior read")
	})
}

// TestValueStoreGCKeepsFresh checks the sweep leaves records still within
// maxRecordAge in place.
func TestValueStoreGCKeepsFresh(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, dstore := newTestStore(gcMaxAge)
		mustPut(t, s, "/pk/k", "hello")

		s.StartGC(t.Context(), gcInterval)
		defer func() { require.NoError(t, s.Close()) }()

		time.Sleep(withinMaxAge)
		synctest.Wait()

		require.Truef(t, hasKey(t, dstore, valueDsKey("/pk/k")),
			"a record within maxRecordAge must survive GC")
	})
}

// TestValueStoreGCLeavesProviderRecords checks the value sweep never touches the
// reserved provider subtree, so value and provider records can share a
// datastore.
func TestValueStoreGCLeavesProviderRecords(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, dstore := newTestStore(gcMaxAge)
		mustPut(t, s, "/pk/k", "hello")

		provKey := ds.NewKey(ProvidersKeyPrefix + "somecid/someprovider")
		require.NoError(t, dstore.Put(t.Context(), provKey, []byte("provider-entry")))

		s.StartGC(t.Context(), gcInterval)
		defer func() { require.NoError(t, s.Close()) }()

		time.Sleep(pastMaxAge)
		synctest.Wait()

		require.Falsef(t, hasKey(t, dstore, valueDsKey("/pk/k")), "expired value must be swept")
		require.Truef(t, hasKey(t, dstore, provKey), "provider records must never be swept by value GC")
	})
}

// TestValueStoreGCScopesToNamespaces checks that when the validator is
// namespaced the sweep queries only the value namespaces, leaving unrelated
// datastore entries untouched.
func TestValueStoreGCScopesToNamespaces(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		dstore := dssync.MutexWrap(ds.NewMapDatastore())
		nsval := record.NamespacedValidator{"pk": lenValidator{}}
		s := NewValueStore(dstore, nsval, gcMaxAge)
		mustPut(t, s, "/pk/k", "hello")

		foreign := ds.NewKey("/unrelated/entry")
		require.NoError(t, dstore.Put(t.Context(), foreign, []byte("not a value record")))

		s.StartGC(t.Context(), gcInterval)
		defer func() { require.NoError(t, s.Close()) }()

		time.Sleep(pastMaxAge)
		synctest.Wait()

		require.Falsef(t, hasKey(t, dstore, valueDsKey("/pk/k")), "expired value must be swept")
		require.Truef(t, hasKey(t, dstore, foreign),
			"entries outside the value namespaces must be left untouched")
	})
}

// TestValueStoreGCDisabled checks StartGC is a no-op when age expiry or the
// interval is off, and that Close is safe to call regardless.
func TestValueStoreGCDisabled(t *testing.T) {
	tests := []struct {
		name     string
		maxAge   time.Duration
		interval time.Duration
	}{
		{"age expiry disabled", 0, gcInterval},
		{"interval disabled", gcMaxAge, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				s, dstore := newTestStore(tt.maxAge)
				mustPut(t, s, "/pk/k", "hello")

				s.StartGC(t.Context(), tt.interval)
				defer func() { require.NoError(t, s.Close()) }()

				time.Sleep(pastMaxAge)
				synctest.Wait()

				require.Truef(t, hasKey(t, dstore, valueDsKey("/pk/k")),
					"no sweep must run when GC is disabled")
			})
		})
	}
}

// TestValueStoreCloseWithoutGC checks Close is safe when StartGC was never
// called.
func TestValueStoreCloseWithoutGC(t *testing.T) {
	s, _ := newTestStore(gcMaxAge)
	require.NoError(t, s.Close())
	require.NoError(t, s.Close(), "Close must be idempotent")
}

// TestValueStoreCloseIdempotentAfterStartGC checks Close joins the sweeper and
// that a second Close after a started sweep still returns cleanly.
func TestValueStoreCloseIdempotentAfterStartGC(t *testing.T) {
	s, _ := newTestStore(gcMaxAge)
	s.StartGC(t.Context(), gcInterval)
	require.NoError(t, s.Close())
	require.NoError(t, s.Close(), "Close after a started sweep must be idempotent")
}

// TestValueStoreGCWholeStoreLeavesNonOwnEntries drives the whole-datastore scan
// (non-namespaced validator) and checks its safety guards: entries that are not
// well-formed records, or are filed under the wrong datastore key, are left
// alone; only the store's own expired records are swept.
func TestValueStoreGCWholeStoreLeavesNonOwnEntries(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, dstore := newTestStore(time.Minute) // lenValidator -> whole-store scan
		ctx := t.Context()

		// An expired record the store itself owns: must be swept.
		mine := record.MakePutRecord("/pk/mine", []byte("stale"))
		mine.TimeReceived = internal.FormatRFC3339(time.Now().Add(-2 * time.Minute))
		mineData, err := proto.Marshal(mine)
		require.NoError(t, err)
		require.NoError(t, dstore.Put(ctx, valueDsKey("/pk/mine"), mineData))

		// Non-protobuf bytes: unmarshal fails, so the sweep must leave them.
		garbage := valueDsKey("/pk/garbage")
		require.NoError(t, dstore.Put(ctx, garbage, []byte("not a protobuf")))

		// A record filed under the wrong datastore key: the round-trip check keeps
		// the sweep from deleting it (Get discards such anomalies on read; GC, being
		// conservative, does not).
		misfiled := valueDsKey("/pk/here")
		other := record.MakePutRecord("/pk/other", []byte("value"))
		other.TimeReceived = internal.FormatRFC3339(time.Now().Add(-2 * time.Minute))
		otherData, err := proto.Marshal(other)
		require.NoError(t, err)
		require.NoError(t, dstore.Put(ctx, misfiled, otherData))

		s.StartGC(ctx, gcInterval)
		defer func() { require.NoError(t, s.Close()) }()

		time.Sleep(pastMaxAge)
		synctest.Wait()

		require.Falsef(t, hasKey(t, dstore, valueDsKey("/pk/mine")), "expired own record must be swept")
		require.Truef(t, hasKey(t, dstore, garbage), "non-protobuf entry must survive the sweep")
		require.Truef(t, hasKey(t, dstore, misfiled), "misfiled record must survive the sweep")
	})
}

// TestValueStoreGCConcurrentPutSurvives checks the sweep cannot clobber a record
// written by a concurrent Put: the safe delete only removes bytes it still sees,
// so a freshly re-Put (non-expired) record always survives. Runs under -race.
func TestValueStoreGCConcurrentPutSurvives(t *testing.T) {
	s, dstore := newTestStore(time.Minute)
	const key = "/pk/race"

	// Seed an already-expired record the sweep wants to delete.
	expired := record.MakePutRecord(key, []byte("old"))
	expired.TimeReceived = internal.FormatRFC3339(time.Now().Add(-2 * time.Minute))
	data, err := proto.Marshal(expired)
	require.NoError(t, err)
	require.NoError(t, dstore.Put(t.Context(), valueDsKey(key), data))

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for range 200 {
			s.collectExpired(context.Background())
		}
	}()
	go func() {
		defer wg.Done()
		for range 200 {
			// A fresh record is never expired, so it must survive the sweep.
			_ = s.Put(context.Background(), key, record.MakePutRecord(key, []byte("keepme")))
		}
	}()
	wg.Wait()

	// The last write is a fresh Put, so the key still holds a fresh record.
	rec, err := s.Get(t.Context(), key)
	require.NoError(t, err)
	require.NotNilf(t, rec, "a concurrently-Put fresh record must never be swept by GC")
	require.Equal(t, []byte("keepme"), rec.GetValue())
}
