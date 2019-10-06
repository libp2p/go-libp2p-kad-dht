package dht

import (
	"context"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	u "github.com/ipfs/go-ipfs-util"
	"github.com/jbenet/goprocess"
	"github.com/jbenet/goprocess/context"
	"github.com/libp2p/go-libp2p-kad-dht/providers"
	recpb "github.com/libp2p/go-libp2p-record/pb"
)

// vars for cleaning up expired records
var nonProvRecordCleanupInterval = time.Hour

// maxNonProvRecordAge specifies the maximum time that any node will hold onto a record
// from the time its received. This does not apply to any other forms of validity that
// the record may contain.
// For example, a record may contain an ipns entry with an EOL saying its valid
// until the year 2020 (a great time in the future). For that record to stick around
// it must be rebroadcasted more frequently than once every 'maxNonProvRecordAge'
var maxNonProvRecordAge = time.Hour * 36

// vars for republishing records
var nonProvRecordRePublishInterval = 1 * time.Hour
var nonProvRecordRePublishAge = 1 * time.Hour
var enableRepublishJitter = true

type NonProvRecordsManager struct {
	dht *IpfsDHT
	ctx context.Context

	proc   goprocess.Process
	dstore ds.Batching

	cleanupInterval time.Duration // scan interval for expiring records

	rePublishInterval time.Duration // scan interval for republishing records
}

func NewNonProvRecordsManager(ctx context.Context, dht *IpfsDHT, dstore ds.Batching) *NonProvRecordsManager {
	m := new(NonProvRecordsManager)
	m.dht = dht
	m.ctx = ctx
	m.dstore = dstore
	m.proc = goprocessctx.WithContext(ctx)

	// expire records beyond maxage
	m.cleanupInterval = nonProvRecordCleanupInterval
	m.proc.Go(m.expire)

	// republish records older than prescribed age
	m.rePublishInterval = nonProvRecordRePublishInterval
	m.proc.Go(m.rePublish)

	return m
}

func (m *NonProvRecordsManager) Process() goprocess.Process {
	return m.proc
}

func (m *NonProvRecordsManager) rePublish(proc goprocess.Process) {
	for {
		var d = 0 * time.Minute
		// minimizes the probability of all peers re-publishing together
		// the first peer that re-publishes resets the receivedAt time on the record
		// on all other peers that are among the K closest to the key, thus minimizing the number of republishes by other peers
		if enableRepublishJitter {
			d = time.Duration(rand.Intn(16)) * time.Minute
		}

		select {
		case <-proc.Closing():
			return
		case <-time.After(m.rePublishInterval + d):
		}

		tFnc := func(t time.Time) bool {
			return time.Since(t) > nonProvRecordRePublishAge && time.Since(t) < maxNonProvRecordAge
		}

		res, err := m.dstore.Query(query.Query{Filters: []query.Filter{&nonProvRecordFilter{tFnc}}})
		if err != nil {
			logger.Errorf("republish records proc: failed to run query against datastore, error is %s", err)
			continue
		}

		var wg sync.WaitGroup
		// semaphore to rate-limit number of concurrent PutValue calls
		semaphore := make(chan struct{}, 5)
		for {
			e, ok := res.NextSync()
			if !ok {
				break
			}

			semaphore <- struct{}{}
			wg.Add(1)
			go func(e query.Result) {
				defer func() {
					<-semaphore
					wg.Done()
				}()

				// unmarshal record
				rec := new(recpb.Record)
				if err := proto.Unmarshal(e.Value, rec); err != nil {
					logger.Debugf("republish records proc: failed to unmarshal DHT record from datastore, error is %s", err)
					return
				}

				// call put value
				putCtx, cancel := context.WithTimeout(m.ctx, 2*time.Minute)
				defer cancel()

				// do not use e.key here as that represents the transformed version of the original key
				// rec.GetKey is the original key sent by the peer who put this record to dht
				if err := m.dht.PutValue(putCtx, string(rec.GetKey()), rec.Value); err != nil {
					logger.Debugf("republish records proc: failed to re-publish to the network, error is %s", err)
				}
			}(e)
		}
		wg.Wait()
	}
}

func (m *NonProvRecordsManager) expire(proc goprocess.Process) {
	for {
		select {
		case <-proc.Closing():
			return
		case <-time.After(m.cleanupInterval):
		}

		tFnc := func(t time.Time) bool {
			return time.Since(t) > maxNonProvRecordAge
		}

		res, err := m.dstore.Query(query.Query{Filters: []query.Filter{&nonProvRecordFilter{tFnc}}})
		if err != nil {
			logger.Errorf("expire records proc: failed to run query against datastore, error is %s", err)
			continue
		}

		for {
			e, ok := res.NextSync()
			if !ok {
				break
			}
			if err := m.dstore.Delete(ds.RawKey(e.Key)); err != nil {
				logger.Errorf("expire records proc: failed to delete key %s from datastore, error is %s", e.Key, err)
			}
		}
	}
}

type timeFilterFnc = func(t time.Time) bool

type nonProvRecordFilter struct {
	tFnc timeFilterFnc
}

func (f *nonProvRecordFilter) Filter(e query.Entry) bool {
	// unmarshal record
	rec := new(recpb.Record)
	if err := proto.Unmarshal(e.Value, rec); err != nil {
		logger.Debugf("expire records filter: failed to unmarshal DHT record from datastore, error is %s", err)
		return false
	}

	// should not be a provider record
	if strings.HasPrefix(e.Key, providers.ProvidersKeyPrefix) {
		return false
	}

	// parse received time
	t, err := u.ParseRFC3339(rec.TimeReceived)
	if err != nil {
		logger.Debugf("expire records filter: failed to parse time in DHT record, error is %s", err)
		return false
	}

	// apply the time filter fnc to the received time
	return f.tFnc(t)
}
