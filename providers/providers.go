package providers

import (
	"context"
	"encoding/binary"
	"fmt"
	"strings"
	"time"

	lru "github.com/hashicorp/golang-lru/simplelru"
	cid "github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	autobatch "github.com/ipfs/go-datastore/autobatch"
	dsq "github.com/ipfs/go-datastore/query"
	logging "github.com/ipfs/go-log"
	goprocess "github.com/jbenet/goprocess"
	goprocessctx "github.com/jbenet/goprocess/context"
	peer "github.com/libp2p/go-libp2p-peer"
	base32 "github.com/whyrusleeping/base32"
)

var batchBufferSize = 256

var log = logging.Logger("providers")

var lruCacheSize = 256
var ProvideValidity = time.Hour * 24
var defaultCleanupInterval = time.Hour

type ProviderManager struct {
	// all non channel fields are meant to be accessed only within
	// the run method
	providers *lru.LRU
	dstore    *autobatch.Datastore

	newprovs chan *addProv
	getprovs chan *getProv
	proc     goprocess.Process

	cleanupInterval time.Duration
}

type providerSet struct {
	providers []peer.ID
	set       map[peer.ID]time.Time
}

type addProv struct {
	k   cid.Cid
	val peer.ID
}

type getProv struct {
	k    cid.Cid
	resp chan []peer.ID
}

func NewProviderManager(ctx context.Context, local peer.ID, dstore ds.Batching) *ProviderManager {
	pm := new(ProviderManager)
	pm.getprovs = make(chan *getProv)
	pm.newprovs = make(chan *addProv)
	pm.dstore = autobatch.NewAutoBatching(dstore, batchBufferSize)
	cache, err := lru.NewLRU(lruCacheSize, nil)
	if err != nil {
		panic(err) //only happens if negative value is passed to lru constructor
	}
	pm.providers = cache

	pm.proc = goprocessctx.WithContext(ctx)
	pm.cleanupInterval = defaultCleanupInterval
	pm.proc.Go(pm.run)

	return pm
}

const providersKeyPrefix = "/providers/"

func mkProvKey(k cid.Cid) string {
	return providersKeyPrefix + base32.RawStdEncoding.EncodeToString(k.Bytes())
}

func (pm *ProviderManager) Process() goprocess.Process {
	return pm.proc
}

func (pm *ProviderManager) providersForKey(k cid.Cid) ([]peer.ID, error) {
	pset, err := pm.getProvSet(k)
	if err != nil {
		return nil, err
	}
	return pset.providers, nil
}

func (pm *ProviderManager) getProvSet(k cid.Cid) (*providerSet, error) {
	cached, ok := pm.providers.Get(k)
	if ok {
		return cached.(*providerSet), nil
	}

	pset, err := loadProvSet(pm.dstore, k)
	if err != nil {
		return nil, err
	}

	if len(pset.providers) > 0 {
		pm.providers.Add(k, pset)
	}

	return pset, nil
}

func loadProvSet(dstore ds.Datastore, k cid.Cid) (*providerSet, error) {
	res, err := dstore.Query(dsq.Query{Prefix: mkProvKey(k)})
	if err != nil {
		return nil, err
	}
	defer res.Close()

	now := time.Now()
	out := newProviderSet()
	for {
		e, ok := res.NextSync()
		if !ok {
			break
		}
		if e.Error != nil {
			log.Error("got an error: ", e.Error)
			continue
		}

		// check expiration time
		t, err := readTimeValue(e.Value)
		switch {
		case err != nil:
			// couldn't parse the time
			log.Warning("parsing providers record from disk: ", err)
			fallthrough
		case now.Sub(t) > ProvideValidity:
			// or just expired
			err = dstore.Delete(ds.RawKey(e.Key))
			if err != nil && err != ds.ErrNotFound {
				log.Warning("failed to remove provider record from disk: ", err)
			}
			continue
		}

		lix := strings.LastIndex(e.Key, "/")

		decstr, err := base32.RawStdEncoding.DecodeString(e.Key[lix+1:])
		if err != nil {
			log.Error("base32 decoding error: ", err)
			err = dstore.Delete(ds.RawKey(e.Key))
			if err != nil && err != ds.ErrNotFound {
				log.Warning("failed to remove provider record from disk: ", err)
			}
			continue
		}

		pid := peer.ID(decstr)

		out.setVal(pid, t)
	}

	return out, nil
}

func readTimeValue(data []byte) (time.Time, error) {
	nsec, n := binary.Varint(data)
	if n <= 0 {
		return time.Time{}, fmt.Errorf("failed to parse time")
	}

	return time.Unix(0, nsec), nil
}

func (pm *ProviderManager) addProv(k cid.Cid, p peer.ID) error {
	now := time.Now()
	if provs, ok := pm.providers.Get(k); ok {
		provs.(*providerSet).setVal(p, now)
	} // else not cached, just write through

	return writeProviderEntry(pm.dstore, k, p, now)
}

func writeProviderEntry(dstore ds.Datastore, k cid.Cid, p peer.ID, t time.Time) error {
	dsk := mkProvKey(k) + "/" + base32.RawStdEncoding.EncodeToString([]byte(p))

	buf := make([]byte, 16)
	n := binary.PutVarint(buf, t.UnixNano())

	return dstore.Put(ds.NewKey(dsk), buf[:n])
}

func (pm *ProviderManager) gc() {
	res, err := pm.dstore.Query(dsq.Query{
		Prefix: providersKeyPrefix,
	})
	if err != nil {
		log.Error("error garbage collecting provider records: ", err)
		return
	}
	defer res.Close()

	now := time.Now()
	for {
		e, ok := res.NextSync()
		if !ok {
			return
		}

		if e.Error != nil {
			log.Error("got an error: ", e.Error)
			continue
		}

		// check expiration time
		t, err := readTimeValue(e.Value)
		switch {
		case err != nil:
			// couldn't parse the time
			log.Warning("parsing providers record from disk: ", err)
			fallthrough
		case now.Sub(t) > ProvideValidity:
			// or just expired
			err = pm.dstore.Delete(ds.RawKey(e.Key))
			if err != nil {
				log.Warning("failed to remove provider record from disk: ", err)
			}
		}
	}
}

func (pm *ProviderManager) run(proc goprocess.Process) {
	tick := time.NewTicker(pm.cleanupInterval)
	defer tick.Stop()
	defer pm.dstore.Flush()

	for {
		select {
		case np := <-pm.newprovs:
			err := pm.addProv(np.k, np.val)
			if err != nil {
				log.Error("error adding new providers: ", err)
			}
		case gp := <-pm.getprovs:
			provs, err := pm.providersForKey(gp.k)
			if err != nil && err != ds.ErrNotFound {
				log.Error("error reading providers: ", err)
			}

			// set the cap so the user can't append to this.
			gp.resp <- provs[0:len(provs):len(provs)]
		case <-tick.C:
			// You know the wonderful thing about caches? You can
			// drop them.
			//
			// Much faster than GCing.
			pm.providers.Purge()
			pm.gc()
		case <-proc.Closing():
			return
		}
	}
}

// AddProvider adds a provider.
func (pm *ProviderManager) AddProvider(ctx context.Context, k cid.Cid, val peer.ID) {
	prov := &addProv{
		k:   k,
		val: val,
	}
	select {
	case pm.newprovs <- prov:
	case <-ctx.Done():
	}
}

// GetProviders returns the set of providers for the given key.
// This method _does not_ copy the set. Do not modify it.
func (pm *ProviderManager) GetProviders(ctx context.Context, k cid.Cid) []peer.ID {
	gp := &getProv{
		k:    k,
		resp: make(chan []peer.ID, 1), // buffered to prevent sender from blocking
	}
	select {
	case <-ctx.Done():
		return nil
	case pm.getprovs <- gp:
	}
	select {
	case <-ctx.Done():
		return nil
	case peers := <-gp.resp:
		return peers
	}
}

func newProviderSet() *providerSet {
	return &providerSet{
		set: make(map[peer.ID]time.Time),
	}
}

func (ps *providerSet) Add(p peer.ID) {
	ps.setVal(p, time.Now())
}

func (ps *providerSet) setVal(p peer.ID, t time.Time) {
	_, found := ps.set[p]
	if !found {
		ps.providers = append(ps.providers, p)
	}

	ps.set[p] = t
}
