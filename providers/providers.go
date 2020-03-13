package providers

import (
	"context"
	"encoding/binary"
	"fmt"
	"strings"
	"time"

	"github.com/libp2p/go-libp2p-core/peer"

	lru "github.com/hashicorp/golang-lru/simplelru"
	ds "github.com/ipfs/go-datastore"
	autobatch "github.com/ipfs/go-datastore/autobatch"
	dsq "github.com/ipfs/go-datastore/query"
	logging "github.com/ipfs/go-log"
	goprocess "github.com/jbenet/goprocess"
	goprocessctx "github.com/jbenet/goprocess/context"
	base32 "github.com/multiformats/go-base32"
)

var batchBufferSize = 256

var log = logging.Logger("providers")

var lruCacheSize = 256
var ProvideValidity = time.Hour * 24
var defaultCleanupInterval = time.Hour

const providersKeyPrefix = "/providers/"

type ProviderManager struct {
	// all non channel fields are meant to be accessed only within
	// the run method
	cache  *lru.LRU
	dstore *autobatch.Datastore

	newprovs chan *addProv
	getprovs chan *GetProv
	proc     goprocess.Process

	cleanupInterval time.Duration
}

type providerSet struct {
	providers []peer.ID
	set       map[peer.ID]time.Time
}

type addProv struct {
	k   []byte
	val peer.ID
}

type GetProv struct {
	k    []byte
	resp chan []peer.ID
}

func NewProviderManager(ctx context.Context, local peer.ID, dstore ds.Batching) *ProviderManager {
	pm := new(ProviderManager)
	pm.getprovs = make(chan *GetProv)
	pm.newprovs = make(chan *addProv)
	pm.dstore = autobatch.NewAutoBatching(dstore, batchBufferSize)
	cache, err := lru.NewLRU(lruCacheSize, nil)
	if err != nil {
		panic(err) //only happens if negative value is passed to lru constructor
	}
	pm.cache = cache

	pm.proc = goprocessctx.WithContext(ctx)
	pm.cleanupInterval = defaultCleanupInterval
	pm.proc.Go(pm.run)

	return pm
}

func readTimeValue(data []byte) (time.Time, error) {
	nsec, n := binary.Varint(data)
	if n <= 0 {
		return time.Time{}, fmt.Errorf("failed to parse time")
	}

	return time.Unix(0, nsec), nil
}

func (pm *ProviderManager) Process() goprocess.Process {
	return pm.proc
}

func (pm *ProviderManager) run(proc goprocess.Process) {
	var (
		gcQuery    dsq.Results
		gcQueryRes <-chan dsq.Result
		gcSkip     map[string]struct{}
		gcTime     time.Time
		gcTimer    = time.NewTimer(pm.cleanupInterval)
	)

	defer func() {
		gcTimer.Stop()
		if gcQuery != nil {
			// don't really care if this fails.
			_ = gcQuery.Close()
		}
		if err := pm.dstore.Flush(); err != nil {
			log.Error("failed to flush datastore: ", err)
		}
	}()

	for {
		select {
		case np := <-pm.newprovs:
			err := pm.addProv(np.k, np.val)
			if err != nil {
				log.Error("error adding new providers: ", err)
				continue
			}
			if gcSkip != nil {
				// we have an gc, tell it to skip this provider
				// as we've updated it since the GC started.
				gcSkip[mkProvKeyFor(np.k, np.val)] = struct{}{}
			}
		case gp := <-pm.getprovs:
			provs, err := pm.getProvidersForKey(gp.k)
			if err != nil && err != ds.ErrNotFound {
				log.Error("error reading providers: ", err)
			}

			// set the cap so the user can't append to this.
			gp.resp <- provs[0:len(provs):len(provs)]
		case res, ok := <-gcQueryRes:
			if !ok {
				if err := gcQuery.Close(); err != nil {
					log.Error("failed to close provider GC query: ", err)
				}
				gcTimer.Reset(pm.cleanupInterval)

				// cleanup GC round
				gcQueryRes = nil
				gcSkip = nil
				gcQuery = nil
				continue
			}
			if res.Error != nil {
				log.Error("got error from GC query: ", res.Error)
				continue
			}
			if _, ok := gcSkip[res.Key]; ok {
				// We've updated this record since starting the
				// GC round, skip it.
				continue
			}

			// check expiration time
			t, err := readTimeValue(res.Value)
			switch {
			case err != nil:
				// couldn't parse the time
				log.Error("parsing providers record from disk: ", err)
				fallthrough
			case gcTime.Sub(t) > ProvideValidity:
				// or expired
				err = pm.dstore.Delete(ds.RawKey(res.Key))
				if err != nil && err != ds.ErrNotFound {
					log.Error("failed to remove provider record from disk: ", err)
				}
			}

		case gcTime = <-gcTimer.C:
			// You know the wonderful thing about caches? You can drop them.
			// Much faster than GCing.
			pm.cache.Purge()

			// Now, kick off a GC of the datastore.
			q, err := pm.dstore.Query(dsq.Query{
				Prefix: providersKeyPrefix,
			})
			if err != nil {
				log.Error("provider record GC query failed: ", err)
				continue
			}
			gcQuery = q
			gcQueryRes = q.Next()
			gcSkip = make(map[string]struct{})
		case <-proc.Closing():
			return
		}
	}
}

// AddProvider adds a provider.
func (pm *ProviderManager) AddProvider(ctx context.Context, k []byte, val peer.ID) {
	prov := &addProv{
		k:   k,
		val: val,
	}
	select {
	case pm.newprovs <- prov:
	case <-ctx.Done():
	}
}

// addProv updates the cache if the key was already on the cache
func (pm *ProviderManager) addProv(k []byte, p peer.ID) error {
	now := time.Now()
	// If there is something on cache, update cache
	if cached, ok := pm.cache.Get(string(k)); ok {
		cached.(*providerSet).setVal(p, now)
	} // else not cached, just write through

	return writeProviderEntry(pm.dstore, k, p, now)
}

func mkProvKeyFor(k []byte, p peer.ID) string {
	return mkProvKey(k) + "/" + base32.RawStdEncoding.EncodeToString([]byte(p))
}

func mkProvKey(k []byte) string {
	return providersKeyPrefix + base32.RawStdEncoding.EncodeToString(k)
}

func writeProviderEntry(dstore ds.Datastore, k []byte, p peer.ID, t time.Time) error {
	dsk := mkProvKeyFor(k, p)

	buf := make([]byte, 16)
	n := binary.PutVarint(buf, t.UnixNano())

	return dstore.Put(ds.NewKey(dsk), buf[:n])
}

// GetProviders returns the set of providers for the given key.
// Warning: This method _does not_ copy the set. Do not modify it.
func (pm *ProviderManager) GetProviders(ctx context.Context, k []byte) []peer.ID {
	gp := &GetProv{
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

// GetAllProviders returns the set of all providers
// Warning: This method _does not_ copy the set. Do not modify it.
func (pm *ProviderManager) GetAllProviders() ([]GetProv, error) {

	// Collect all keys with providersKeyPrefix
	dsKeys, err := pm.dstore.Query(dsq.Query{Prefix: providersKeyPrefix, KeysOnly: true})
	if err != nil {
		return nil, err
	}

	pks := []string{}
	for dsKey := range dsKeys.Next() {
		// extract the providerKey
		split := strings.Split(dsKey.Key, "/")
		providerKeyStr := split[2]
		pks = append(pks, providerKeyStr)
	}
	pks = removeDuplicatesUnordered(pks)
	fmt.Println(pks)

	getProvs := []GetProv{}
	for _, providerKeyStr := range pks {
		providerKeyByte, err := base32.RawStdEncoding.DecodeString(providerKeyStr)
		if err != nil {
			return nil, err

		}
		// ask for the PeerIds using standard process
		gp := GetProv{
			k:    providerKeyByte,
			resp: make(chan []peer.ID, 1),
		}

		getProvs = append(getProvs, gp)
		pm.getprovs <- &gp

	}

	return getProvs, nil
}

func removeDuplicatesUnordered(elements []string) []string {
	encountered := map[string]bool{}

	// Create a map of all unique elements.
	for v := range elements {
		encountered[elements[v]] = true
	}

	// Place all keys from the map into a slice.
	result := []string{}
	for key, _ := range encountered {
		result = append(result, key)
	}
	return result
}

// Yields the PeerIDs of the providers from local (cache or datastore) for a given key
func (pm *ProviderManager) getProvidersForKey(k []byte) ([]peer.ID, error) {
	pset, err := pm.getProviderSetForKey(k)
	if err != nil {
		return nil, err
	}
	return pset.providers, nil
}

// Yields a ProvidersSet from local (cache or datastore). Checks cache first
func (pm *ProviderManager) getProviderSetForKey(k []byte) (*providerSet, error) {
	cached, ok := pm.cache.Get(string(k))
	if ok {
		return cached.(*providerSet), nil
	}

	pset, err := loadProviderSet(pm.dstore, k)
	if err != nil {
		return nil, err
	}

	if len(pset.providers) > 0 {
		pm.cache.Add(string(k), pset)
	}

	return pset, nil
}

// Retrieve providers from the datastore
func loadProviderSet(dstore ds.Datastore, k []byte) (*providerSet, error) {
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
			log.Error("parsing providers record from disk: ", err)
			fallthrough
		case now.Sub(t) > ProvideValidity:
			// or just expired
			err = dstore.Delete(ds.RawKey(e.Key))
			if err != nil && err != ds.ErrNotFound {
				log.Error("failed to remove provider record from disk: ", err)
			}
			continue
		}

		lix := strings.LastIndex(e.Key, "/")

		decstr, err := base32.RawStdEncoding.DecodeString(e.Key[lix+1:])
		if err != nil {
			log.Error("base32 decoding error: ", err)
			err = dstore.Delete(ds.RawKey(e.Key))
			if err != nil && err != ds.ErrNotFound {
				log.Error("failed to remove provider record from disk: ", err)
			}
			continue
		}

		pid := peer.ID(decstr)

		out.setVal(pid, t)
	}

	return out, nil
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
