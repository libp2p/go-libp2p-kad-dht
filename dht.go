package dht

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/opentracing/opentracing-go/log"

	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/peerstore"
	"github.com/libp2p/go-libp2p-core/protocol"
	"github.com/libp2p/go-libp2p-core/routing"

	"go.opencensus.io/tag"

	"github.com/libp2p/go-libp2p-kad-dht/metrics"
	opts "github.com/libp2p/go-libp2p-kad-dht/opts"
	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
	"github.com/libp2p/go-libp2p-kad-dht/providers"

	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	logging "github.com/ipfs/go-log"
	"github.com/jbenet/goprocess"
	goprocessctx "github.com/jbenet/goprocess/context"
	kb "github.com/libp2p/go-libp2p-kbucket"
	record "github.com/libp2p/go-libp2p-record"
	recpb "github.com/libp2p/go-libp2p-record/pb"
	"github.com/multiformats/go-base32"

	"github.com/opentracing/opentracing-go"
)

var logger = logging.Logger("dht")

// IpfsDHT is an implementation of Kademlia with S/Kademlia modifications.
// It is used to implement the base Routing module.
type IpfsDHT struct {
	host      host.Host           // the network services we need
	self      peer.ID             // Local peer (yourself)
	peerstore peerstore.Peerstore // Peer Registry

	datastore ds.Datastore // Local data

	routingTable *kb.RoutingTable // Array of routing tables for differently distanced nodes
	providers    *providers.ProviderManager

	birth time.Time // When this peer started up

	Validator record.Validator

	ctx  context.Context
	proc goprocess.Process

	strmap map[peer.ID]*messageSender
	smlk   sync.Mutex

	plk sync.Mutex

	stripedPutLocks [256]sync.Mutex

	protocols []protocol.ID // DHT protocols

	bucketSize int

	autoRefresh           bool
	rtRefreshQueryTimeout time.Duration
	rtRefreshPeriod       time.Duration
	triggerRtRefresh      chan chan<- error

	maxRecordAge time.Duration
}

// Assert that IPFS assumptions about interfaces aren't broken. These aren't a
// guarantee, but we can use them to aid refactoring.
var (
	_ routing.ContentRouting = (*IpfsDHT)(nil)
	_ routing.Routing        = (*IpfsDHT)(nil)
	_ routing.PeerRouting    = (*IpfsDHT)(nil)
	_ routing.PubKeyFetcher  = (*IpfsDHT)(nil)
	_ routing.ValueStore     = (*IpfsDHT)(nil)
)

// New creates a new DHT with the specified host and options.
func New(ctx context.Context, h host.Host, options ...opts.Option) (*IpfsDHT, error) {
	var cfg opts.Options
	cfg.BucketSize = KValue
	if err := cfg.Apply(append([]opts.Option{opts.Defaults}, options...)...); err != nil {
		return nil, err
	}
	dht := makeDHT(ctx, h, cfg.Datastore, cfg.Protocols, cfg.BucketSize)
	dht.autoRefresh = cfg.RoutingTable.AutoRefresh
	dht.rtRefreshPeriod = cfg.RoutingTable.RefreshPeriod
	dht.rtRefreshQueryTimeout = cfg.RoutingTable.RefreshQueryTimeout

	dht.maxRecordAge = cfg.MaxRecordAge

	// register for network notifs.
	dht.host.Network().Notify((*netNotifiee)(dht))

	dht.proc = goprocessctx.WithContextAndTeardown(ctx, func() error {
		// remove ourselves from network notifs.
		dht.host.Network().StopNotify((*netNotifiee)(dht))
		return nil
	})

	dht.proc.AddChild(dht.providers.Process())
	dht.Validator = cfg.Validator

	if !cfg.Client {
		for _, p := range cfg.Protocols {
			h.SetStreamHandler(p, dht.handleNewStream)
		}
	}
	dht.startRefreshing()
	return dht, nil
}

// NewDHT creates a new DHT object with the given peer as the 'local' host.
// IpfsDHT's initialized with this function will respond to DHT requests,
// whereas IpfsDHT's initialized with NewDHTClient will not.
func NewDHT(ctx context.Context, h host.Host, dstore ds.Batching) *IpfsDHT {
	dht, err := New(ctx, h, opts.Datastore(dstore))
	if err != nil {
		panic(err)
	}
	return dht
}

// NewDHTClient creates a new DHT object with the given peer as the 'local'
// host. IpfsDHT clients initialized with this function will not respond to DHT
// requests. If you need a peer to respond to DHT requests, use NewDHT instead.
// NewDHTClient creates a new DHT object with the given peer as the 'local' host
func NewDHTClient(ctx context.Context, h host.Host, dstore ds.Batching) *IpfsDHT {
	dht, err := New(ctx, h, opts.Datastore(dstore), opts.Client(true))
	if err != nil {
		panic(err)
	}
	return dht
}

func makeDHT(ctx context.Context, h host.Host, dstore ds.Batching, protocols []protocol.ID, bucketSize int) *IpfsDHT {
	rt := kb.NewRoutingTable(bucketSize, kb.ConvertPeerID(h.ID()), time.Minute, h.Peerstore())
	cmgr := h.ConnManager()

	rt.PeerAdded = func(p peer.ID) {
		cmgr.TagPeer(p, "kbucket", 5)
	}

	rt.PeerRemoved = func(p peer.ID) {
		cmgr.UntagPeer(p, "kbucket")
	}

	dht := &IpfsDHT{
		datastore:        dstore,
		self:             h.ID(),
		peerstore:        h.Peerstore(),
		host:             h,
		strmap:           make(map[peer.ID]*messageSender),
		ctx:              ctx,
		providers:        providers.NewProviderManager(ctx, h.ID(), dstore),
		birth:            time.Now(),
		routingTable:     rt,
		protocols:        protocols,
		bucketSize:       bucketSize,
		triggerRtRefresh: make(chan chan<- error),
	}

	dht.ctx = dht.newContextWithLocalTags(ctx)

	return dht
}

// TODO Implement RT seeding as described in https://github.com/libp2p/go-libp2p-kad-dht/pull/384#discussion_r320994340 OR
// come up with an alternative solution.
// issue is being tracked at https://github.com/libp2p/go-libp2p-kad-dht/issues/387
/*func (dht *IpfsDHT) rtRecovery(proc goprocess.Process) {
	writeResp := func(errorChan chan error, err error) {
		select {
		case <-proc.Closing():
		case errorChan <- errChan:
		}
		close(errorChan)
	}

	for {
		select {
		case req := <-dht.rtRecoveryChan:
			if dht.routingTable.Size() == 0 {
				logger.Infof("rt recovery proc: received request with reqID=%s, RT is empty. initiating recovery", req.id)
				// TODO Call Seeder with default bootstrap peers here once #383 is merged
				if dht.routingTable.Size() > 0 {
					logger.Infof("rt recovery proc: successfully recovered RT for reqID=%s, RT size is now %d", req.id, dht.routingTable.Size())
					go writeResp(req.errorChan, nil)
				} else {
					logger.Errorf("rt recovery proc: failed to recover RT for reqID=%s, RT is still empty", req.id)
					go writeResp(req.errorChan, errors.New("RT empty after seed attempt"))
				}
			} else {
				logger.Infof("rt recovery proc: RT is not empty, no need to act on request with reqID=%s", req.id)
				go writeResp(req.errorChan, nil)
			}
		case <-proc.Closing():
			return
		}
	}
}*/

// putValueToPeer stores the given key/value pair at the peer 'p'.
func (dht *IpfsDHT) putValueToPeer(ctx context.Context, p peer.ID, rec *recpb.Record) error {
	pmes := pb.NewMessage(pb.Message_PUT_VALUE, rec.Key, 0)
	pmes.Record = rec

	var err error
	sp, ctx := opentracing.StartSpanFromContext(ctx, "dht.put_value.out", opentracing.Tags{
		"target.peer_id": p,
		"dht.record_key": rec.GetKey(),
	})
	defer LinkedFinish(sp, &err)

	rpmes, err := dht.sendRequest(ctx, p, pmes)
	if err != nil {
		logger.Debugf("putValueToPeer: %v. (peer: %s, key: %s)", err, p.Pretty(), loggableKey(string(rec.Key)))
		return err
	}

	if !bytes.Equal(rpmes.GetRecord().Value, pmes.GetRecord().Value) {
		logger.Warningf("putValueToPeer: value not put correctly. (%v != %v)", pmes, rpmes)
		err = errors.New("value not put correctly")
		return err
	}

	return nil
}

var errInvalidRecord = errors.New("received invalid record")

// getValueOrPeers queries a particular peer p for the value for
// key. It returns either the value or a list of closer peers.
// NOTE: It will update the dht's peerstore with any new addresses
// it finds for the given peer.
func (dht *IpfsDHT) getValueOrPeers(ctx context.Context, p peer.ID, key string) (*recpb.Record, []*peer.AddrInfo, error) {
	var err error
	sp, ctx := opentracing.StartSpanFromContext(ctx, "dht.get_value_or_peers", opentracing.Tags{
		"target.peer_id": p,
		"dht.record_key": loggableKey(key),
	})
	defer LinkedFinish(sp, &err)

	pmes, err := dht.getValueSingle(ctx, p, key)
	if err != nil {
		return nil, nil, err
	}

	var (
		peers  = pb.PBPeersToPeerInfos(pmes.GetCloserPeers())
		record = pmes.GetRecord()
	)

	// We have a value.
	if record != nil {
		logger.Debug("getValueOrPeers: got value")

		// Is the record valid?
		err = dht.Validator.Validate(string(record.GetKey()), record.GetValue())
		if err != nil {
			logger.Info("Received invalid record! (discarded)")
			// return a sentinel to signify an invalid record was received.
			err = errInvalidRecord
			record = new(recpb.Record)
		}
		return record, peers, err
	}

	if l := len(peers); l > 0 {
		sp.LogFields(log.Int("dht.closer_peers", l))
		logger.Debug("getValueOrPeers: peers")
		return nil, peers, nil
	}

	logger.Warning("getValueOrPeers: routing.ErrNotFound")
	return nil, nil, routing.ErrNotFound
}

// getValueSingle simply performs the get value RPC with the given parameters
func (dht *IpfsDHT) getValueSingle(ctx context.Context, p peer.ID, key string) (*pb.Message, error) {
	var err error
	sp, ctx := opentracing.StartSpanFromContext(ctx, "dht.get_value_single", opentracing.Tags{
		"peer.id":        p,
		"dht.record_key": loggableKey(key),
	})
	defer LinkedFinish(sp, &err)

	pmes := pb.NewMessage(pb.Message_GET_VALUE, []byte(key), 0)
	resp, err := dht.sendRequest(ctx, p, pmes)
	switch err {
	case nil:
		return resp, nil
	case ErrReadTimeout:
		logger.Warningf("getValueSingle: read timeout %s %s", p.Pretty(), key)
		fallthrough
	default:
		return nil, err
	}
}

// Update signals the routingTable to Update its last-seen status
// on the given peer.
func (dht *IpfsDHT) Update(ctx context.Context, p peer.ID) {
	evicted, err := dht.routingTable.Update(p)

	sp := opentracing.SpanFromContext(ctx)
	if sp == nil {
		return
	}

	fields := []log.Field{
		log.String("component", "routing table"),
		log.String("event", "routing table updated"),
	}

	if err != nil {
		fields = append(fields, log.Error(err))
	} else if evicted == "" {
		fields = append(fields, log.Bool("evicted", false))
	} else {
		fields = append(fields, log.Bool("evicted", true), log.String("evicted.peer_id", p.Pretty()))
	}

	sp.LogFields(fields...)
}

// FindLocal looks for a peer with a given ID connected to this dht and returns
// the peer and the table it was found in.
func (dht *IpfsDHT) FindLocal(id peer.ID) peer.AddrInfo {
	switch dht.host.Network().Connectedness(id) {
	case network.Connected, network.CanConnect:
		return dht.peerstore.PeerInfo(id)
	default:
		return peer.AddrInfo{}
	}
}

// findPeerSingle asks peer 'p' if they know where the peer with id 'id' is
func (dht *IpfsDHT) findPeerSingle(ctx context.Context, p peer.ID, id peer.ID) (*pb.Message, error) {
	var err error
	sp, ctx := opentracing.StartSpanFromContext(ctx, "dht.find_peer_single", opentracing.Tags{
		"peer.id":   p,
		"target.id": id,
	})
	defer LinkedFinish(sp, &err)

	pmes := pb.NewMessage(pb.Message_FIND_NODE, []byte(id), 0)
	resp, err := dht.sendRequest(ctx, p, pmes)
	switch err {
	case nil:
		return resp, nil
	case ErrReadTimeout:
		logger.Warningf("read timeout: %s %s", p.Pretty(), id)
		fallthrough
	default:
		return nil, err
	}
}

func (dht *IpfsDHT) findProvidersSingle(ctx context.Context, p peer.ID, key cid.Cid) (*pb.Message, error) {
	var err error
	sp, ctx := opentracing.StartSpanFromContext(ctx, "dht.find_providers_single", opentracing.Tags{
		"peer.id":   p,
		"target.id": key.String(),
	})
	defer LinkedFinish(sp, &err)

	pmes := pb.NewMessage(pb.Message_GET_PROVIDERS, key.Bytes(), 0)
	resp, err := dht.sendRequest(ctx, p, pmes)
	switch err {
	case nil:
		return resp, nil
	case ErrReadTimeout:
		logger.Warningf("read timeout: %s %s", p.Pretty(), key)
		fallthrough
	default:
		return nil, err
	}
}

// betterPeersToQuery returns the nearest peers in the routing table, but if and
// only if closer than self.
func (dht *IpfsDHT) betterPeersToQuery(pmes *pb.Message, target peer.ID, count int) []peer.ID {
	var (
		key    = kb.ConvertKey(string(pmes.GetKey()))
		closer = dht.routingTable.NearestPeers(key, count)
	)

	if closer == nil {
		logger.Warning("betterPeersToQuery: no closer peers to send:", target)
		return nil
	}

	filtered := make([]peer.ID, 0, len(closer))
	for _, p := range closer {
		switch p {
		case dht.self:
			// == to self? thats bad
			logger.Error("betterPeersToQuery: attempted to return self; this shouldn't happen.")
			return nil

		case target:
			continue

		default:
			filtered = append(filtered, p)
		}
	}

	return filtered
}

// Context returns the DHT context.Context.
func (dht *IpfsDHT) Context() context.Context {
	return dht.ctx
}

// Process returns DHT goprocess.Process.
func (dht *IpfsDHT) Process() goprocess.Process {
	return dht.proc
}

// RoutingTable returns the DHT's kbucket.RoutingTable.
func (dht *IpfsDHT) RoutingTable() *kb.RoutingTable {
	return dht.routingTable
}

// Close calls goprocess.Process Close.
func (dht *IpfsDHT) Close() error {
	return dht.proc.Close()
}

func (dht *IpfsDHT) protocolStrs() []string {
	pstrs := make([]string, len(dht.protocols))
	for idx, proto := range dht.protocols {
		pstrs[idx] = string(proto)
	}

	return pstrs
}

func mkDsKey(s string) ds.Key {
	return ds.NewKey(base32.RawStdEncoding.EncodeToString([]byte(s)))
}

func (dht *IpfsDHT) PeerID() peer.ID {
	return dht.self
}

func (dht *IpfsDHT) PeerKey() []byte {
	return kb.ConvertPeerID(dht.self)
}

func (dht *IpfsDHT) Host() host.Host {
	return dht.host
}

func (dht *IpfsDHT) Ping(ctx context.Context, p peer.ID) error {
	var err error
	sp, ctx := opentracing.StartSpanFromContext(ctx, "dht.ping.out", opentracing.Tags{
		"remote.peer_id": p.Pretty(),
	})
	defer LinkedFinish(sp, &err)

	req := pb.NewMessage(pb.Message_PING, nil, 0)
	resp, err := dht.sendRequest(ctx, p, req)
	if err != nil {
		err = fmt.Errorf("sending request: %w", err)
		return err
	}
	if resp.Type != pb.Message_PING {
		err = fmt.Errorf("got unexpected response type: %v", resp.Type)
		return err
	}
	return nil
}

// newContextWithLocalTags returns a new context.Context with the InstanceID and
// PeerID keys populated. It will also take any extra tags that need adding to
// the context as tag.Mutators.
func (dht *IpfsDHT) newContextWithLocalTags(ctx context.Context, extraTags ...tag.Mutator) context.Context {
	extraTags = append(
		extraTags,
		tag.Upsert(metrics.KeyPeerID, dht.self.Pretty()),
		tag.Upsert(metrics.KeyInstanceID, fmt.Sprintf("%p", dht)),
	)
	ctx, _ = tag.New(
		ctx,
		extraTags...,
	) // ignoring error as it is unrelated to the actual function of this code.
	return ctx
}
