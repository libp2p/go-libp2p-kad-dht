package dht

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/peerstore"
	"github.com/libp2p/go-libp2p-core/protocol"
	"github.com/libp2p/go-libp2p-core/routing"

	"go.opencensus.io/tag"

	"github.com/libp2p/go-libp2p-kad-dht/metrics"
	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
	"github.com/libp2p/go-libp2p-kad-dht/providers"
	kb "github.com/libp2p/go-libp2p-kbucket"
	record "github.com/libp2p/go-libp2p-record"
	recpb "github.com/libp2p/go-libp2p-record/pb"

	"github.com/gogo/protobuf/proto"
	ds "github.com/ipfs/go-datastore"
	logging "github.com/ipfs/go-log"
	"github.com/jbenet/goprocess"
	goprocessctx "github.com/jbenet/goprocess/context"
	"github.com/multiformats/go-base32"
	"github.com/multiformats/go-multihash"
)

var logger = logging.Logger("dht")

const BaseConnMgrScore = 5

type mode int

const (
	modeServer mode = iota + 1
	modeClient
)

const (
	kad1 protocol.ID = "/kad/1.0.0"
	kad2 protocol.ID = "/kad/2.0.0"
)

// KadDHT is an implementation of Kademlia with S/Kademlia modifications.
// It is used to implement the base Routing module.
type KadDHT struct {
	host      host.Host           // the network services we need
	self      peer.ID             // Local peer (yourself)
	peerstore peerstore.Peerstore // Peer Registry

	datastore ds.Datastore // Local data

	routingTable *kb.RoutingTable // Array of routing tables for differently distanced nodes
	// ProviderManager stores & manages the provider records for this Dht peer.
	ProviderManager *providers.ProviderManager

	birth time.Time // When this peer started up

	Validator record.Validator

	ctx  context.Context
	proc goprocess.Process

	strmap map[peer.ID]*messageSender
	smlk   sync.Mutex

	plk sync.Mutex

	stripedPutLocks [256]sync.Mutex

	// DHT protocols we query with. We'll only add peers to our routing
	// table if they speak these protocols.
	protocols []protocol.ID

	// DHT protocols we can respond to.
	serverProtocols []protocol.ID

	auto   bool
	mode   mode
	modeLk sync.Mutex

	bucketSize int
	alpha      int // The concurrency parameter per path
	beta       int // The number of peers closest to a target that must have responded for a query path to terminate

	queryPeerFilter        QueryFilterFunc
	routingTablePeerFilter RouteTableFilterFunc

	autoRefresh           bool
	rtRefreshQueryTimeout time.Duration
	rtRefreshInterval     time.Duration
	triggerRtRefresh      chan chan<- error
	triggerSelfLookup     chan chan<- error

	maxRecordAge time.Duration

	// Allows disabling dht subsystems. These should _only_ be set on
	// "forked" DHTs (e.g., DHTs with custom protocols and/or private
	// networks).
	enableProviders, enableValues bool

	// maxLastSuccessfulOutboundThreshold is the max threshold/upper limit for the value of "lastSuccessfulOutboundQuery"
	// of the peer in the bucket above which we will evict it to make place for a new peer if the bucket
	// is full
	maxLastSuccessfulOutboundThreshold time.Duration

	fixLowPeersChan chan struct{}
}

// Assert that IPFS assumptions about interfaces aren't broken. These aren't a
// guarantee, but we can use them to aid refactoring.
var (
	_ routing.ContentRouting = (*KadDHT)(nil)
	_ routing.Routing        = (*KadDHT)(nil)
	_ routing.PeerRouting    = (*KadDHT)(nil)
	_ routing.PubKeyFetcher  = (*KadDHT)(nil)
	_ routing.ValueStore     = (*KadDHT)(nil)
)

// New creates a new DHT with the specified host and options.
// Please note that being connected to a DHT peer does not necessarily imply that it's also in the DHT Routing Table.
// If the Routing Table has more than "minRTRefreshThreshold" peers, we consider a peer as a Routing Table candidate ONLY when
// we successfully get a query response from it OR if it send us a query.
func New(ctx context.Context, h host.Host, options ...Option) (*KadDHT, error) {
	var cfg config
	if err := cfg.apply(append([]Option{defaults}, options...)...); err != nil {
		return nil, err
	}
	if err := cfg.applyFallbacks(h); err != nil {
		return nil, err
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}
	dht, err := makeDHT(ctx, h, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create DHT, err=%s", err)
	}

	dht.autoRefresh = cfg.routingTable.autoRefresh
	dht.rtRefreshInterval = cfg.routingTable.refreshInterval
	dht.rtRefreshQueryTimeout = cfg.routingTable.refreshQueryTimeout

	dht.maxRecordAge = cfg.maxRecordAge
	dht.enableProviders = cfg.enableProviders
	dht.enableValues = cfg.enableValues

	dht.Validator = cfg.validator

	switch cfg.mode {
	case ModeAuto:
		dht.auto = true
		dht.mode = modeClient
	case ModeClient:
		dht.auto = false
		dht.mode = modeClient
	case ModeServer:
		dht.auto = false
		dht.mode = modeServer
	default:
		return nil, fmt.Errorf("invalid dht mode %d", cfg.mode)
	}

	if dht.mode == modeServer {
		if err := dht.moveToServerMode(); err != nil {
			return nil, err
		}
	}

	// register for event bus and network notifications
	sn, err := newSubscriberNotifiee(dht)
	if err != nil {
		return nil, err
	}
	dht.proc.Go(sn.subscribe)
	// handle providers
	dht.proc.AddChild(dht.ProviderManager.Process())

	dht.startSelfLookup()
	dht.startRefreshing()

	// go-routine to make sure we ALWAYS have RT peer addresses in the peerstore
	// since RT membership is decoupled from connectivity
	go dht.persistRTPeersInPeerStore()

	// listens to the fix low peers chan and tries to fix the Routing Table
	dht.proc.Go(dht.fixLowPeersRoutine)

	return dht, nil
}

// NewDHT creates a new DHT object with the given peer as the 'local' host.
// IpfsDHT's initialized with this function will respond to DHT requests,
// whereas IpfsDHT's initialized with NewDHTClient will not.
func NewDHT(ctx context.Context, h host.Host, dstore ds.Batching) *KadDHT {
	dht, err := New(ctx, h, Datastore(dstore))
	if err != nil {
		panic(err)
	}
	return dht
}

// NewDHTClient creates a new DHT object with the given peer as the 'local'
// host. IpfsDHT clients initialized with this function will not respond to DHT
// requests. If you need a peer to respond to DHT requests, use NewDHT instead.
// NewDHTClient creates a new DHT object with the given peer as the 'local' host
func NewDHTClient(ctx context.Context, h host.Host, dstore ds.Batching) *KadDHT {
	dht, err := New(ctx, h, Datastore(dstore), Mode(ModeClient))
	if err != nil {
		panic(err)
	}
	return dht
}

func makeDHT(ctx context.Context, h host.Host, cfg config) (*KadDHT, error) {
	var protocols, serverProtocols []protocol.ID

	// check if custom test protocols were set
	if cfg.v1CompatibleMode {
		// In compat mode, query/serve using the old protocol.
		//
		// DO NOT accept requests on the new protocol. Otherwise:
		// 1. We'll end up in V2 routing tables.
		// 2. We'll have V1 peers in our routing table.
		//
		// In other words, we'll pollute the V2 network.
		protocols = []protocol.ID{cfg.protocolPrefix + kad1}
		serverProtocols = []protocol.ID{cfg.protocolPrefix + kad1}
	} else {
		// In v2 mode, serve on both protocols, but only
		// query/accept peers in v2 mode.
		protocols = []protocol.ID{cfg.protocolPrefix + kad2}
		serverProtocols = []protocol.ID{cfg.protocolPrefix + kad2, cfg.protocolPrefix + kad1}
	}

	dht := &KadDHT{
		datastore:              cfg.datastore,
		self:                   h.ID(),
		peerstore:              h.Peerstore(),
		host:                   h,
		strmap:                 make(map[peer.ID]*messageSender),
		birth:                  time.Now(),
		protocols:              protocols,
		serverProtocols:        serverProtocols,
		bucketSize:             cfg.bucketSize,
		alpha:                  cfg.concurrency,
		beta:                   cfg.resiliency,
		triggerRtRefresh:       make(chan chan<- error),
		triggerSelfLookup:      make(chan chan<- error),
		queryPeerFilter:        cfg.queryPeerFilter,
		routingTablePeerFilter: cfg.routingTable.peerFilter,
		fixLowPeersChan:        make(chan struct{}),
	}

	// construct routing table
	rt, err := makeRoutingTable(dht, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to construct routing table,err=%s", err)
	}
	dht.routingTable = rt

	// create a DHT proc with the given context
	dht.proc = goprocessctx.WithContext(ctx)

	// create a tagged context derived from the original context
	ctxTags := dht.newContextWithLocalTags(ctx)
	// the DHT context should be done when the process is closed
	dht.ctx = goprocessctx.WithProcessClosing(ctxTags, dht.proc)

	dht.ProviderManager = providers.NewProviderManager(dht.ctx, h.ID(), cfg.datastore)

	return dht, nil
}

func makeRoutingTable(dht *KadDHT, cfg config) (*kb.RoutingTable, error) {
	// The threshold is calculated based on the expected amount of time that should pass before we
	// query a peer as part of our refresh cycle.
	// To grok the Math Wizardy that produced these exact equations, please be patient as a document explaining it will
	// be published soon.
	l1 := math.Log(float64(1) / float64(defaultBucketSize))                              //(Log(1/K))
	l2 := math.Log(float64(1) - (float64(cfg.concurrency) / float64(defaultBucketSize))) // Log(1 - (alpha / K))
	maxLastSuccessfulOutboundThreshold := l1 / l2 * float64(cfg.routingTable.refreshInterval)

	self := kb.ConvertPeerID(dht.host.ID())

	rt, err := kb.NewRoutingTable(cfg.bucketSize, self, time.Minute, dht.host.Peerstore(), maxLastSuccessfulOutboundThreshold)
	cmgr := dht.host.ConnManager()

	rt.PeerAdded = func(p peer.ID) {
		commonPrefixLen := kb.CommonPrefixLen(self, kb.ConvertPeerID(p))
		cmgr.TagPeer(p, "kbucket", BaseConnMgrScore+commonPrefixLen)
	}
	rt.PeerRemoved = func(p peer.ID) {
		cmgr.UntagPeer(p, "kbucket")

		// try to fix the RT
		dht.fixRTIfNeeded()
	}

	return rt, err
}

// fixLowPeers tries to get more peers into the routing table if we're below the threshold
func (dht *KadDHT) fixLowPeersRoutine(proc goprocess.Process) {
	for {
		select {
		case <-dht.fixLowPeersChan:
		case <-proc.Closing():
			return
		}
		if dht.routingTable.Size() > minRTRefreshThreshold {
			continue
		}

		for _, p := range dht.host.Network().Peers() {
			dht.peerFound(dht.Context(), p, false)
		}

		if dht.autoRefresh {
			select {
			case dht.triggerRtRefresh <- nil:
			default:
			}
		}
	}

}

// TODO This is hacky, horrible and the programmer needs to have his mother called a hamster.
// SHOULD be removed once https://github.com/libp2p/go-libp2p/issues/800 goes in.
func (dht *KadDHT) persistRTPeersInPeerStore() {
	tickr := time.NewTicker(peerstore.RecentlyConnectedAddrTTL / 3)
	defer tickr.Stop()

	for {
		select {
		case <-tickr.C:
			ps := dht.routingTable.ListPeers()
			for _, p := range ps {
				dht.peerstore.UpdateAddrs(p, peerstore.RecentlyConnectedAddrTTL, peerstore.RecentlyConnectedAddrTTL)
			}
		case <-dht.ctx.Done():
			return
		}
	}
}

// putValueToPeer stores the given key/value pair at the peer 'p'
func (dht *KadDHT) putValueToPeer(ctx context.Context, p peer.ID, rec *recpb.Record) error {
	pmes := pb.NewMessage(pb.Message_PUT_VALUE, rec.Key, 0)
	pmes.Record = rec
	rpmes, err := dht.sendRequest(ctx, p, pmes)
	if err != nil {
		logger.Debugw("failed to put value to peer", "to", p, "key", loggableKeyBytes(rec.Key), "error", err)
		return err
	}

	if !bytes.Equal(rpmes.GetRecord().Value, pmes.GetRecord().Value) {
		logger.Infow("value not put correctly", "put-message", pmes, "get-message", rpmes)
		return errors.New("value not put correctly")
	}

	return nil
}

var errInvalidRecord = errors.New("received invalid record")

// getValueOrPeers queries a particular peer p for the value for
// key. It returns either the value or a list of closer peers.
// NOTE: It will update the dht's peerstore with any new addresses
// it finds for the given peer.
func (dht *KadDHT) getValueOrPeers(ctx context.Context, p peer.ID, key string) (*recpb.Record, []*peer.AddrInfo, error) {
	pmes, err := dht.getValueSingle(ctx, p, key)
	if err != nil {
		return nil, nil, err
	}

	// Perhaps we were given closer peers
	peers := pb.PBPeersToPeerInfos(pmes.GetCloserPeers())

	if record := pmes.GetRecord(); record != nil {
		// Success! We were given the value
		logger.Debug("got value")

		// make sure record is valid.
		err = dht.Validator.Validate(string(record.GetKey()), record.GetValue())
		if err != nil {
			logger.Debug("received invalid record (discarded)")
			// return a sentinal to signify an invalid record was received
			err = errInvalidRecord
			record = new(recpb.Record)
		}
		return record, peers, err
	}

	if len(peers) > 0 {
		return nil, peers, nil
	}

	return nil, nil, routing.ErrNotFound
}

// getValueSingle simply performs the get value RPC with the given parameters
func (dht *KadDHT) getValueSingle(ctx context.Context, p peer.ID, key string) (*pb.Message, error) {
	pmes := pb.NewMessage(pb.Message_GET_VALUE, []byte(key), 0)
	return dht.sendRequest(ctx, p, pmes)
}

// getLocal attempts to retrieve the value from the datastore
func (dht *KadDHT) getLocal(key string) (*recpb.Record, error) {
	logger.Debugw("finding value in datastore", "key", loggableKeyString(key))

	rec, err := dht.getRecordFromDatastore(mkDsKey(key))
	if err != nil {
		logger.Warnw("get local failed", "key", key, "error", err)
		return nil, err
	}

	// Double check the key. Can't hurt.
	if rec != nil && string(rec.GetKey()) != key {
		logger.Errorw("BUG: found a DHT record that didn't match it's key", "expected", key, "got", rec.GetKey())
		return nil, nil

	}
	return rec, nil
}

// putLocal stores the key value pair in the datastore
func (dht *KadDHT) putLocal(key string, rec *recpb.Record) error {
	data, err := proto.Marshal(rec)
	if err != nil {
		logger.Warnw("failed to put marshal record for local put", "error", err, "key", key)
		return err
	}

	return dht.datastore.Put(mkDsKey(key), data)
}

// peerFound signals the routingTable that we've found a peer that
// might support the DHT protocol.
func (dht *KadDHT) peerFound(ctx context.Context, p peer.ID, queryPeer bool) {
	logger.Debugw("peer found", "peer", p)
	b, err := dht.validRTPeer(p)
	if err != nil {
		logger.Errorw("failed to validate if peer is a DHT peer", "peer", p, "error", err)
	} else if b {
		_, err := dht.routingTable.TryAddPeer(p, queryPeer)
		if err != nil {
			// peer not added.
			return
		}

		// If we discovered the peer because of a query, we need to ensure we override the "zero" lastSuccessfulOutboundQuery
		// value that must have been set in the Routing Table for this peer when it was first added during a connection.
		if queryPeer {
			dht.routingTable.UpdateLastSuccessfulOutboundQuery(p, time.Now())
		}
	}
}

// peerStoppedDHT signals the routing table that a peer is unable to responsd to DHT queries anymore.
func (dht *KadDHT) peerStoppedDHT(ctx context.Context, p peer.ID) {
	logger.Debugw("peer stopped dht", "peer", p)
	// A peer that does not support the DHT protocol is dead for us.
	// There's no point in talking to anymore till it starts supporting the DHT protocol again.
	dht.routingTable.RemovePeer(p)

	// since we lost a peer from the RT, we should do this here
	dht.fixRTIfNeeded()
}

func (dht *KadDHT) fixRTIfNeeded() {
	select {
	case dht.fixLowPeersChan <- struct{}{}:
	default:
	}
}

// FindLocal looks for a peer with a given ID connected to this dht and returns the peer and the table it was found in.
func (dht *KadDHT) FindLocal(id peer.ID) peer.AddrInfo {
	switch dht.host.Network().Connectedness(id) {
	case network.Connected, network.CanConnect:
		return dht.peerstore.PeerInfo(id)
	default:
		return peer.AddrInfo{}
	}
}

// findPeerSingle asks peer 'p' if they know where the peer with id 'id' is
func (dht *KadDHT) findPeerSingle(ctx context.Context, p peer.ID, id peer.ID) (*pb.Message, error) {
	pmes := pb.NewMessage(pb.Message_FIND_NODE, []byte(id), 0)
	return dht.sendRequest(ctx, p, pmes)
}

func (dht *KadDHT) findProvidersSingle(ctx context.Context, p peer.ID, key multihash.Multihash) (*pb.Message, error) {
	pmes := pb.NewMessage(pb.Message_GET_PROVIDERS, key, 0)
	return dht.sendRequest(ctx, p, pmes)
}

// nearestPeersToQuery returns the routing tables closest peers.
func (dht *KadDHT) nearestPeersToQuery(pmes *pb.Message, count int) []peer.ID {
	closer := dht.routingTable.NearestPeers(kb.ConvertKey(string(pmes.GetKey())), count)
	return closer
}

// betterPeersToQuery returns nearestPeersToQuery with some additional filtering
func (dht *KadDHT) betterPeersToQuery(pmes *pb.Message, from peer.ID, count int) []peer.ID {
	closer := dht.nearestPeersToQuery(pmes, count)

	// no node? nil
	if closer == nil {
		logger.Infow("no closer peers to send", from)
		return nil
	}

	filtered := make([]peer.ID, 0, len(closer))
	for _, clp := range closer {

		// == to self? thats bad
		if clp == dht.self {
			logger.Error("BUG betterPeersToQuery: attempted to return self! this shouldn't happen...")
			return nil
		}
		// Dont send a peer back themselves
		if clp == from {
			continue
		}

		filtered = append(filtered, clp)
	}

	// ok seems like closer nodes
	return filtered
}

func (dht *KadDHT) setMode(m mode) error {
	dht.modeLk.Lock()
	defer dht.modeLk.Unlock()

	if m == dht.mode {
		return nil
	}

	switch m {
	case modeServer:
		return dht.moveToServerMode()
	case modeClient:
		return dht.moveToClientMode()
	default:
		return fmt.Errorf("unrecognized dht mode: %d", m)
	}
}

// moveToServerMode advertises (via libp2p identify updates) that we are able to respond to DHT queries and sets the appropriate stream handlers.
// Note: We may support responding to queries with protocols aside from our primary ones in order to support
// interoperability with older versions of the DHT protocol.
func (dht *KadDHT) moveToServerMode() error {
	dht.mode = modeServer
	for _, p := range dht.serverProtocols {
		dht.host.SetStreamHandler(p, dht.handleNewStream)
	}
	return nil
}

// moveToClientMode stops advertising (and rescinds advertisements via libp2p identify updates) that we are able to
// respond to DHT queries and removes the appropriate stream handlers. We also kill all inbound streams that were
// utilizing the handled protocols.
// Note: We may support responding to queries with protocols aside from our primary ones in order to support
// interoperability with older versions of the DHT protocol.
func (dht *KadDHT) moveToClientMode() error {
	dht.mode = modeClient
	for _, p := range dht.serverProtocols {
		dht.host.RemoveStreamHandler(p)
	}

	pset := make(map[protocol.ID]bool)
	for _, p := range dht.serverProtocols {
		pset[p] = true
	}

	for _, c := range dht.host.Network().Conns() {
		for _, s := range c.GetStreams() {
			if pset[s.Protocol()] {
				if s.Stat().Direction == network.DirInbound {
					_ = s.Reset()
				}
			}
		}
	}
	return nil
}

func (dht *KadDHT) getMode() mode {
	dht.modeLk.Lock()
	defer dht.modeLk.Unlock()
	return dht.mode
}

// Context return dht's context
func (dht *KadDHT) Context() context.Context {
	return dht.ctx
}

// Process return dht's process
func (dht *KadDHT) Process() goprocess.Process {
	return dht.proc
}

// RoutingTable return dht's routingTable
func (dht *KadDHT) RoutingTable() *kb.RoutingTable {
	return dht.routingTable
}

// Close calls Process Close
func (dht *KadDHT) Close() error {
	return dht.proc.Close()
}

func mkDsKey(s string) ds.Key {
	return ds.NewKey(base32.RawStdEncoding.EncodeToString([]byte(s)))
}

func (dht *KadDHT) PeerID() peer.ID {
	return dht.self
}

func (dht *KadDHT) PeerKey() []byte {
	return kb.ConvertPeerID(dht.self)
}

func (dht *KadDHT) Host() host.Host {
	return dht.host
}

func (dht *KadDHT) Ping(ctx context.Context, p peer.ID) error {
	req := pb.NewMessage(pb.Message_PING, nil, 0)
	resp, err := dht.sendRequest(ctx, p, req)
	if err != nil {
		return fmt.Errorf("sending request: %w", err)
	}
	if resp.Type != pb.Message_PING {
		return fmt.Errorf("got unexpected response type: %v", resp.Type)
	}
	return nil
}

// newContextWithLocalTags returns a new context.Context with the InstanceID and
// PeerID keys populated. It will also take any extra tags that need adding to
// the context as tag.Mutators.
func (dht *KadDHT) newContextWithLocalTags(ctx context.Context, extraTags ...tag.Mutator) context.Context {
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
