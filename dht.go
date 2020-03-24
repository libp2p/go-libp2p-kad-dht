package dht

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/peerstore"
	"github.com/libp2p/go-libp2p-core/protocol"
	"github.com/libp2p/go-libp2p-core/routing"

	"go.opencensus.io/tag"
	"golang.org/x/xerrors"

	"github.com/libp2p/go-libp2p-kad-dht/metrics"
	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
	"github.com/libp2p/go-libp2p-kad-dht/providers"

	"github.com/gogo/protobuf/proto"
	ds "github.com/ipfs/go-datastore"
	logging "github.com/ipfs/go-log"
	"github.com/jbenet/goprocess"
	goprocessctx "github.com/jbenet/goprocess/context"
	kb "github.com/libp2p/go-libp2p-kbucket"
	record "github.com/libp2p/go-libp2p-record"
	recpb "github.com/libp2p/go-libp2p-record/pb"
	"github.com/multiformats/go-base32"
	"github.com/multiformats/go-multihash"
)

var logger = logging.Logger("dht")
var rtPvLogger = logging.Logger("dht/rt-validation")

const BaseConnMgrScore = 5

type mode int

const (
	modeServer mode = 1
	modeClient      = 2
)

const (
	kad1 protocol.ID = "/kad/1.0.0"
	kad2 protocol.ID = "/kad/2.0.0"
)

// IpfsDHT is an implementation of Kademlia with S/Kademlia modifications.
// It is used to implement the base Routing module.
type IpfsDHT struct {
	host      host.Host           // the network services we need
	self      peer.ID             // Local peer (yourself)
	peerstore peerstore.Peerstore // Peer Registry

	datastore ds.Datastore // Local data

	routingTable *kb.RoutingTable // Array of routing tables for differently distanced nodes
	providers    *providers.ProviderManager

	birth time.Time  // When this peer started up
	rng   *rand.Rand // Source of randomness
	rnglk sync.Mutex // Rand does not support concurrency

	Validator record.Validator

	ctx  context.Context
	proc goprocess.Process

	strmap map[peer.ID]*messageSender
	smlk   sync.Mutex

	plk sync.Mutex

	stripedPutLocks [256]sync.Mutex

	// Primary DHT protocols - we query and respond to these protocols
	protocols []protocol.ID

	// DHT protocols we can respond to (may contain protocols in addition to the primary protocols)
	serverProtocols []protocol.ID

	auto   bool
	mode   mode
	modeLk sync.Mutex

	bucketSize int
	alpha      int // The concurrency parameter per path
	beta       int // The number of peers closest to a target that must have responded for a query path to terminate

	autoRefresh           bool
	rtRefreshQueryTimeout time.Duration
	rtRefreshPeriod       time.Duration
	triggerRtRefresh      chan chan<- error
	triggerSelfLookup     chan chan<- error

	maxRecordAge time.Duration

	// Allows disabling dht subsystems. These should _only_ be set on
	// "forked" DHTs (e.g., DHTs with custom protocols and/or private
	// networks).
	enableProviders, enableValues bool
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
func New(ctx context.Context, h host.Host, options ...Option) (*IpfsDHT, error) {
	var cfg config
	if err := cfg.apply(append([]Option{defaults}, options...)...); err != nil {
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
	dht.rtRefreshPeriod = cfg.routingTable.refreshPeriod
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
	dht.proc.AddChild(dht.providers.Process())

	dht.startSelfLookup()
	dht.startRefreshing()
	return dht, nil
}

// NewDHT creates a new DHT object with the given peer as the 'local' host.
// IpfsDHT's initialized with this function will respond to DHT requests,
// whereas IpfsDHT's initialized with NewDHTClient will not.
func NewDHT(ctx context.Context, h host.Host, dstore ds.Batching) *IpfsDHT {
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
func NewDHTClient(ctx context.Context, h host.Host, dstore ds.Batching) *IpfsDHT {
	dht, err := New(ctx, h, Datastore(dstore), Mode(ModeClient))
	if err != nil {
		panic(err)
	}
	return dht
}

func makeDHT(ctx context.Context, h host.Host, cfg config) (*IpfsDHT, error) {

	protocols := []protocol.ID{cfg.protocolPrefix + kad2}
	serverProtocols := []protocol.ID{cfg.protocolPrefix + kad2, cfg.protocolPrefix + kad1}

	// check if custom test protocols were set
	if len(cfg.testProtocols) > 0 {
		protocols = make([]protocol.ID, len(cfg.testProtocols))
		serverProtocols = make([]protocol.ID, len(cfg.testProtocols))
		for i, p := range cfg.testProtocols {
			protocols[i] = cfg.protocolPrefix + p
			serverProtocols[i] = cfg.protocolPrefix + p
		}
	}

	dht := &IpfsDHT{
		datastore:         cfg.datastore,
		self:              h.ID(),
		peerstore:         h.Peerstore(),
		host:              h,
		strmap:            make(map[peer.ID]*messageSender),
		birth:             time.Now(),
		rng:               rand.New(rand.NewSource(rand.Int63())),
		protocols:         protocols,
		serverProtocols:   serverProtocols,
		bucketSize:        cfg.bucketSize,
		alpha:             cfg.concurrency,
		beta:              cfg.resiliency,
		triggerRtRefresh:  make(chan chan<- error),
		triggerSelfLookup: make(chan chan<- error),
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

	dht.providers = providers.NewProviderManager(dht.ctx, h.ID(), cfg.datastore)

	return dht, nil
}

func makeRoutingTable(dht *IpfsDHT, cfg config) (*kb.RoutingTable, error) {
	self := kb.ConvertPeerID(dht.host.ID())
	// construct the routing table with a peer validation function
	pvF := func(c context.Context, p peer.ID) bool {
		// connect should work
		if err := dht.host.Connect(c, peer.AddrInfo{ID: p}); err != nil {
			rtPvLogger.Infof("failed to connect to peer %s for validation, err=%s", p, err)
			return false
		}

		// peer should support the DHT protocol
		b, err := dht.validRTPeer(p)
		if err != nil {
			rtPvLogger.Errorf("failed to check if peer %s supports DHT protocol, err=%s", p, err)
			return false
		}

		return b
	}

	rtOpts := []kb.Option{kb.PeerValidationFnc(pvF)}
	if !(cfg.routingTable.checkInterval == 0) {
		rtOpts = append(rtOpts, kb.TableCleanupInterval(cfg.routingTable.checkInterval))
	}

	rt, err := kb.NewRoutingTable(cfg.bucketSize, self, time.Minute, dht.host.Peerstore(),
		rtOpts...)
	cmgr := dht.host.ConnManager()

	rt.PeerAdded = func(p peer.ID) {
		commonPrefixLen := kb.CommonPrefixLen(self, kb.ConvertPeerID(p))
		cmgr.TagPeer(p, "kbucket", BaseConnMgrScore+commonPrefixLen)
	}
	rt.PeerRemoved = func(p peer.ID) {
		cmgr.UntagPeer(p, "kbucket")
	}

	return rt, err
}

// putValueToPeer stores the given key/value pair at the peer 'p'
func (dht *IpfsDHT) putValueToPeer(ctx context.Context, p peer.ID, rec *recpb.Record) error {
	pmes := pb.NewMessage(pb.Message_PUT_VALUE, rec.Key, 0)
	pmes.Record = rec
	rpmes, err := dht.sendRequest(ctx, p, pmes)
	if err != nil {
		logger.Debugf("putValueToPeer: %v. (peer: %s, key: %s)", err, p.Pretty(), loggableKey(string(rec.Key)))
		return err
	}

	if !bytes.Equal(rpmes.GetRecord().Value, pmes.GetRecord().Value) {
		logger.Warningf("putValueToPeer: value not put correctly. (%v != %v)", pmes, rpmes)
		return errors.New("value not put correctly")
	}

	return nil
}

var errInvalidRecord = errors.New("received invalid record")

// getValueOrPeers queries a particular peer p for the value for
// key. It returns either the value or a list of closer peers.
// NOTE: It will update the dht's peerstore with any new addresses
// it finds for the given peer.
func (dht *IpfsDHT) getValueOrPeers(ctx context.Context, p peer.ID, key string) (*recpb.Record, []*peer.AddrInfo, error) {
	pmes, err := dht.getValueSingle(ctx, p, key)
	if err != nil {
		return nil, nil, err
	}

	// Perhaps we were given closer peers
	peers := pb.PBPeersToPeerInfos(pmes.GetCloserPeers())

	if record := pmes.GetRecord(); record != nil {
		// Success! We were given the value
		logger.Debug("getValueOrPeers: got value")

		// make sure record is valid.
		err = dht.Validator.Validate(string(record.GetKey()), record.GetValue())
		if err != nil {
			logger.Info("Received invalid record! (discarded)")
			// return a sentinal to signify an invalid record was received
			err = errInvalidRecord
			record = new(recpb.Record)
		}
		return record, peers, err
	}

	if len(peers) > 0 {
		logger.Debug("getValueOrPeers: peers")
		return nil, peers, nil
	}

	logger.Warning("getValueOrPeers: routing.ErrNotFound")
	return nil, nil, routing.ErrNotFound
}

// getValueSingle simply performs the get value RPC with the given parameters
func (dht *IpfsDHT) getValueSingle(ctx context.Context, p peer.ID, key string) (*pb.Message, error) {
	meta := logging.LoggableMap{
		"key":  key,
		"peer": p,
	}

	eip := logger.EventBegin(ctx, "getValueSingle", meta)
	defer eip.Done()

	pmes := pb.NewMessage(pb.Message_GET_VALUE, []byte(key), 0)
	resp, err := dht.sendRequest(ctx, p, pmes)
	switch err {
	case nil:
		return resp, nil
	case ErrReadTimeout:
		logger.Warningf("getValueSingle: read timeout %s %s", p.Pretty(), key)
		fallthrough
	default:
		eip.SetError(err)
		return nil, err
	}
}

// getLocal attempts to retrieve the value from the datastore
func (dht *IpfsDHT) getLocal(key string) (*recpb.Record, error) {
	logger.Debugf("getLocal %s", key)
	rec, err := dht.getRecordFromDatastore(mkDsKey(key))
	if err != nil {
		logger.Warningf("getLocal: %s", err)
		return nil, err
	}

	// Double check the key. Can't hurt.
	if rec != nil && string(rec.GetKey()) != key {
		logger.Errorf("BUG getLocal: found a DHT record that didn't match it's key: %s != %s", rec.GetKey(), key)
		return nil, nil

	}
	return rec, nil
}

// putLocal stores the key value pair in the datastore
func (dht *IpfsDHT) putLocal(key string, rec *recpb.Record) error {
	logger.Debugf("putLocal: %v %v", key, rec)
	data, err := proto.Marshal(rec)
	if err != nil {
		logger.Warningf("putLocal: %s", err)
		return err
	}

	return dht.datastore.Put(mkDsKey(key), data)
}

// peerFound signals the routingTable that we've found a peer that
// supports the DHT protocol.
func (dht *IpfsDHT) peerFound(ctx context.Context, p peer.ID) {
	logger.Event(ctx, "peerFound", p)
	dht.routingTable.HandlePeerAlive(p)
}

// peerStoppedDHT signals the routing table that a peer has stopped supporting the DHT protocol.
func (dht *IpfsDHT) peerStoppedDHT(ctx context.Context, p peer.ID) {
	logger.Event(ctx, "peerStoppedDHT", p)
	// A peer that does not support the DHT protocol is dead for us.
	// There's no point in talking to anymore till it starts supporting the DHT protocol again.
	dht.routingTable.HandlePeerDead(p)
}

// peerDisconnected signals the routing table that a peer is not connected anymore.
func (dht *IpfsDHT) peerDisconnected(ctx context.Context, p peer.ID) {
	logger.Event(ctx, "peerDisconnected", p)
	dht.routingTable.HandlePeerDisconnect(p)

}

// FindLocal looks for a peer with a given ID connected to this dht and returns the peer and the table it was found in.
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
	eip := logger.EventBegin(ctx, "findPeerSingle",
		logging.LoggableMap{
			"peer":   p,
			"target": id,
		})
	defer eip.Done()

	pmes := pb.NewMessage(pb.Message_FIND_NODE, []byte(id), 0)
	resp, err := dht.sendRequest(ctx, p, pmes)
	switch err {
	case nil:
		return resp, nil
	case ErrReadTimeout:
		logger.Warningf("read timeout: %s %s", p.Pretty(), id)
		fallthrough
	default:
		eip.SetError(err)
		return nil, err
	}
}

func (dht *IpfsDHT) findProvidersSingle(ctx context.Context, p peer.ID, key multihash.Multihash) (*pb.Message, error) {
	eip := logger.EventBegin(ctx, "findProvidersSingle", p, multihashLoggableKey(key))
	defer eip.Done()

	pmes := pb.NewMessage(pb.Message_GET_PROVIDERS, key, 0)
	resp, err := dht.sendRequest(ctx, p, pmes)
	switch err {
	case nil:
		return resp, nil
	case ErrReadTimeout:
		logger.Warningf("read timeout: %s %s", p.Pretty(), key)
		fallthrough
	default:
		eip.SetError(err)
		return nil, err
	}
}

// nearestPeersToQuery returns the routing tables closest peers.
func (dht *IpfsDHT) nearestPeersToQuery(pmes *pb.Message, count int) []peer.ID {
	closer := dht.routingTable.NearestPeers(kb.ConvertKey(string(pmes.GetKey())), count)
	return closer
}

// betterPeersToQuery returns nearestPeersToQuery with some additional filtering
func (dht *IpfsDHT) betterPeersToQuery(pmes *pb.Message, p peer.ID, count int) []peer.ID {
	closer := dht.nearestPeersToQuery(pmes, count)

	// no node? nil
	if closer == nil {
		logger.Warning("betterPeersToQuery: no closer peers to send:", p)
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
		if clp == p {
			continue
		}

		filtered = append(filtered, clp)
	}

	// ok seems like closer nodes
	return filtered
}

func (dht *IpfsDHT) setMode(m mode) error {
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
func (dht *IpfsDHT) moveToServerMode() error {
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
func (dht *IpfsDHT) moveToClientMode() error {
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
					s.Reset()
				}
			}
		}
	}
	return nil
}

func (dht *IpfsDHT) getMode() mode {
	dht.modeLk.Lock()
	defer dht.modeLk.Unlock()
	return dht.mode
}

// Context return dht's context
func (dht *IpfsDHT) Context() context.Context {
	return dht.ctx
}

// Process return dht's process
func (dht *IpfsDHT) Process() goprocess.Process {
	return dht.proc
}

// RoutingTable return dht's routingTable
func (dht *IpfsDHT) RoutingTable() *kb.RoutingTable {
	return dht.routingTable
}

// Close calls Process Close
func (dht *IpfsDHT) Close() error {
	return dht.proc.Close()
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
	req := pb.NewMessage(pb.Message_PING, nil, 0)
	resp, err := dht.sendRequest(ctx, p, req)
	if err != nil {
		return xerrors.Errorf("sending request: %w", err)
	}
	if resp.Type != pb.Message_PING {
		return xerrors.Errorf("got unexpected response type: %v", resp.Type)
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
