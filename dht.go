package dht

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/libp2p/go-eventbus"
	"github.com/libp2p/go-libp2p-core/event"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/peerstore"
	"github.com/libp2p/go-libp2p-core/protocol"
	"github.com/libp2p/go-libp2p-core/routing"

	"go.opencensus.io/tag"
	"golang.org/x/xerrors"

	"github.com/libp2p/go-libp2p-kad-dht/metrics"
	opts "github.com/libp2p/go-libp2p-kad-dht/opts"
	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
	"github.com/libp2p/go-libp2p-kad-dht/providers"

	"github.com/gogo/protobuf/proto"
	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	logging "github.com/ipfs/go-log"
	goprocess "github.com/jbenet/goprocess"
	goprocessctx "github.com/jbenet/goprocess/context"
	circuit "github.com/libp2p/go-libp2p-circuit"
	kb "github.com/libp2p/go-libp2p-kbucket"
	record "github.com/libp2p/go-libp2p-record"
	recpb "github.com/libp2p/go-libp2p-record/pb"
	"github.com/multiformats/go-base32"
	ma "github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr-net"
)

var logger = logging.Logger("dht")

type DHTMode int

const (
	ModeServer = DHTMode(1)
	ModeClient = DHTMode(2)
)

const BaseConnMgrScore = 5

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

	mode   DHTMode
	modeLk sync.Mutex

	bucketSize int

	subscriptions struct {
		evtPeerIdentification event.Subscription
	}

	autoRefresh           bool
	rtRefreshQueryTimeout time.Duration
	rtRefreshPeriod       time.Duration
	triggerRtRefresh      chan chan<- error

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
func New(ctx context.Context, h host.Host, options ...opts.Option) (*IpfsDHT, error) {
	var cfg opts.Options
	cfg.BucketSize = KValue
	if err := cfg.Apply(append([]opts.Option{opts.Defaults}, options...)...); err != nil {
		return nil, err
	}
	dht, err := makeDHT(ctx, h, &cfg)
	if err != nil {
		return nil, err
	}
	dht.autoRefresh = cfg.RoutingTable.AutoRefresh
	dht.rtRefreshPeriod = cfg.RoutingTable.RefreshPeriod
	dht.rtRefreshQueryTimeout = cfg.RoutingTable.RefreshQueryTimeout

	dht.maxRecordAge = cfg.MaxRecordAge
	dht.enableProviders = cfg.EnableProviders
	dht.enableValues = cfg.EnableValues

	dht.Validator = cfg.Validator
	dht.mode = ModeClient

	if !cfg.Client {
		if err := dht.moveToServerMode(); err != nil {
			return nil, err
		}
	}

	dht.proc = goprocessctx.WithContextAndTeardown(ctx, func() error {
		if dht.subscriptions.evtPeerIdentification != nil {
			_ = dht.subscriptions.evtPeerIdentification.Close()
		}
		return nil
	})

	// register for network notifs.
	dht.proc.AddChild((*subscriberNotifee)(dht).Process())

	// handle protocol changes
	dht.proc.Go(dht.handleProtocolChanges)

	// handle providers
	dht.proc.AddChild(dht.providers.Process())

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

func makeRoutingTable(h host.Host, cfg *opts.Options) (*kb.RoutingTable, error) {
	self := kb.ConvertPeerID(h.ID())
	// construct the routing table with a peer validation function
	pvLogger := logging.Logger("RT peer validation")
	pvF := func(c context.Context, p peer.ID) bool {
		if err := h.Connect(c, peer.AddrInfo{ID: p}); err != nil {
			pvLogger.Errorf("failed to connect to peer %s for validation, err=%s", p, err)
			return false
		}
		fmt.Print("Returning true")
		return true
	}

	rtOpts := []kb.Option{kb.PeerValidationFnc(pvF)}
	if !(cfg.RoutingTableCleanup.Interval == 0) {
		rtOpts = append(rtOpts, kb.TableCleanupInterval(cfg.RoutingTableCleanup.Interval))
	}

	rt, err := kb.NewRoutingTable(cfg.BucketSize, self, time.Minute, h.Peerstore(),
		rtOpts...)
	cmgr := h.ConnManager()

	rt.PeerAdded = func(p peer.ID) {
		commonPrefixLen := kb.CommonPrefixLen(self, kb.ConvertPeerID(p))
		cmgr.TagPeer(p, "kbucket", BaseConnMgrScore+commonPrefixLen)
	}

	rt.PeerRemoved = func(p peer.ID) {
		cmgr.UntagPeer(p, "kbucket")
	}

	return rt, err
}

func makeDHT(ctx context.Context, h host.Host, cfg *opts.Options) (*IpfsDHT, error) {
	rt, err := makeRoutingTable(h, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to construct routing table,err=%s", err)
	}

	dht := &IpfsDHT{
		datastore:        cfg.Datastore,
		self:             h.ID(),
		peerstore:        h.Peerstore(),
		host:             h,
		strmap:           make(map[peer.ID]*messageSender),
		ctx:              ctx,
		providers:        providers.NewProviderManager(ctx, h.ID(), cfg.Datastore),
		birth:            time.Now(),
		routingTable:     rt,
		protocols:        cfg.Protocols,
		bucketSize:       cfg.BucketSize,
		triggerRtRefresh: make(chan chan<- error),
	}

	evts := []interface{}{&event.EvtPeerIdentificationCompleted{}, &event.EvtPeerIdentificationFailed{}}
	dht.subscriptions.evtPeerIdentification, err = h.EventBus().Subscribe(evts, eventbus.BufSize(256))
	if err != nil {
		logger.Errorf("dht not subscribed to peer identification events; things will fail; err: %s", err)
	}

	dht.ctx = dht.newContextWithLocalTags(ctx)

	return dht, nil
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

// Update signals the routingTable to Update its last-seen status
// on the given peer.
func (dht *IpfsDHT) Update(ctx context.Context, p peer.ID) {
	logger.Event(ctx, "updatePeer", p)
	for _, c := range dht.host.Network().ConnsToPeer(p) {
		if dht.shouldAddPeerToRoutingTable(c) {
			dht.routingTable.HandlePeerAlive(c.RemotePeer())
			return
		}
	}
}

func (dht *IpfsDHT) UpdateConn(ctx context.Context, c network.Conn) {
	if !dht.shouldAddPeerToRoutingTable(c) {
		return
	}
	logger.Event(ctx, "updatePeer", c.RemotePeer())
	dht.routingTable.HandlePeerAlive(c.RemotePeer())
}

func (dht *IpfsDHT) shouldAddPeerToRoutingTable(c network.Conn) bool {
	if isRelayAddr(c.RemoteMultiaddr()) {
		return false
	}
	if c.Stat().Direction == network.DirOutbound {
		// we established this connection, so they're definitely diallable.
		return true
	}

	ai := dht.host.Peerstore().PeerInfo(c.RemotePeer())
	if isPeerLocallyConnected(c) {
		// TODO: for now, we can't easily tell if the peer on our subnet
		// is dialable or not, so don't discriminate.

		// We won't return these peers in queries unless the requester's
		// remote addr is also private.
		return len(ai.Addrs) > 0
	}

	return isPubliclyRoutable(ai)
}

func isPubliclyRoutable(ai peer.AddrInfo) bool {
	if len(ai.Addrs) == 0 {
		return false
	}

	var hasPublicAddr bool
	for _, a := range ai.Addrs {
		if isRelayAddr(a) {
			return false
		}
		if manet.IsPublicAddr(a) {
			hasPublicAddr = true
		}
	}
	return hasPublicAddr
}

// taken from go-libp2p/p2p/host/relay
func isRelayAddr(a ma.Multiaddr) bool {
	isRelay := false

	ma.ForEach(a, func(c ma.Component) bool {
		switch c.Protocol().Code {
		case circuit.P_CIRCUIT:
			isRelay = true
			return false
		default:
			return true
		}
	})

	return isRelay
}

func isPeerLocallyConnected(c network.Conn) bool {
	addr := c.RemoteMultiaddr()
	return manet.IsPrivateAddr(addr) || manet.IsIPLoopback(addr)
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

func (dht *IpfsDHT) findProvidersSingle(ctx context.Context, p peer.ID, key cid.Cid) (*pb.Message, error) {
	eip := logger.EventBegin(ctx, "findProvidersSingle", p, key)
	defer eip.Done()

	pmes := pb.NewMessage(pb.Message_GET_PROVIDERS, key.Bytes(), 0)
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

// betterPeersToQuery returns nearestPeersToQuery, but if and only if closer than self.
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

func (dht *IpfsDHT) SetMode(m DHTMode) error {
	dht.modeLk.Lock()
	defer dht.modeLk.Unlock()

	if m == dht.mode {
		return nil
	}

	switch m {
	case ModeServer:
		return dht.moveToServerMode()
	case ModeClient:
		return dht.moveToClientMode()
	default:
		return fmt.Errorf("unrecognized dht mode: %d", m)
	}
}

func (dht *IpfsDHT) moveToServerMode() error {
	dht.mode = ModeServer
	for _, p := range dht.protocols {
		dht.host.SetStreamHandler(p, dht.handleNewStream)
	}
	return nil
}

func (dht *IpfsDHT) moveToClientMode() error {
	dht.mode = ModeClient
	for _, p := range dht.protocols {
		dht.host.RemoveStreamHandler(p)
	}

	pset := make(map[protocol.ID]bool)
	for _, p := range dht.protocols {
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

func (dht *IpfsDHT) getMode() DHTMode {
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

func (dht *IpfsDHT) connForPeer(p peer.ID) network.Conn {
	if cs := dht.host.Network().ConnsToPeer(p); len(cs) > 0 {
		return cs[0]
	}
	return nil
}

func (dht *IpfsDHT) handleProtocolChanges(proc goprocess.Process) {
	// register for event bus protocol ID changes
	sub, err := dht.host.EventBus().Subscribe(new(event.EvtPeerProtocolsUpdated))
	if err != nil {
		panic(err)
	}
	defer sub.Close()

	pmap := make(map[protocol.ID]bool)
	for _, p := range dht.protocols {
		pmap[p] = true
	}

	for {
		select {
		case ie, ok := <-sub.Out():
			e, ok := ie.(event.EvtPeerProtocolsUpdated)
			if !ok {
				logger.Errorf("got wrong type from subscription: %T", ie)
				return
			}

			if !ok {
				return
			}
			var drop, add bool
			for _, p := range e.Added {
				if pmap[p] {
					add = true
				}
			}
			for _, p := range e.Removed {
				if pmap[p] {
					drop = true
				}
			}

			if add && drop {
				// TODO: discuss how to handle this case
				logger.Warning("peer adding and dropping dht protocols? odd")
			} else if add {
				dht.RoutingTable().HandlePeerAlive(e.Peer)
			} else if drop {
				dht.RoutingTable().HandlePeerDead(e.Peer)
			}
		case <-proc.Closing():
			return
		}
	}
}
