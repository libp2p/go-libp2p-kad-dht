package dht

import (
	"bytes"
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	proto "github.com/gogo/protobuf/proto"
	cid "github.com/ipfs/go-cid"
	u "github.com/ipfs/go-ipfs-util"
	logging "github.com/ipfs/go-log"
	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
	kb "github.com/libp2p/go-libp2p-kbucket"
	inet "github.com/libp2p/go-libp2p-net"
	peer "github.com/libp2p/go-libp2p-peer"
	pset "github.com/libp2p/go-libp2p-peer/peerset"
	pstore "github.com/libp2p/go-libp2p-peerstore"
	record "github.com/libp2p/go-libp2p-record"
	routing "github.com/libp2p/go-libp2p-routing"
	notif "github.com/libp2p/go-libp2p-routing/notifications"
	ropts "github.com/libp2p/go-libp2p-routing/options"
)

// asyncQueryBuffer is the size of buffered channels in async queries. This
// buffer allows multiple queries to execute simultaneously, return their
// results and continue querying closer peers. Note that different query
// results will wait for the channel to drain.
var asyncQueryBuffer = 10

// This file implements the Routing interface for the IpfsDHT struct.

// Basic Put/Get

// PutValue adds value corresponding to given Key.
// This is the top level "Store" operation of the DHT
func (dht *IpfsDHT) PutValue(ctx context.Context, key string, value []byte, options ...ropts.Option) (err error) {
	eip := log.EventBegin(ctx, "PutValue")
	defer func() {
		eip.Append(loggableKey(key))
		if err != nil {
			eip.SetError(err)
		}
		eip.Done()
	}()
	log.Debugf("PutValue %s", key)

	// TODO: How to handle the offline option?

	rec := record.MakePutRecord(key, value)
	rec.TimeReceived = proto.String(u.FormatRFC3339(time.Now()))
	err = dht.putLocal(key, rec)
	if err != nil {
		return err
	}

	pchan, err := dht.GetClosestPeers(ctx, key)
	if err != nil {
		return err
	}

	wg := sync.WaitGroup{}
	for p := range pchan {
		wg.Add(1)
		go func(p peer.ID) {
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			defer wg.Done()
			notif.PublishQueryEvent(ctx, &notif.QueryEvent{
				Type: notif.Value,
				ID:   p,
			})

			err := dht.putValueToPeer(ctx, p, key, rec)
			if err != nil {
				log.Debugf("failed putting value to peer: %s", err)
			}
		}(p)
	}
	wg.Wait()
	return nil
}

// GetValue searches for the value corresponding to given Key.
func (dht *IpfsDHT) GetValue(ctx context.Context, key string, opts ...ropts.Option) (_ []byte, err error) {
	eip := log.EventBegin(ctx, "GetValue")
	defer func() {
		eip.Append(loggableKey(key))
		if err != nil {
			eip.SetError(err)
		}
		eip.Done()
	}()
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	var cfg ropts.Options
	if err := cfg.Apply(opts...); err != nil {
		return nil, err
	}

	results, err := dht.getValues(ctx, key, &cfg)
	if err != nil {
		return nil, err
	}

	var outdatedPeers, currentPeers []peer.ID

	var best []byte
	for result := range results {
		switch {
		case result.val == nil:
			outdatedPeers = append(outdatedPeers, result.from)
		case best == nil:
			best = result.val
			fallthrough
		case bytes.Equal(result.val, best):
			currentPeers = append(currentPeers, result.from)
		default:
			i, err := dht.Selector.BestRecord(key, [][]byte{best, result.val})
			if err != nil {
				log.Error(err)
				return nil, err
			}
			switch i {
			case 0:
				outdatedPeers = append(outdatedPeers, result.from)
			case 1:
				outdatedPeers = append(outdatedPeers, currentPeers...)
				currentPeers = append(currentPeers[:0], result.from)
			default:
				err := fmt.Errorf("invalid bad selector for key: %s", loggableKey(key))
				log.Error(err)
				return nil, err
			}
		}
	}

	// if someone sent us a different 'less-valid' record, lets correct them
	if best != nil && len(outdatedPeers) > 0 {
		fixupRec := record.MakePutRecord(key, best)
		for _, p := range outdatedPeers {
			// TODO: Use a worker.
			go func(p peer.ID) {
				if p == dht.self {
					err := dht.putLocal(key, fixupRec)
					if err != nil {
						log.Error("Error correcting local dht entry:", err)
					}
					return
				}
				ctx, cancel := context.WithTimeout(dht.Context(), time.Second*30)
				defer cancel()
				err := dht.putValueToPeer(ctx, p, key, fixupRec)
				if err != nil {
					log.Error("Error correcting DHT entry: ", err)
				}
			}(p)
		}
	}

	if err := ctx.Err(); err != nil {
		return best, err
	}
	if best == nil {
		return nil, routing.ErrNotFound
	}

	return best, nil
}

type recvdVal struct {
	val  []byte
	from peer.ID
}

func (dht *IpfsDHT) getValues(ctx context.Context, key string, opts *ropts.Options) (_ <-chan recvdVal, _err error) {
	eip := log.EventBegin(ctx, "getValues")
	defer func() {
		eip.Append(loggableKey(key))
		if _err != nil {
			eip.SetError(_err)
		}
		eip.Done()
	}()

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	vals := make(chan recvdVal, 1)

	responsesNeeded := getQuorum(opts)

	// If we have it locally, don't bother doing an RPC!
	lrec, err := dht.getLocal(key)
	if err == nil {
		vals <- recvdVal{
			val:  lrec.GetValue(),
			from: dht.self,
		}
		responsesNeeded--
	}

	if opts.Offline || responsesNeeded <= 0 {
		close(vals)
		return vals, nil
	}

	// get closest peers in the routing table
	rtp := dht.routingTable.NearestPeers(kb.ConvertKey(key), AlphaValue)
	log.Debugf("peers in rt: %d %s", len(rtp), rtp)
	if len(rtp) == 0 {
		log.Warning("No peers from routing table!")
		return nil, kb.ErrLookupFailure
	}

	// setup the Query
	parent := ctx
	query := dht.newQuery(key, func(ctx context.Context, p peer.ID) (*dhtQueryResult, error) {
		notif.PublishQueryEvent(parent, &notif.QueryEvent{
			Type: notif.SendingQuery,
			ID:   p,
		})

		rec, peers, err := dht.getValueOrPeers(ctx, p, key)
		switch err {
		case routing.ErrNotFound:
			// in this case, they responded with nothing,
			// still send a notification so listeners can know the
			// request has completed 'successfully'
			notif.PublishQueryEvent(parent, &notif.QueryEvent{
				Type: notif.PeerResponse,
				ID:   p,
			})
			return nil, err
		default:
			return nil, err

		case nil, errInvalidRecord:
			// in either of these cases, we want to keep going
		}

		res := &dhtQueryResult{closerPeers: peers}

		if rec.GetValue() != nil || err == errInvalidRecord {
			select {
			case vals <- recvdVal{
				val:  rec.GetValue(),
				from: p,
			}:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			if atomic.AddInt64(&responsesNeeded, -1) <= 0 {
				res.success = true
			}
		}

		notif.PublishQueryEvent(parent, &notif.QueryEvent{
			Type:      notif.PeerResponse,
			ID:        p,
			Responses: peers,
		})

		return res, nil
	})

	go func() {
		defer close(vals)
		// run it!
		_, err := query.Run(ctx, rtp)

		if err == nil || ctx.Err() != nil {
			return
		}

		// Not much we can do about the error.
		// Any error that's *not* a context related error is likely a
		// programmer error. There's not much that a user can do about
		// it and no real reason to expose it.
		log.Error(err)
	}()

	return vals, nil
}

// Provider abstraction for indirect stores.
// Some DHTs store values directly, while an indirect store stores pointers to
// locations of the value, similarly to Coral and Mainline DHT.

// Provide makes this node announce that it can provide a value for the given key
func (dht *IpfsDHT) Provide(ctx context.Context, key *cid.Cid, brdcst bool) (err error) {
	eip := log.EventBegin(ctx, "Provide", key, logging.LoggableMap{"broadcast": brdcst})
	defer func() {
		if err != nil {
			eip.SetError(err)
		}
		eip.Done()
	}()

	// add self locally
	dht.providers.AddProvider(ctx, key, dht.self)
	if !brdcst {
		return nil
	}

	peers, err := dht.GetClosestPeers(ctx, key.KeyString())
	if err != nil {
		return err
	}

	mes, err := dht.makeProvRecord(key)
	if err != nil {
		return err
	}

	wg := sync.WaitGroup{}
	for p := range peers {
		wg.Add(1)
		go func(p peer.ID) {
			defer wg.Done()
			log.Debugf("putProvider(%s, %s)", key, p)
			err := dht.sendMessage(ctx, p, mes)
			if err != nil {
				log.Debug(err)
			}
		}(p)
	}
	wg.Wait()
	return nil
}
func (dht *IpfsDHT) makeProvRecord(skey *cid.Cid) (*pb.Message, error) {
	pi := pstore.PeerInfo{
		ID:    dht.self,
		Addrs: dht.host.Addrs(),
	}

	// // only share WAN-friendly addresses ??
	// pi.Addrs = addrutil.WANShareableAddrs(pi.Addrs)
	if len(pi.Addrs) < 1 {
		return nil, fmt.Errorf("no known addresses for self. cannot put provider.")
	}

	pmes := pb.NewMessage(pb.Message_ADD_PROVIDER, skey.KeyString(), 0)
	pmes.ProviderPeers = pb.RawPeerInfosToPBPeers([]pstore.PeerInfo{pi})
	return pmes, nil
}

// FindProviders searches until the context expires.
func (dht *IpfsDHT) FindProviders(ctx context.Context, c *cid.Cid) ([]pstore.PeerInfo, error) {
	var providers []pstore.PeerInfo
	for p := range dht.FindProvidersAsync(ctx, c, KValue) {
		providers = append(providers, p)
	}
	return providers, nil
}

// FindProvidersAsync is the same thing as FindProviders, but returns a channel.
// Peers will be returned on the channel as soon as they are found, even before
// the search query completes.
func (dht *IpfsDHT) FindProvidersAsync(ctx context.Context, key *cid.Cid, count int) <-chan pstore.PeerInfo {
	log.Event(ctx, "findProviders", key)
	peerOut := make(chan pstore.PeerInfo, count)
	go dht.findProvidersAsyncRoutine(ctx, key, count, peerOut)
	return peerOut
}

func (dht *IpfsDHT) findProvidersAsyncRoutine(ctx context.Context, key *cid.Cid, count int, peerOut chan pstore.PeerInfo) {
	defer log.EventBegin(ctx, "findProvidersAsync", key).Done()
	defer close(peerOut)

	ps := pset.NewLimited(count)
	provs := dht.providers.GetProviders(ctx, key)
	for _, p := range provs {
		// NOTE: Assuming that this list of peers is unique
		if ps.TryAdd(p) {
			pi := dht.peerstore.PeerInfo(p)
			select {
			case peerOut <- pi:
			case <-ctx.Done():
				return
			}
		}

		// If we have enough peers locally, don't bother with remote RPC
		// TODO: is this a DOS vector?
		if ps.Size() >= count {
			return
		}
	}

	// setup the Query
	parent := ctx
	query := dht.newQuery(key.KeyString(), func(ctx context.Context, p peer.ID) (*dhtQueryResult, error) {
		notif.PublishQueryEvent(parent, &notif.QueryEvent{
			Type: notif.SendingQuery,
			ID:   p,
		})
		pmes, err := dht.findProvidersSingle(ctx, p, key)
		if err != nil {
			return nil, err
		}

		log.Debugf("%d provider entries", len(pmes.GetProviderPeers()))
		provs := pb.PBPeersToPeerInfos(pmes.GetProviderPeers())
		log.Debugf("%d provider entries decoded", len(provs))

		// Add unique providers from request, up to 'count'
		for _, prov := range provs {
			if prov.ID != dht.self {
				dht.peerstore.AddAddrs(prov.ID, prov.Addrs, pstore.TempAddrTTL)
			}
			log.Debugf("got provider: %s", prov)
			if ps.TryAdd(prov.ID) {
				log.Debugf("using provider: %s", prov)
				select {
				case peerOut <- *prov:
				case <-ctx.Done():
					log.Debug("context timed out sending more providers")
					return nil, ctx.Err()
				}
			}
			if ps.Size() >= count {
				log.Debugf("got enough providers (%d/%d)", ps.Size(), count)
				return &dhtQueryResult{success: true}, nil
			}
		}

		// Give closer peers back to the query to be queried
		closer := pmes.GetCloserPeers()
		clpeers := pb.PBPeersToPeerInfos(closer)
		log.Debugf("got closer peers: %d %s", len(clpeers), clpeers)

		notif.PublishQueryEvent(parent, &notif.QueryEvent{
			Type:      notif.PeerResponse,
			ID:        p,
			Responses: clpeers,
		})
		return &dhtQueryResult{closerPeers: clpeers}, nil
	})

	peers := dht.routingTable.NearestPeers(kb.ConvertKey(key.KeyString()), AlphaValue)
	_, err := query.Run(ctx, peers)
	if err != nil {
		log.Debugf("Query error: %s", err)
		// Special handling for issue: https://github.com/ipfs/go-ipfs/issues/3032
		if fmt.Sprint(err) == "<nil>" {
			log.Error("reproduced bug 3032:")
			log.Errorf("Errors type information: %#v", err)
			log.Errorf("go version: %s", runtime.Version())
			log.Error("please report this information to: https://github.com/ipfs/go-ipfs/issues/3032")

			// replace problematic error with something that won't crash the daemon
			err = fmt.Errorf("<nil>")
		}
		notif.PublishQueryEvent(ctx, &notif.QueryEvent{
			Type:  notif.QueryError,
			Extra: err.Error(),
		})
	}
}

// FindPeer searches for a peer with given ID.
func (dht *IpfsDHT) FindPeer(ctx context.Context, id peer.ID) (_ pstore.PeerInfo, err error) {
	eip := log.EventBegin(ctx, "FindPeer", id)
	defer func() {
		if err != nil {
			eip.SetError(err)
		}
		eip.Done()
	}()

	// Check if were already connected to them
	if pi := dht.FindLocal(id); pi.ID != "" {
		return pi, nil
	}

	peers := dht.routingTable.NearestPeers(kb.ConvertPeerID(id), AlphaValue)
	if len(peers) == 0 {
		return pstore.PeerInfo{}, kb.ErrLookupFailure
	}

	// Sanity...
	for _, p := range peers {
		if p == id {
			log.Debug("found target peer in list of closest peers...")
			return dht.peerstore.PeerInfo(p), nil
		}
	}

	// setup the Query
	parent := ctx
	query := dht.newQuery(string(id), func(ctx context.Context, p peer.ID) (*dhtQueryResult, error) {
		notif.PublishQueryEvent(parent, &notif.QueryEvent{
			Type: notif.SendingQuery,
			ID:   p,
		})

		pmes, err := dht.findPeerSingle(ctx, p, id)
		if err != nil {
			return nil, err
		}

		closer := pmes.GetCloserPeers()
		clpeerInfos := pb.PBPeersToPeerInfos(closer)

		// see if we got the peer here
		for _, npi := range clpeerInfos {
			if npi.ID == id {
				return &dhtQueryResult{
					peer:    npi,
					success: true,
				}, nil
			}
		}

		notif.PublishQueryEvent(parent, &notif.QueryEvent{
			Type:      notif.PeerResponse,
			ID:        p,
			Responses: clpeerInfos,
		})

		return &dhtQueryResult{closerPeers: clpeerInfos}, nil
	})

	// run it!
	result, err := query.Run(ctx, peers)
	if err != nil {
		return pstore.PeerInfo{}, err
	}

	log.Debugf("FindPeer %v %v", id, result.success)
	if result.peer.ID == "" {
		return pstore.PeerInfo{}, routing.ErrNotFound
	}

	return *result.peer, nil
}

// FindPeersConnectedToPeer searches for peers directly connected to a given peer.
func (dht *IpfsDHT) FindPeersConnectedToPeer(ctx context.Context, id peer.ID) (<-chan *pstore.PeerInfo, error) {

	peerchan := make(chan *pstore.PeerInfo, asyncQueryBuffer)
	peersSeen := make(map[peer.ID]struct{})

	peers := dht.routingTable.NearestPeers(kb.ConvertPeerID(id), AlphaValue)
	if len(peers) == 0 {
		return nil, kb.ErrLookupFailure
	}

	// setup the Query
	query := dht.newQuery(string(id), func(ctx context.Context, p peer.ID) (*dhtQueryResult, error) {

		pmes, err := dht.findPeerSingle(ctx, p, id)
		if err != nil {
			return nil, err
		}

		var clpeers []*pstore.PeerInfo
		closer := pmes.GetCloserPeers()
		for _, pbp := range closer {
			pi := pb.PBPeerToPeerInfo(pbp)

			// skip peers already seen
			if _, found := peersSeen[pi.ID]; found {
				continue
			}
			peersSeen[pi.ID] = struct{}{}

			// if peer is connected, send it to our client.
			if pb.Connectedness(*pbp.Connection) == inet.Connected {
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case peerchan <- pi:
				}
			}

			// if peer is the peer we're looking for, don't bother querying it.
			// TODO maybe query it?
			if pb.Connectedness(*pbp.Connection) != inet.Connected {
				clpeers = append(clpeers, pi)
			}
		}

		return &dhtQueryResult{closerPeers: clpeers}, nil
	})

	// run it! run it asynchronously to gen peers as results are found.
	// this does no error checking
	go func() {
		if _, err := query.Run(ctx, peers); err != nil {
			log.Debug(err)
		}

		// close the peerchan channel when done.
		close(peerchan)
	}()

	return peerchan, nil
}
