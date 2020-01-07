package dht

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/opentracing/opentracing-go/log"

	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/peerstore"
	pstore "github.com/libp2p/go-libp2p-peerstore"

	"github.com/gogo/protobuf/proto"
	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	u "github.com/ipfs/go-ipfs-util"
	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
	"github.com/multiformats/go-base32"

	"github.com/opentracing/opentracing-go"
)

// dhthandler specifies the signature of functions that handle DHT messages.
type dhtHandler func(context.Context, peer.ID, *pb.Message) (*pb.Message, error)

func (dht *IpfsDHT) handlerForMsgType(t pb.Message_MessageType) dhtHandler {
	switch t {
	case pb.Message_GET_VALUE:
		return dht.handleGetValue
	case pb.Message_PUT_VALUE:
		return dht.handlePutValue
	case pb.Message_FIND_NODE:
		return dht.handleFindPeer
	case pb.Message_ADD_PROVIDER:
		return dht.handleAddProvider
	case pb.Message_GET_PROVIDERS:
		return dht.handleGetProviders
	case pb.Message_PING:
		return dht.handlePing
	default:
		return nil
	}
}

func (dht *IpfsDHT) handleGetValue(ctx context.Context, p peer.ID, pmes *pb.Message) (*pb.Message, error) {
	var (
		key = pmes.GetKey()
		err error
	)

	logger.Debugf("%s handleGetValue for key: %s", dht.self, key)
	sp, ctx := opentracing.StartSpanFromContext(ctx, "dht.get_value.in.handler", opentracing.Tags{
		"peer.id":        p,
		"dht.record_key": loggableKey(string(key)),
	})
	defer LinkedFinish(sp, &err)

	// be defensive: is there even a key?
	if len(key) == 0 {
		// TODO: send back an error response? could be bad, but the other node's hanging.
		err = errors.New("handleGetValue but no key was provided")
		return nil, err
	}

	resp := pb.NewMessage(pmes.GetType(), key, pmes.GetClusterLevel())
	resp.Record, err = dht.checkLocalDatastore(key)
	if err != nil {
		return nil, err
	}

	if resp.Record != nil {
		sp.SetTag("record.returned", true)
	}

	// Find closest peer on given cluster to desired key and reply with that info
	closer := dht.betterPeersToQuery(pmes, p, dht.bucketSize)
	if l := len(closer); l > 0 {
		// TODO: pstore.PeerInfos should move to core (=> peerstore.AddrInfos).
		closerinfos := pstore.PeerInfos(dht.peerstore, closer)
		for _, pi := range closerinfos {
			logger.Debugf("handleGetValue returning closer peer: '%s'", pi.ID)
			if len(pi.Addrs) < 1 {
				logger.Warningf(`no addresses on peer being sent!
					[local:%s]
					[sending:%s]
					[remote:%s]`, dht.self, pi.ID, p)
			}
		}
		resp.CloserPeers = pb.PeerInfosToPBPeers(dht.host.Network(), closerinfos)
		sp.SetTag("dht.closer.count", l)
	}

	return resp, nil
}

// Store a value in this peer local storage
func (dht *IpfsDHT) handlePutValue(ctx context.Context, p peer.ID, pmes *pb.Message) (*pb.Message, error) {
	var err error
	sp, ctx := opentracing.StartSpanFromContext(ctx, "dht.put_value.in.handler", opentracing.Tags{
		"remote.peer_id": p.Pretty(),
	})
	defer LinkedFinish(sp, &err)

	rec := pmes.GetRecord()
	if rec == nil {
		logger.Infof("Got nil record from: %s", p.Pretty())
		err = errors.New("nil record")
		return nil, err
	}

	if !bytes.Equal(pmes.GetKey(), rec.GetKey()) {
		err = errors.New("put key doesn't match record key")
		return nil, err
	}

	cleanRecord(rec)

	// Make sure the record is valid (not expired, valid signature etc)
	if err = dht.Validator.Validate(string(rec.GetKey()), rec.GetValue()); err != nil {
		logger.Warningf("Bad dht record in PUT from: %s. %s", p.Pretty(), err)
		return nil, err
	}

	dskey := convertToDsKey(rec.GetKey())

	// fetch the striped lock for this key
	var indexForLock byte
	if l := len(rec.GetKey()); l == 0 {
		indexForLock = 0
	} else {
		indexForLock = rec.GetKey()[l-1]
	}
	lk := &dht.stripedPutLocks[indexForLock]
	lk.Lock()
	defer lk.Unlock()

	// Make sure the new record is "better" than the record we have locally.
	// This prevents a record with for example a lower sequence number from
	// overwriting a record with a higher sequence number.
	existing, err := dht.getRecordFromDatastore(dskey)
	if err != nil {
		return nil, err
	}

	if existing != nil {
		var i int
		recs := [][]byte{rec.GetValue(), existing.GetValue()}
		i, err = dht.Validator.Select(string(rec.GetKey()), recs)
		if err != nil {
			logger.Warningf("Bad dht record in PUT from %s: %s", p.Pretty(), err)
			return nil, err
		}
		if i != 0 {
			logger.Infof("DHT record in PUT from %s is older than existing record. Ignoring", p.Pretty())
			err = errors.New("old record")
			return nil, err
		}
	}

	// record the time we receive every record
	rec.TimeReceived = u.FormatRFC3339(time.Now())

	data, err := proto.Marshal(rec)
	if err != nil {
		return nil, err
	}

	err = dht.datastore.Put(dskey, data)
	logger.Debugf("%s handlePutValue %v", dht.self, dskey)
	return pmes, err
}

func (dht *IpfsDHT) handlePing(ctx context.Context, p peer.ID, pmes *pb.Message) (*pb.Message, error) {
	sp, ctx := opentracing.StartSpanFromContext(ctx, "dht.ping.in.handler", opentracing.Tags{
		"remote.peer_id": p.Pretty(),
	})
	defer sp.Finish()

	logger.Debugf("%s Responding to ping from %s!\n", dht.self, p)
	return pmes, nil
}

func (dht *IpfsDHT) handleFindPeer(ctx context.Context, p peer.ID, pmes *pb.Message) (_ *pb.Message, _err error) {
	sp, ctx := opentracing.StartSpanFromContext(ctx, "dht.find_peer.in.handler", opentracing.Tags{
		"remote.peer_id": p.Pretty(),
	})
	defer sp.Finish()

	resp := pb.NewMessage(pmes.GetType(), nil, pmes.GetClusterLevel())
	var closest []peer.ID

	// if looking for self... special case where we send it on CloserPeers.
	targetPid := peer.ID(pmes.GetKey())
	sp.SetTag("lookup.key", targetPid.Pretty())

	if targetPid == dht.self {
		closest = []peer.ID{dht.self}
	} else {
		closest = dht.betterPeersToQuery(pmes, p, dht.bucketSize)

		// Never tell a peer about itself.
		if targetPid != p {
			// If we're connected to the target peer, report their peer info.
			// This makes FindPeer work even if the target peer isn't in our
			// routing table.
			//
			// Alternatively, we could just check our peerstore. However, we
			// don't want to return out of date information. We can change this
			// in the future when we add a progressive, asynchronous
			// `SearchPeer` function and improve peer routing in the host.
			switch dht.host.Network().Connectedness(targetPid) {
			case network.Connected, network.CanConnect:
				closest = append(closest, targetPid)
			}
		}
	}

	sp.SetTag("result.raw.count", len(closest))

	if closest == nil {
		logger.Infof("%s handleFindPeer %s: could not find anything.", dht.self, p)
		return resp, nil
	}

	// TODO: pstore.PeerInfos should move to core (=> peerstore.AddrInfos).
	closestinfos := pstore.PeerInfos(dht.peerstore, closest)
	// possibly an over-allocation but this array is temporary anyways.
	withAddresses := make([]peer.AddrInfo, 0, len(closestinfos))
	for _, pi := range closestinfos {
		if len(pi.Addrs) > 0 {
			withAddresses = append(withAddresses, pi)
		}
	}

	sp.SetTag("result.filtered.count", len(withAddresses))
	resp.CloserPeers = pb.PeerInfosToPBPeers(dht.host.Network(), withAddresses)
	return resp, nil
}

func (dht *IpfsDHT) handleGetProviders(ctx context.Context, p peer.ID, pmes *pb.Message) (*pb.Message, error) {
	var err error
	sp, ctx := opentracing.StartSpanFromContext(ctx, "dht.get_providers.in.handler", opentracing.Tags{
		"remote.peer_id": p.Pretty(),
	})
	defer LinkedFinish(sp, &err)

	c, err := cid.Cast([]byte(pmes.GetKey()))
	if err != nil {
		sp.SetTag("lookup.key.bad.hex", hex.EncodeToString(pmes.GetKey()))
		return nil, err
	}

	sp.SetTag("lookup.key", c.String())

	// debug logging niceness.
	reqDesc := fmt.Sprintf("%s handleGetProviders(%s, %s): ", dht.self, p, c)
	logger.Debugf("%s begin", reqDesc)
	defer logger.Debugf("%s end", reqDesc)

	// check if we have this value, to add ourselves as provider.
	has, err := dht.datastore.Has(convertToDsKey(c.Bytes()))
	if err != nil && err != ds.ErrNotFound {
		logger.Debugf("unexpected datastore error: %v\n", err)
		sp.LogFields(log.Error(err))
		err = nil // do not mark the span as errored.
		has = false
	}

	// do we know of peers providing this record?
	providers := dht.providers.GetProviders(ctx, c)
	if has {
		// add ourselves if we have the record.
		sp.SetTag("record.local", true)
		providers = append(providers, dht.self)
		logger.Debugf("%s have the value. added self as provider", reqDesc)
	}

	resp := pb.NewMessage(pmes.GetType(), pmes.GetKey(), pmes.GetClusterLevel())

	if len(providers) > 0 {
		// TODO: pstore.PeerInfos should move to core (=> peerstore.AddrInfos).
		infos := pstore.PeerInfos(dht.peerstore, providers)
		resp.ProviderPeers = pb.PeerInfosToPBPeers(dht.host.Network(), infos)
		logger.Debugf("%s have %d providers: %s", reqDesc, len(providers), infos)
	}

	// Also send closer peers.
	closer := dht.betterPeersToQuery(pmes, p, dht.bucketSize)
	if closer != nil {
		// TODO: pstore.PeerInfos should move to core (=> peerstore.AddrInfos).
		infos := pstore.PeerInfos(dht.peerstore, closer)
		resp.CloserPeers = pb.PeerInfosToPBPeers(dht.host.Network(), infos)
		logger.Debugf("%s have %d closer peers: %s", reqDesc, len(closer), infos)
	}

	sp.SetTag("result.raw.count", len(providers))
	sp.SetTag("dht.closer.count", len(closer))

	return resp, nil
}

func (dht *IpfsDHT) handleAddProvider(ctx context.Context, p peer.ID, pmes *pb.Message) (*pb.Message, error) {
	var err error
	sp, ctx := opentracing.StartSpanFromContext(ctx, "dht.add_provider.in.handler", opentracing.Tags{
		"remote.peer_id": p.Pretty(),
	})
	defer LinkedFinish(sp, &err)

	c, err := cid.Cast([]byte(pmes.GetKey()))
	if err != nil {
		sp.SetTag("record.key.bad.hex", hex.EncodeToString(pmes.GetKey()))
		return nil, err
	}

	sp.SetTag("record.key", c.String())

	logger.Debugf("%s adding %s as a provider for '%s'\n", dht.self, p, c)

	// add provider should use the address given in the message
	pinfos := pb.PBPeersToPeerInfos(pmes.GetProviderPeers())
	for _, pi := range pinfos {
		if pi.ID != p {
			msg := fmt.Sprintf("handleAddProvider received provider %s from %s. Ignore.", pi.ID, p)
			sp.LogFields(log.String("error", "invalid_origin"), log.String("message", msg))
			// we should ignore this provider record! not from originator.
			// (we should sign them and check signature later...)
			logger.Debugf(msg)
			continue
		}

		if len(pi.Addrs) < 1 {
			msg := fmt.Sprintf("%s got no valid addresses for provider %s. Ignore.", dht.self, p)
			sp.LogFields(log.String("error", "invalid_addrs"), log.String("message", msg))
			logger.Debugf(msg)
			continue
		}

		logger.Debugf("received provider %s for %s (addrs: %s)", p, c, pi.Addrs)
		if pi.ID != dht.self { // don't add own addrs.
			// add the received addresses to our peerstore.
			sp.LogFields(log.String("event", "addresses_added"), log.Int("count", len(pi.Addrs)))
			dht.peerstore.AddAddrs(pi.ID, pi.Addrs, peerstore.ProviderAddrTTL)
		}

		dht.providers.AddProvider(ctx, c, p)
	}

	return nil, nil
}

func convertToDsKey(s []byte) ds.Key {
	return ds.NewKey(base32.RawStdEncoding.EncodeToString(s))
}
