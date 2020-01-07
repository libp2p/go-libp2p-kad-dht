package dht

import (
	"context"
	"fmt"
	"time"

	"github.com/gogo/protobuf/proto"
	ds "github.com/ipfs/go-datastore"
	u "github.com/ipfs/go-ipfs-util"
	"github.com/opentracing/opentracing-go"

	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/routing"
	recpb "github.com/libp2p/go-libp2p-record/pb"

	ci "github.com/libp2p/go-libp2p-core/crypto"
)

type pubkrs struct {
	pubk ci.PubKey
	err  error
}

func (dht *IpfsDHT) GetPublicKey(ctx context.Context, p peer.ID) (ci.PubKey, error) {
	logger.Debugf("getPublicKey for: %s", p)

	sp, ctx := opentracing.StartSpanFromContext(ctx, "dht.get_public_key", opentracing.Tags{
		"peer.id": p,
	})
	defer sp.Finish()

	// Check locally. Will also try to extract the public key from the peer
	// ID itself if possible (if inlined).
	pk := dht.peerstore.PubKey(p)
	if pk != nil {
		sp.SetTag("provenance", "local")
		return pk, nil
	}

	sp.SetTag("provenance", "network")

	// Try getting the public key both directly from the node it identifies
	// and from the DHT, in parallel
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	resp := make(chan pubkrs, 2)
	go func() {
		pubk, err := dht.getPublicKeyFromNode(ctx, p)
		resp <- pubkrs{pubk, err}
	}()

	// Note that the number of open connections is capped by the dial
	// limiter, so there is a chance that getPublicKeyFromDHT(), which
	// potentially opens a lot of connections, will block
	// getPublicKeyFromNode() from getting a connection.
	// Currently this doesn't seem to cause an issue so leaving as is
	// for now.
	go func() {
		pubk, err := dht.getPublicKeyFromDHT(ctx, p)
		resp <- pubkrs{pubk, err}
	}()

	// Wait for one of the two go routines to return
	// a public key (or for both to error out).
	var err error
	for i := 0; i < 2; i++ {
		r := <-resp
		if r.err == nil {
			// Found the public key
			err := dht.peerstore.AddPubKey(p, r.pubk)
			if err != nil {
				logger.Warningf("Failed to add public key to peerstore for %v", p)
			}
			return r.pubk, nil
		}
		err = r.err
	}

	// Both go routines failed to find a public key
	return nil, err
}

func (dht *IpfsDHT) getPublicKeyFromDHT(ctx context.Context, p peer.ID) (ci.PubKey, error) {
	// Only retrieve one value, because the public key is immutable
	// so there's no need to retrieve multiple versions
	pkkey := routing.KeyForPublicKey(p)
	val, err := dht.GetValue(ctx, pkkey, Quorum(1))
	if err != nil {
		return nil, err
	}

	pubk, err := ci.UnmarshalPublicKey(val)
	if err != nil {
		logger.Errorf("Could not unmarshall public key retrieved from DHT for %v", p)
		return nil, err
	}

	// Note: No need to check that public key hash matches peer ID
	// because this is done by GetValues()
	logger.Debugf("Got public key for %s from DHT", p)
	return pubk, nil
}

func (dht *IpfsDHT) getPublicKeyFromNode(ctx context.Context, p peer.ID) (ci.PubKey, error) {
	// check locally, just in case...
	pk := dht.peerstore.PubKey(p)
	if pk != nil {
		return pk, nil
	}

	// Get the key from the node itself
	pkkey := routing.KeyForPublicKey(p)
	pmes, err := dht.getValueSingle(ctx, p, pkkey)
	if err != nil {
		return nil, err
	}

	// node doesn't have key :(
	record := pmes.GetRecord()
	if record == nil {
		return nil, fmt.Errorf("node %v not responding with its public key", p)
	}

	pubk, err := ci.UnmarshalPublicKey(record.GetValue())
	if err != nil {
		logger.Errorf("Could not unmarshall public key for %v", p)
		return nil, err
	}

	// Make sure the public key matches the peer ID
	id, err := peer.IDFromPublicKey(pubk)
	if err != nil {
		logger.Errorf("Could not extract peer id from public key for %v", p)
		return nil, err
	}
	if id != p {
		return nil, fmt.Errorf("public key %v does not match peer %v", id, p)
	}

	logger.Debugf("Got public key from node %v itself", p)
	return pubk, nil
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

func (dht *IpfsDHT) checkLocalDatastore(k []byte) (*recpb.Record, error) {
	logger.Debugf("%s handleGetValue looking into ds", dht.self)
	dskey := convertToDsKey(k)
	buf, err := dht.datastore.Get(dskey)
	logger.Debugf("%s handleGetValue looking into ds GOT %v", dht.self, buf)

	if err == ds.ErrNotFound {
		return nil, nil
	}

	// if we got an unexpected error, bail.
	if err != nil {
		return nil, err
	}

	// if we have the value, send it back
	logger.Debugf("%s handleGetValue success!", dht.self)

	rec := new(recpb.Record)
	err = proto.Unmarshal(buf, rec)
	if err != nil {
		logger.Debug("failed to unmarshal DHT record from datastore")
		return nil, err
	}

	var recordIsBad bool
	recvtime, err := u.ParseRFC3339(rec.GetTimeReceived())
	if err != nil {
		logger.Info("either no receive time set on record, or it was invalid: ", err)
		recordIsBad = true
	}

	if time.Since(recvtime) > dht.maxRecordAge {
		logger.Debug("old record found, tossing.")
		recordIsBad = true
	}

	// NOTE: We do not verify the record here beyond checking these timestamps.
	// we put the burden of checking the records on the requester as checking a record
	// may be computationally expensive

	if recordIsBad {
		err := dht.datastore.Delete(dskey)
		if err != nil {
			logger.Error("Failed to delete bad record from datastore: ", err)
		}

		return nil, nil // can treat this as not having the record at all
	}

	return rec, nil
}

// Cleans the record (to avoid storing arbitrary data).
func cleanRecord(rec *recpb.Record) {
	rec.TimeReceived = ""
}

// returns nil, nil when either nothing is found or the value found doesn't
// properly validate. returns nil, some_error when there's a *datastore* error
// (i.e., something goes very wrong)
func (dht *IpfsDHT) getRecordFromDatastore(dskey ds.Key) (*recpb.Record, error) {
	buf, err := dht.datastore.Get(dskey)
	if err == ds.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		logger.Errorf("Got error retrieving record with key %s from datastore: %s", dskey, err)
		return nil, err
	}
	rec := new(recpb.Record)
	err = proto.Unmarshal(buf, rec)
	if err != nil {
		// Bad data in datastore, log it but don't return an error, we'll just overwrite it
		logger.Errorf("Bad record data stored in datastore with key %s: could not unmarshal record", dskey)
		return nil, nil
	}

	err = dht.Validator.Validate(string(rec.GetKey()), rec.GetValue())
	if err != nil {
		// Invalid record in datastore, probably expired but don't return an error,
		// we'll just overwrite it
		logger.Debugf("Local record verify failed: %s (discarded)", err)
		return nil, nil
	}

	return rec, nil
}
