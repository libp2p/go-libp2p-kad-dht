package dht

import (
	"context"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
	recpb "github.com/libp2p/go-libp2p-record/pb"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/assert"
)

func TestExpireNonProviderRecords(t *testing.T) {
	// short sweep duration for testing
	sVal := nonProvRecordCleanupInterval
	defer func() { nonProvRecordCleanupInterval = sVal }()
	nonProvRecordCleanupInterval = 10 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// TEST expiry does not happen if age(record) < MaxAge

	dhtA := setupDHT(ctx, t, false)
	dhtB := setupDHT(ctx, t, false)
	connect(t, ctx, dhtA, dhtB)

	// dhtA puts non-provider record with current time which WILL get stored on B
	key1 := "/v/key1"
	value1 := []byte("v1")
	assert.NoError(t, dhtA.PutValue(ctx, key1, value1))

	// sweep will not delete it
	time.Sleep(100 * time.Millisecond)

	// get & verify it's present on B
	_, err := dhtB.datastore.Get(convertToDsKey([]byte(key1)))
	assert.NoError(t, err)

	// cleanup
	dhtA.Close()
	dhtA.host.Close()
	dhtB.Close()
	dhtB.host.Close()

	// TEST expiry happens if age(record) > MaxAge

	mVal := maxNonProvRecordAge
	maxNonProvRecordAge = 50 * time.Millisecond
	defer func() { maxNonProvRecordAge = mVal }()

	dhtA = setupDHT(ctx, t, false)
	dhtB = setupDHT(ctx, t, false)
	connect(t, ctx, dhtA, dhtB)
	defer func() {
		dhtA.Close()
		dhtA.host.Close()
		dhtB.Close()
		dhtB.host.Close()
	}()

	// dhtA puts non-provider record with current time
	assert.NoError(t, dhtA.PutValue(ctx, key1, value1))

	// dhtA adds provider record with current time
	mh, err := multihash.Sum([]byte("data"), multihash.SHA2_256, -1)
	assert.NoError(t, err)
	c := cid.NewCidV0(mh)
	assert.NoError(t, dhtA.Provide(ctx, c, true))

	// sweep will remove non-provider record on B now
	time.Sleep(1 * time.Second)

	// verify non-provider record is absent on B
	_, err = dhtB.datastore.Get(convertToDsKey([]byte(key1)))
	assert.Equal(t, ds.ErrNotFound, err)

	// but.... provider record is still available
	m, err := getTestProvRecord(t, ctx, dhtB, c)
	assert.NoError(t, err)
	assert.NotEmpty(t, m.ProviderPeers)
}

func TestRepublishNonProvRecords(t *testing.T) {
	// short scan duration for re-publish
	sVal := nonProvRecordRePublishInterval
	nonProvRecordRePublishInterval = 10 * time.Millisecond
	jVal := enableRepublishJitter
	enableRepublishJitter = false
	defer func() {
		enableRepublishJitter = jVal
		nonProvRecordRePublishInterval = sVal
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// TEST re-publish does not happen if age(record) < republishAge

	dhtA := setupDHT(ctx, t, false)
	dhtB := setupDHT(ctx, t, false)
	connect(t, ctx, dhtA, dhtB)

	// dhtA puts non-provider record with current time which WILL get stored on B
	key1 := "/v/key1"
	value1 := []byte("v1")
	assert.NoError(t, dhtA.PutValue(ctx, key1, value1))

	// dhtA DELETES it locally
	assert.NoError(t, dhtA.datastore.Delete(convertToDsKey([]byte(key1))))

	// it will not  be re-published by B
	time.Sleep(2 * time.Second)

	// get on dhtA & verify it's absent
	_, err := dhtA.datastore.Get(convertToDsKey([]byte(key1)))
	assert.Equal(t, ds.ErrNotFound, err)

	// cleanup
	dhtA.Close()
	dhtA.host.Close()
	dhtB.Close()
	dhtB.host.Close()

	// TEST re-publish happens if age(record) > republishAge

	mVal := nonProvRecordRePublishAge
	nonProvRecordRePublishAge = 100 * time.Millisecond
	defer func() { nonProvRecordRePublishAge = mVal }()

	dhtA = setupDHT(ctx, t, false)
	dhtB = setupDHT(ctx, t, false)
	connect(t, ctx, dhtA, dhtB)
	defer func() {
		dhtA.Close()
		dhtA.host.Close()
		dhtB.Close()
		dhtB.host.Close()
	}()

	// dhtA puts non-provider record with current time
	assert.NoError(t, dhtA.PutValue(ctx, key1, value1))

	// dhtA DELETES it locally
	assert.NoError(t, dhtA.datastore.Delete(convertToDsKey([]byte(key1))))

	// it will be re-published by B
	time.Sleep(2 * time.Second)

	// get on dhtA & verify key is present (because it SHOULD have been re-published by B)
	v, err := dhtA.datastore.Get(convertToDsKey([]byte(key1)))
	assert.NoError(t, err)
	rec := new(recpb.Record)
	assert.NoError(t, proto.Unmarshal(v, rec))
	assert.Equal(t, value1, rec.Value)
}

func getTestProvRecord(t *testing.T, ctx context.Context, d *IpfsDHT, c cid.Cid) (*pb.Message, error) {
	pmes := pb.NewMessage(pb.Message_GET_PROVIDERS, c.Bytes(), 0)
	m, err := d.handleGetProviders(ctx, "test peer", pmes)
	return m, err
}
