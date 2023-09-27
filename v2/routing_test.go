package dht

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore/failstore"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"
	mh "github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/libp2p/go-libp2p-kad-dht/v2/internal/kadtest"
)

// newRandomContent reads 1024 bytes from crypto/rand and builds a content struct.
func newRandomContent(t testing.TB) cid.Cid {
	raw := make([]byte, 1024)
	_, err := rand.Read(raw)
	require.NoError(t, err)

	hash := sha256.New()
	hash.Write(raw)

	mhash, err := mh.Encode(hash.Sum(nil), mh.SHA2_256)
	require.NoError(t, err)

	return cid.NewCidV0(mhash)
}

func makePkKeyValue(t testing.TB) (string, []byte) {
	t.Helper()

	_, pub, _ := crypto.GenerateEd25519Key(rng)
	v, err := crypto.MarshalPublicKey(pub)
	require.NoError(t, err)

	id, err := peer.IDFromPublicKey(pub)
	require.NoError(t, err)

	return routing.KeyForPublicKey(id), v
}

func TestDHT_PutValue_local_only(t *testing.T) {
	ctx := kadtest.CtxShort(t)

	top := NewTopology(t)
	d := top.AddServer(nil)

	key, v := makePkKeyValue(t)

	err := d.PutValue(ctx, key, v, routing.Offline)
	require.NoError(t, err)
}

func TestDHT_PutValue_invalid_key(t *testing.T) {
	ctx := kadtest.CtxShort(t)

	top := NewTopology(t)
	d := top.AddClient(nil)

	_, v := makePkKeyValue(t)

	t.Run("unknown namespace", func(t *testing.T) {
		err := d.PutValue(ctx, "/unknown/some_key", v)
		assert.ErrorIs(t, err, routing.ErrNotSupported)
	})

	t.Run("no namespace", func(t *testing.T) {
		err := d.PutValue(ctx, "no namespace", v)
		assert.ErrorContains(t, err, "splitting key")
	})
}

//func TestGetSetValueLocal(t *testing.T) {
//	ctx := kadtest.CtxShort(t)
//
//	top := NewTopology(t)
//	d := top.AddServer(nil)
//
//	key, v := makePkKeyValue(t)
//
//	err := d.putValueLocal(ctx, key, v)
//	require.NoError(t, err)
//
//	val, err := d.getValueLocal(ctx, key)
//	require.NoError(t, err)
//
//	require.Equal(t, v, val)
//}

func TestGetValueOnePeer(t *testing.T) {
	ctx := kadtest.CtxShort(t)
	top := NewTopology(t)
	local := top.AddServer(nil)
	remote := top.AddServer(nil)

	// store the value on the remote DHT
	key, v := makePkKeyValue(t)
	err := remote.putValueLocal(ctx, key, v)
	require.NoError(t, err)

	// connect the two DHTs
	top.Connect(ctx, local, remote)

	// ask the local DHT to find the value
	val, err := local.GetValue(ctx, key)
	require.NoError(t, err)

	require.Equal(t, v, val)
}

func TestDHT_Provide_no_providers_backend_registered(t *testing.T) {
	ctx := kadtest.CtxShort(t)
	d := newTestDHT(t)

	delete(d.backends, namespaceProviders)
	err := d.Provide(ctx, newRandomContent(t), true)
	assert.ErrorIs(t, err, routing.ErrNotSupported)
}

func TestDHT_Provide_undefined_cid(t *testing.T) {
	ctx := kadtest.CtxShort(t)
	d := newTestDHT(t)

	err := d.Provide(ctx, cid.Cid{}, true)
	assert.ErrorContains(t, err, "invalid cid")
}

func TestDHT_Provide_erroneous_datastore(t *testing.T) {
	ctx := kadtest.CtxShort(t)
	d := newTestDHT(t)

	testErr := fmt.Errorf("some error")

	// construct a datastore that fails for any operation
	memStore, err := InMemoryDatastore()
	require.NoError(t, err)

	dstore := failstore.NewFailstore(memStore, func(s string) error {
		return testErr
	})

	be, err := typedBackend[*ProvidersBackend](d, namespaceProviders)
	require.NoError(t, err)

	be.datastore = dstore

	err = d.Provide(ctx, newRandomContent(t), true)
	assert.ErrorIs(t, err, testErr)
}

func TestDHT_Provide_does_nothing_if_broadcast_is_false(t *testing.T) {
	ctx := kadtest.CtxShort(t)
	d := newTestDHT(t) // unconnected DHT

	c := newRandomContent(t)
	err := d.Provide(ctx, c, false)
	assert.NoError(t, err)

	// still stored locally
	be, err := typedBackend[*ProvidersBackend](d, namespaceProviders)
	require.NoError(t, err)
	val, err := be.Fetch(ctx, string(c.Hash()))
	require.NoError(t, err)

	ps, ok := val.(*providerSet)
	require.True(t, ok)
	require.Len(t, ps.providers, 1)
	assert.Equal(t, d.host.ID(), ps.providers[0].ID)
}

func TestDHT_Provide_fails_if_routing_table_is_empty(t *testing.T) {
	ctx := kadtest.CtxShort(t)
	d := newTestDHT(t)

	err := d.Provide(ctx, newRandomContent(t), true)
	assert.Error(t, err)
}

func TestDHT_FindProvidersAsync_empty_routing_table(t *testing.T) {
	ctx := kadtest.CtxShort(t)
	d := newTestDHT(t)
	c := newRandomContent(t)

	out := d.FindProvidersAsync(ctx, c, 1)
	assertClosed(t, ctx, out)
}

func TestDHT_FindProvidersAsync_dht_does_not_support_providers(t *testing.T) {
	ctx := kadtest.CtxShort(t)
	d := newTestDHT(t)
	fillRoutingTable(t, d, 250)

	delete(d.backends, namespaceProviders)

	out := d.FindProvidersAsync(ctx, newRandomContent(t), 1)
	assertClosed(t, ctx, out)
}

func TestDHT_FindProvidersAsync_providers_stored_locally(t *testing.T) {
	ctx := kadtest.CtxShort(t)
	d := newTestDHT(t)
	fillRoutingTable(t, d, 250)

	c := newRandomContent(t)
	provider := peer.AddrInfo{ID: newPeerID(t)}
	_, err := d.backends[namespaceProviders].Store(ctx, string(c.Hash()), provider)
	require.NoError(t, err)

	out := d.FindProvidersAsync(ctx, c, 1)
	for {
		select {
		case p, more := <-out:
			if !more {
				return
			}
			assert.Equal(t, provider.ID, p.ID)
		case <-ctx.Done():
			t.Fatal("timeout")
		}
	}
}

func TestDHT_FindProvidersAsync_returns_only_count_from_local_store(t *testing.T) {
	ctx := kadtest.CtxShort(t)
	d := newTestDHT(t)
	fillRoutingTable(t, d, 250)

	c := newRandomContent(t)

	storedCount := 5
	requestedCount := 3

	// invariant for this test
	assert.Less(t, requestedCount, storedCount)

	for i := 0; i < storedCount; i++ {
		provider := peer.AddrInfo{ID: newPeerID(t)}
		_, err := d.backends[namespaceProviders].Store(ctx, string(c.Hash()), provider)
		require.NoError(t, err)
	}

	out := d.FindProvidersAsync(ctx, c, requestedCount)

	returnedCount := 0
LOOP:
	for {
		select {
		case _, more := <-out:
			if !more {
				break LOOP
			}
			returnedCount += 1
		case <-ctx.Done():
			t.Fatal("timeout")
		}
	}
	assert.Equal(t, requestedCount, returnedCount)
}

func TestDHT_FindProvidersAsync_queries_other_peers(t *testing.T) {
	ctx := kadtest.CtxShort(t)

	c := newRandomContent(t)

	top := NewTopology(t)
	d1 := top.AddServer(nil)
	d2 := top.AddServer(nil)
	d3 := top.AddServer(nil)

	top.ConnectChain(ctx, d1, d2, d3)

	provider := peer.AddrInfo{ID: newPeerID(t)}
	_, err := d3.backends[namespaceProviders].Store(ctx, string(c.Hash()), provider)
	require.NoError(t, err)

	out := d1.FindProvidersAsync(ctx, c, 1)
	select {
	case p, more := <-out:
		require.True(t, more)
		assert.Equal(t, provider.ID, p.ID)
	case <-ctx.Done():
		t.Fatal("timeout")
	}

	assertClosed(t, ctx, out)
}

func TestDHT_FindProvidersAsync_respects_cancelled_context_for_local_query(t *testing.T) {
	// Test strategy:
	// We let d know about providersCount providers for the CID c
	// Then we ask it to find providers but pass it a cancelled context.
	// We assert that we are sending on the channel while also respecting a
	// cancelled context by checking if the number of returned providers is
	// less than the number of providers d knows about. Since it's random
	// which channel gets selected on, providersCount must be a significantly
	// large. This is a statistical test, and we should observe if it's a flaky
	// one.
	ctx := kadtest.CtxShort(t)
	d := newTestDHT(t)

	c := newRandomContent(t)

	providersCount := 50
	for i := 0; i < providersCount; i++ {
		provider := peer.AddrInfo{ID: newPeerID(t)}
		_, err := d.backends[namespaceProviders].Store(ctx, string(c.Hash()), provider)
		require.NoError(t, err)
	}

	cancelledCtx, cancel := context.WithCancel(ctx)
	cancel()

	out := d.FindProvidersAsync(cancelledCtx, c, 0)

	returnedCount := 0
LOOP:
	for {
		select {
		case _, more := <-out:
			if !more {
				break LOOP
			}
			returnedCount += 1
		case <-ctx.Done():
			t.Fatal("timeout")
		}
	}
	assert.Less(t, returnedCount, providersCount)
}

func TestDHT_FindProvidersAsync_does_not_return_same_record_twice(t *testing.T) {
	// Test setup:
	// There are two providers in the network for CID c.
	// d1 has information about one provider locally.
	// d2 has information about two providers. One of which is the one d1 knew about.
	// We assert that the locally known provider is only returned once.
	// The query should run until exhaustion.
	ctx := kadtest.CtxShort(t)

	c := newRandomContent(t)

	top := NewTopology(t)
	d1 := top.AddServer(nil)
	d2 := top.AddServer(nil)

	top.Connect(ctx, d1, d2)

	provider1 := peer.AddrInfo{ID: newPeerID(t)}
	provider2 := peer.AddrInfo{ID: newPeerID(t)}

	// store provider1 with d1
	_, err := d1.backends[namespaceProviders].Store(ctx, string(c.Hash()), provider1)
	require.NoError(t, err)

	// store provider1 with d2
	_, err = d2.backends[namespaceProviders].Store(ctx, string(c.Hash()), provider1)
	require.NoError(t, err)

	// store provider2 with d2
	_, err = d2.backends[namespaceProviders].Store(ctx, string(c.Hash()), provider2)
	require.NoError(t, err)

	out := d1.FindProvidersAsync(ctx, c, 0)
	count := 0
LOOP:
	for {
		select {
		case p, more := <-out:
			if !more {
				break LOOP
			}
			count += 1
			assert.True(t, p.ID == provider1.ID || p.ID == provider2.ID)
		case <-ctx.Done():
			t.Fatal("timeout")
		}
	}
	assert.Equal(t, 2, count)
}

func TestDHT_FindProvidersAsync_datastore_error(t *testing.T) {
	ctx := kadtest.CtxShort(t)
	d := newTestDHT(t)

	// construct a datastore that fails for any operation
	memStore, err := InMemoryDatastore()
	require.NoError(t, err)

	dstore := failstore.NewFailstore(memStore, func(s string) error {
		return fmt.Errorf("some error")
	})

	be, err := typedBackend[*ProvidersBackend](d, namespaceProviders)
	require.NoError(t, err)

	be.datastore = dstore

	out := d.FindProvidersAsync(ctx, newRandomContent(t), 0)
	assertClosed(t, ctx, out)
}

func TestDHT_FindProvidersAsync_invalid_key(t *testing.T) {
	ctx := kadtest.CtxShort(t)
	d := newTestDHT(t)

	out := d.FindProvidersAsync(ctx, cid.Cid{}, 0)
	assertClosed(t, ctx, out)
}

// assertClosed triggers a test failure if the given channel was not closed but
// carried more values or a timeout occurs (given by the context).
func assertClosed[T any](t testing.TB, ctx context.Context, c <-chan T) {
	t.Helper()

	select {
	case _, more := <-c:
		assert.False(t, more)
	case <-ctx.Done():
		t.Fatal("timeout closing channel")
	}
}

func readItem[T any](t testing.TB, ctx context.Context, c <-chan T) T {
	t.Helper()

	select {
	case val, more := <-c:
		require.True(t, more, "channel closed unexpectedly")
		return val
	case <-ctx.Done():
		t.Fatal("timeout reading item")
		return *new(T)
	}
}

func TestDHT_SearchValue_simple(t *testing.T) {
	// Test setup:
	// There is just one other server that returns a valid value.
	ctx := kadtest.CtxShort(t)

	key, v := makePkKeyValue(t)

	top := NewTopology(t)
	d1 := top.AddServer(nil)
	d2 := top.AddServer(nil)

	top.Connect(ctx, d1, d2)

	err := d2.putValueLocal(ctx, key, v)
	require.NoError(t, err)

	valChan, err := d1.SearchValue(ctx, key)
	require.NoError(t, err)

	val := readItem(t, ctx, valChan)
	assert.Equal(t, v, val)

	assertClosed(t, ctx, valChan)
}

func TestDHT_SearchValue_returns_best_values(t *testing.T) {
	// Test setup:
	// d2 returns no value
	// d3 returns valid value
	// d4 returns worse value
	// d5 returns better value
	// assert that we receive two values on the channel (valid + better)
	ctx := kadtest.CtxShort(t)

	clk := clock.NewMock()
	clk.Set(time.Now()) // needed because record validators don't use mock clocks

	cfg := DefaultConfig()
	cfg.Clock = clk

	// generate new identity for the peer that issues the request
	priv, _, err := crypto.GenerateEd25519Key(rng)
	require.NoError(t, err)

	key, validValue := makeIPNSKeyValue(t, clk, priv, 1, time.Hour)
	key, worseValue := makeIPNSKeyValue(t, clk, priv, 0, time.Hour)
	key, betterValue := makeIPNSKeyValue(t, clk, priv, 2, time.Hour) // higher sequence number means better value

	top := NewTopology(t)
	d1 := top.AddServer(cfg)
	d2 := top.AddServer(cfg)
	d3 := top.AddServer(cfg)
	d4 := top.AddServer(cfg)
	d5 := top.AddServer(cfg)

	top.ConnectChain(ctx, d1, d2, d3, d4, d5)

	err = d3.putValueLocal(ctx, key, validValue)
	require.NoError(t, err)

	err = d4.putValueLocal(ctx, key, worseValue)
	require.NoError(t, err)

	err = d5.putValueLocal(ctx, key, betterValue)
	require.NoError(t, err)

	valChan, err := d1.SearchValue(ctx, key)
	require.NoError(t, err)

	val := readItem(t, ctx, valChan)
	assert.Equal(t, validValue, val)

	val = readItem(t, ctx, valChan)
	assert.Equal(t, betterValue, val)

	assertClosed(t, ctx, valChan)
}

func TestDHT_SearchValue_stop_query_after_quorum_reached(t *testing.T) {
	// Test setup:
	// we create 1 DHT server does searches for values
	// we create 10 additional DHT servers and connect all of them in a chain
	// the first server holds an invalid record
	// the next three servers of the 10 DHT servers hold a valid record
	// the remaining 6 servers of the 10 DHT servers hold a better record
	// first test assertion: with quorum of 3 we expect the valid but old record
	// second test assertion: with a quorum of 5 we expect to receive the valid but also better record.
	ctx := context.Background() // kadtest.CtxShort(t)

	clk := clock.NewMock()
	clk.Set(time.Now()) // needed because record validators don't use mock clocks

	cfg := DefaultConfig()
	cfg.Clock = clk
	top := NewTopology(t)

	// init privileged DHT server
	d := top.AddServer(cfg)

	// init remaining ones
	servers := make([]*DHT, 10)
	for i := 0; i < 10; i++ {
		servers[i] = top.AddServer(cfg)
	}

	// generate records
	remote, priv := newIdentity(t)
	invalidPutReq := newPutIPNSRequest(t, clk, priv, 3, -time.Hour)
	key, validValue := makeIPNSKeyValue(t, clk, priv, 1, time.Hour)
	key, betterValue := makeIPNSKeyValue(t, clk, priv, 2, time.Hour) // higher sequence number means better value

	// store invalid (expired) record directly in the datastore of
	// the respective DHT server (bypassing any validation).
	invalidRec, err := invalidPutReq.Record.Marshal()
	require.NoError(t, err)

	rbe, err := typedBackend[*RecordBackend](servers[0], namespaceIPNS)
	require.NoError(t, err)

	dsKey := newDatastoreKey(namespaceIPNS, string(remote))
	err = rbe.datastore.Put(context.Background(), dsKey, invalidRec)
	require.NoError(t, err)

	// The first four DHT servers hold a valid but old value
	for i := 1; i < 4; i++ {
		err = servers[i].putValueLocal(ctx, key, validValue)
		require.NoError(t, err)
	}

	// The remaining six DHT servers hold a valid and newer record
	for i := 4; i < 10; i++ {
		err = servers[i].putValueLocal(ctx, key, betterValue)
		require.NoError(t, err)
	}

	// connect all together
	top.ConnectChain(ctx, append([]*DHT{d}, servers...)...)

	t.Run("quorum reached prematurely", func(t *testing.T) {
		out, err := d.SearchValue(ctx, key, RoutingQuorum(3))
		require.NoError(t, err)

		val := readItem(t, ctx, out)
		assert.Equal(t, validValue, val)

		assertClosed(t, ctx, out)
	})

	t.Run("quorum reached when better value discovered", func(t *testing.T) {
		out, err := d.SearchValue(ctx, key, RoutingQuorum(5))
		require.NoError(t, err)

		val := readItem(t, ctx, out)
		assert.Equal(t, validValue, val)

		val = readItem(t, ctx, out)
		assert.Equal(t, betterValue, val)

		assertClosed(t, ctx, out)
	})

	//t.Run("zero quorum", func(t *testing.T) {
	//	// search until query exhausted
	//	out, err := d.SearchValue(ctx, key, RoutingQuorum(0))
	//	require.NoError(t, err)
	//
	//	val := readItem(t, ctx, out)
	//	assert.Equal(t, validValue, val)
	//
	//	val = readItem(t, ctx, out)
	//	assert.Equal(t, betterValue, val)
	//
	//	assertClosed(t, ctx, out)
	//})
}

func TestDHT_SearchValue_routing_option_returns_error(t *testing.T) {
	ctx := kadtest.CtxShort(t)
	d := newTestDHT(t)

	errOption := func(opts *routing.Options) error {
		return fmt.Errorf("some error")
	}

	valueChan, err := d.SearchValue(ctx, "some-key", errOption)
	assert.ErrorContains(t, err, "routing options")
	assert.Nil(t, valueChan)
}

func TestDHT_SearchValue_invalid_key(t *testing.T) {
	ctx := kadtest.CtxShort(t)
	d := newTestDHT(t)

	valueChan, err := d.SearchValue(ctx, "invalid-key")
	assert.ErrorContains(t, err, "splitting key")
	assert.Nil(t, valueChan)
}

func TestDHT_SearchValue_key_for_unsupported_namespace(t *testing.T) {
	ctx := kadtest.CtxShort(t)
	d := newTestDHT(t)

	valueChan, err := d.SearchValue(ctx, "/unsupported/key")
	assert.ErrorIs(t, err, routing.ErrNotSupported)
	assert.Nil(t, valueChan)
}

func TestDHT_SearchValue_offline_quorum_mismatch(t *testing.T) {
	t.Skip("not implemented")
	// return an error if the error flag is set but the user requires a quorum
}

func TestDHT_SearchValue_peers_return_old_records(t *testing.T) {
	t.Skip("not implemented")
}
