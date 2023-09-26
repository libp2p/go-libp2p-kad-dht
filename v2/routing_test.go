package dht

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"testing"

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

func makePkKeyValue(t *testing.T) (string, []byte) {
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

func TestGetSetValueLocal(t *testing.T) {
	ctx := kadtest.CtxShort(t)

	top := NewTopology(t)
	d := top.AddServer(nil)

	key, v := makePkKeyValue(t)

	err := d.putValueLocal(ctx, key, v)
	require.NoError(t, err)

	val, err := d.getValueLocal(ctx, key)
	require.NoError(t, err)

	require.Equal(t, v, val)
}

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
	select {
	case _, more := <-out:
		require.False(t, more)
	case <-ctx.Done():
		t.Fatal("timeout")
	}
}

func TestDHT_FindProvidersAsync_dht_does_not_support_providers(t *testing.T) {
	ctx := kadtest.CtxShort(t)
	d := newTestDHT(t)
	fillRoutingTable(t, d, 250)

	delete(d.backends, namespaceProviders)

	out := d.FindProvidersAsync(ctx, newRandomContent(t), 1)
	select {
	case _, more := <-out:
		require.False(t, more)
	case <-ctx.Done():
		t.Fatal("timeout")
	}
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

	select {
	case _, more := <-out:
		assert.False(t, more)
	case <-ctx.Done():
		t.Fatal("timeout")
	}
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
	select {
	case _, more := <-out:
		assert.False(t, more)
	case <-ctx.Done():
		t.Fatal("timeout")
	}
}

func TestDHT_FindProvidersAsync_invalid_key(t *testing.T) {
	ctx := kadtest.CtxShort(t)
	d := newTestDHT(t)

	out := d.FindProvidersAsync(ctx, cid.Cid{}, 0)
	select {
	case _, more := <-out:
		assert.False(t, more)
	case <-ctx.Done():
		t.Fatal("timeout")
	}
}

func TestDHT_SearchValue(t *testing.T) {
	// 1.
}
