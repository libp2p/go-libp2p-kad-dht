package dht

import (
	"testing"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/libp2p/go-libp2p-kad-dht/v2/internal/kadtest"
)

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
