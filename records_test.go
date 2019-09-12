package dht

import (
	"context"
	"crypto/rand"
	"github.com/libp2p/go-libp2p-core/test"
	"testing"
	"time"

	u "github.com/ipfs/go-ipfs-util"
	ci "github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/routing"
	record "github.com/libp2p/go-libp2p-record"
	tnet "github.com/libp2p/go-libp2p-testing/net"
)

// Check that GetPublicKey() correctly extracts a public key
func TestPubkeyExtract(t *testing.T) {
	t.Skip("public key extraction for ed25519 keys has been disabled. See https://github.com/libp2p/specs/issues/111")
	ctx := context.Background()
	dht := setupDHT(ctx, t, false)
	defer dht.Close()

	_, pk, err := ci.GenerateEd25519Key(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	pid, err := peer.IDFromPublicKey(pk)
	if err != nil {
		t.Fatal(err)
	}

	pkOut, err := dht.GetPublicKey(context.Background(), pid)
	if err != nil {
		t.Fatal(err)
	}

	if !pkOut.Equals(pk) {
		t.Fatal("got incorrect public key out")
	}
}

// Check that GetPublicKey() correctly retrieves a public key from the peerstore
func TestPubkeyPeerstore(t *testing.T) {
	ctx := context.Background()
	dht := setupDHT(ctx, t, false)

	identity := tnet.RandIdentityOrFatal(t)
	err := dht.peerstore.AddPubKey(identity.ID(), identity.PublicKey())
	if err != nil {
		t.Fatal(err)
	}

	rpubk, err := dht.GetPublicKey(context.Background(), identity.ID())
	if err != nil {
		t.Fatal(err)
	}

	if !identity.PublicKey().Equals(rpubk) {
		t.Fatal("got incorrect public key")
	}
}

// Check that GetPublicKey() correctly retrieves a public key directly
// from the node it identifies
func TestPubkeyDirectFromNode(t *testing.T) {
	ctx := context.Background()

	dhtA := setupDHT(ctx, t, false)
	dhtB := setupDHT(ctx, t, false)

	defer dhtA.Close()
	defer dhtB.Close()
	defer dhtA.host.Close()
	defer dhtB.host.Close()

	connect(t, ctx, dhtA, dhtB)

	pubk, err := dhtA.GetPublicKey(context.Background(), dhtB.self)
	if err != nil {
		t.Fatal(err)
	}

	id, err := peer.IDFromPublicKey(pubk)
	if err != nil {
		t.Fatal(err)
	}

	if id != dhtB.self {
		t.Fatal("got incorrect public key")
	}
}

// Check that GetPublicKey() correctly retrieves a public key
// from the DHT
func TestPubkeyFromDHT(t *testing.T) {
	ctx := context.Background()

	dhtA := setupDHT(ctx, t, false)
	dhtB := setupDHT(ctx, t, false)

	defer dhtA.Close()
	defer dhtB.Close()
	defer dhtA.host.Close()
	defer dhtB.host.Close()

	connect(t, ctx, dhtA, dhtB)

	identity := tnet.RandIdentityOrFatal(t)
	pubk := identity.PublicKey()
	id := identity.ID()
	pkkey := routing.KeyForPublicKey(id)
	pkbytes, err := pubk.Bytes()
	if err != nil {
		t.Fatal(err)
	}

	// Store public key on node B
	err = dhtB.PutValue(ctx, pkkey, pkbytes)
	if err != nil {
		t.Fatal(err)
	}

	// Retrieve public key on node A
	rpubk, err := dhtA.GetPublicKey(ctx, id)
	if err != nil {
		t.Fatal(err)
	}

	if !pubk.Equals(rpubk) {
		t.Fatal("got incorrect public key")
	}
}

// Check that GetPublicKey() correctly returns an error when the
// public key is not available directly from the node or on the DHT
func TestPubkeyNotFound(t *testing.T) {
	ctx := context.Background()

	dhtA := setupDHT(ctx, t, false)
	dhtB := setupDHT(ctx, t, false)

	defer dhtA.Close()
	defer dhtB.Close()
	defer dhtA.host.Close()
	defer dhtB.host.Close()

	connect(t, ctx, dhtA, dhtB)

	r := u.NewSeededRand(15) // generate deterministic keypair
	_, pubk, err := ci.GenerateKeyPairWithReader(ci.RSA, 2048, r)
	if err != nil {
		t.Fatal(err)
	}
	id, err := peer.IDFromPublicKey(pubk)
	if err != nil {
		t.Fatal(err)
	}

	// Attempt to retrieve public key on node A (should be not found)
	_, err = dhtA.GetPublicKey(ctx, id)
	if err == nil {
		t.Fatal("Expected not found error")
	}
}

// Check that GetPublicKey() returns an error when
// the DHT returns the wrong key
func TestPubkeyBadKeyFromDHT(t *testing.T) {
	ctx := context.Background()

	dhtA := setupDHT(ctx, t, false)
	dhtB := setupDHT(ctx, t, false)

	defer dhtA.Close()
	defer dhtB.Close()
	defer dhtA.host.Close()
	defer dhtB.host.Close()

	connect(t, ctx, dhtA, dhtB)

	_, pk, err := test.RandTestKeyPair(ci.RSA, 2048)
	if err != nil {
		t.Fatal(err)
	}
	id, err := peer.IDFromPublicKey(pk)
	if err != nil {
		t.Fatal(err)
	}
	pkkey := routing.KeyForPublicKey(id)

	peer2 := tnet.RandIdentityOrFatal(t)
	if pk == peer2.PublicKey() {
		t.Fatal("Public keys shouldn't match here")
	}
	wrongbytes, err := peer2.PublicKey().Bytes()
	if err != nil {
		t.Fatal(err)
	}

	// Store incorrect public key on node B
	rec := record.MakePutRecord(pkkey, wrongbytes)
	rec.TimeReceived = u.FormatRFC3339(time.Now())
	err = dhtB.putLocal(pkkey, rec)
	if err != nil {
		t.Fatal(err)
	}

	// Retrieve public key from node A
	_, err = dhtA.GetPublicKey(ctx, id)
	if err == nil {
		t.Fatal("Expected error because public key is incorrect")
	}
}

// Check that GetPublicKey() returns the correct value
// when the DHT returns the wrong key but the direct
// connection returns the correct key
func TestPubkeyBadKeyFromDHTGoodKeyDirect(t *testing.T) {
	ctx := context.Background()

	dhtA := setupDHT(ctx, t, false)
	dhtB := setupDHT(ctx, t, false)

	defer dhtA.Close()
	defer dhtB.Close()
	defer dhtA.host.Close()
	defer dhtB.host.Close()

	connect(t, ctx, dhtA, dhtB)

	wrong := tnet.RandIdentityOrFatal(t)
	pkkey := routing.KeyForPublicKey(dhtB.self)

	wrongbytes, err := wrong.PublicKey().Bytes()
	if err != nil {
		t.Fatal(err)
	}

	// Store incorrect public key on node B
	rec := record.MakePutRecord(pkkey, wrongbytes)
	rec.TimeReceived = u.FormatRFC3339(time.Now())
	err = dhtB.putLocal(pkkey, rec)
	if err != nil {
		t.Fatal(err)
	}

	// Retrieve public key from node A
	pubk, err := dhtA.GetPublicKey(ctx, dhtB.self)
	if err != nil {
		t.Fatal(err)
	}

	id, err := peer.IDFromPublicKey(pubk)
	if err != nil {
		t.Fatal(err)
	}

	// The incorrect public key retrieved from the DHT
	// should be ignored in favour of the correct public
	// key retrieved from the node directly
	if id != dhtB.self {
		t.Fatal("got incorrect public key")
	}
}

// Check that GetPublicKey() returns the correct value
// when both the DHT returns the correct key and the direct
// connection returns the correct key
func TestPubkeyGoodKeyFromDHTGoodKeyDirect(t *testing.T) {
	ctx := context.Background()

	dhtA := setupDHT(ctx, t, false)
	dhtB := setupDHT(ctx, t, false)

	defer dhtA.Close()
	defer dhtB.Close()
	defer dhtA.host.Close()
	defer dhtB.host.Close()

	connect(t, ctx, dhtA, dhtB)

	pubk := dhtB.peerstore.PubKey(dhtB.self)
	pkbytes, err := pubk.Bytes()
	if err != nil {
		t.Fatal(err)
	}

	// Store public key on node B
	pkkey := routing.KeyForPublicKey(dhtB.self)
	err = dhtB.PutValue(ctx, pkkey, pkbytes)
	if err != nil {
		t.Fatal(err)
	}

	// Retrieve public key on node A
	rpubk, err := dhtA.GetPublicKey(ctx, dhtB.self)
	if err != nil {
		t.Fatal(err)
	}

	if !pubk.Equals(rpubk) {
		t.Fatal("got incorrect public key")
	}
}
