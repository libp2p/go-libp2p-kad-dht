package dht

import (
	"context"
	"crypto/rand"
	"testing"

	ci "github.com/libp2p/go-libp2p-crypto"
	peer "github.com/libp2p/go-libp2p-peer"
)

func TestPubkeyExtract(t *testing.T) {
	_, pk, err := ci.GenerateEd25519Key(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	pid, err := peer.IDFromEd25519PublicKey(pk)
	if err != nil {
		t.Fatal(err)
	}

	// no need to actually construct one
	d := new(IpfsDHT)

	pk_out, err := d.GetPublicKey(context.Background(), pid)
	if err != nil {
		t.Fatal(err)
	}

	if !pk_out.Equals(pk) {
		t.Fatal("got incorrect public key out")
	}
}
