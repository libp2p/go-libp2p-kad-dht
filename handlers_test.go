package dht

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
	crypto "github.com/libp2p/go-libp2p/core/crypto"
	peer "github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

func TestBadMessage(t *testing.T) {
	ctx := t.Context()

	dht := setupDHT(ctx, t, false)

	for _, typ := range []pb.Message_MessageType{
		pb.Message_PUT_VALUE, pb.Message_GET_VALUE, pb.Message_ADD_PROVIDER,
		pb.Message_GET_PROVIDERS, pb.Message_FIND_NODE,
	} {
		msg := &pb.Message{
			Type: typ,
			// explicitly avoid the key.
		}
		_, err := dht.handlerForMsgType(typ)(ctx, dht.Host().ID(), msg)
		if err == nil {
			t.Fatalf("expected processing message to fail for type %s", pb.Message_FIND_NODE)
		}
	}
}

// TestHandlerForMsgTypeGatedByStorePresence checks that value/provider RPC
// support follows the presence of the corresponding store: a disabled subsystem
// has no store, so its message types resolve to no handler (unsupported RPC).
func TestHandlerForMsgTypeGatedByStorePresence(t *testing.T) {
	ctx := t.Context()

	valueTypes := []pb.Message_MessageType{pb.Message_GET_VALUE, pb.Message_PUT_VALUE}
	providerTypes := []pb.Message_MessageType{pb.Message_ADD_PROVIDER, pb.Message_GET_PROVIDERS}

	assertSupport := func(t *testing.T, d *IpfsDHT, types []pb.Message_MessageType, want bool) {
		t.Helper()
		for _, mt := range types {
			if supported := d.handlerForMsgType(mt) != nil; supported != want {
				t.Fatalf("handler for %s: supported=%v, want %v", mt, supported, want)
			}
		}
	}

	t.Run("all enabled", func(t *testing.T) {
		d := setupDHT(ctx, t, false)
		if d.valueStore == nil || d.providerStore == nil {
			t.Fatal("both stores should be present when enabled")
		}
		assertSupport(t, d, valueTypes, true)
		assertSupport(t, d, providerTypes, true)
	})

	t.Run("values disabled", func(t *testing.T) {
		d := setupDHT(ctx, t, false, DisableValues())
		if d.valueStore != nil {
			t.Fatal("value store should be absent when disabled")
		}
		assertSupport(t, d, valueTypes, false)
		assertSupport(t, d, providerTypes, true)
	})

	t.Run("providers disabled", func(t *testing.T) {
		d := setupDHT(ctx, t, false, DisableProviders())
		if d.providerStore != nil {
			t.Fatal("provider store should be absent when disabled")
		}
		assertSupport(t, d, providerTypes, false)
		assertSupport(t, d, valueTypes, true)
	})

	t.Run("both disabled", func(t *testing.T) {
		// Both stores absent: only FIND_NODE/PING remain; Close (via setupDHT's
		// cleanup) must tolerate the both-nil case.
		d := setupDHT(ctx, t, false, DisableValues(), DisableProviders())
		if d.valueStore != nil || d.providerStore != nil {
			t.Fatal("both stores should be absent when disabled")
		}
		assertSupport(t, d, valueTypes, false)
		assertSupport(t, d, providerTypes, false)
	})
}

func BenchmarkHandleFindPeer(b *testing.B) {
	ctx := b.Context()
	h, err := libp2p.New()
	if err != nil {
		b.Fatal(err)
	}
	defer h.Close()

	d, err := New(ctx, h)
	if err != nil {
		b.Fatal(err)
	}

	rng := rand.New(rand.NewSource(150))
	var peers []peer.ID
	for i := range 1000 {
		_, pubk, _ := crypto.GenerateEd25519Key(rng)
		id, err := peer.IDFromPublicKey(pubk)
		if err != nil {
			panic(err)
		}

		d.peerFound(id)

		peers = append(peers, id)
		a, err := ma.NewMultiaddr(fmt.Sprintf("/ip4/127.0.0.1/tcp/%d", 2000+i))
		if err != nil {
			panic(err)
		}

		d.host.Peerstore().AddAddr(id, a, time.Minute*50)
	}

	var reqs []*pb.Message
	for i := 0; i < b.N; i++ {
		reqs = append(reqs, &pb.Message{
			Key: []byte("asdasdasd"),
		})
	}
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err = d.handleFindPeer(ctx, peers[0], reqs[i])
		if err != nil {
			b.Error(err)
		}
	}

}
