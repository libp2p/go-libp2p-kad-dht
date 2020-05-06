package dht

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/peerstore"
	"github.com/libp2p/go-libp2p-core/protocol"
	"github.com/libp2p/go-libp2p-core/routing"

	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
	record "github.com/libp2p/go-libp2p-record"
	swarmt "github.com/libp2p/go-libp2p-swarm/testing"
	bhost "github.com/libp2p/go-libp2p/p2p/host/basic"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"

	ggio "github.com/gogo/protobuf/io"
	u "github.com/ipfs/go-ipfs-util"
)

// Test that one hung request to a peer doesn't prevent another request
// using that same peer from obeying its context.
func TestHungRequest(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mn, err := mocknet.FullMeshLinked(ctx, 2)
	if err != nil {
		t.Fatal(err)
	}
	hosts := mn.Hosts()

	os := []Option{testPrefix, DisableAutoRefresh(), Mode(ModeServer)}
	d, err := New(ctx, hosts[0], os...)
	if err != nil {
		t.Fatal(err)
	}
	for _, proto := range d.serverProtocols {
		// Hang on every request.
		hosts[1].SetStreamHandler(proto, func(s network.Stream) {
			defer s.Reset() //nolint
			<-ctx.Done()
		})
	}

	err = mn.ConnectAllButSelf()
	if err != nil {
		t.Fatal("failed to connect peers", err)
	}

	// Wait at a bit for a peer in our routing table.
	for i := 0; i < 100 && d.routingTable.Size() == 0; i++ {
		time.Sleep(10 * time.Millisecond)
	}
	if d.routingTable.Size() == 0 {
		t.Fatal("failed to fill routing table")
	}

	ctx1, cancel1 := context.WithTimeout(ctx, 1*time.Second)
	defer cancel1()

	done := make(chan error, 1)
	go func() {
		_, err := d.GetClosestPeers(ctx1, testCaseCids[0].KeyString())
		done <- err
	}()

	time.Sleep(100 * time.Millisecond)
	ctx2, cancel2 := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel2()
	err = d.Provide(ctx2, testCaseCids[0], true)
	if err != context.DeadlineExceeded {
		t.Errorf("expected to fail with deadline exceeded, got: %s", ctx2.Err())
	}
	select {
	case err = <-done:
		t.Error("GetClosestPeers should not have returned yet", err)
	default:
		err = <-done
		if err != context.DeadlineExceeded {
			t.Errorf("expected the deadline to be exceeded, got %s", err)
		}
	}

	if d.routingTable.Size() == 0 {
		// make sure we didn't just disconnect
		t.Fatal("expected peers in the routing table")
	}
}

func TestGetFailures(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	ctx := context.Background()

	host1 := bhost.New(swarmt.GenSwarm(t, ctx, swarmt.OptDisableReuseport))
	host2 := bhost.New(swarmt.GenSwarm(t, ctx, swarmt.OptDisableReuseport))

	d, err := New(ctx, host1, testPrefix, DisableAutoRefresh(), Mode(ModeServer))
	if err != nil {
		t.Fatal(err)
	}

	// Reply with failures to every message
	for _, proto := range d.serverProtocols {
		host2.SetStreamHandler(proto, func(s network.Stream) {
			time.Sleep(400 * time.Millisecond)
			s.Close()
		})
	}

	host1.Peerstore().AddAddrs(host2.ID(), host2.Addrs(), peerstore.ConnectedAddrTTL)
	_, err = host1.Network().DialPeer(ctx, host2.ID())
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(1 * time.Second)

	// This one should time out
	ctx1, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if _, err := d.GetValue(ctx1, "test"); err != nil {
		if merr, ok := err.(u.MultiErr); ok && len(merr) > 0 {
			err = merr[0]
		}

		if err != context.DeadlineExceeded {
			t.Fatal("Got different error than we expected", err)
		}
	} else {
		t.Fatal("Did not get expected error!")
	}

	t.Log("Timeout test passed.")

	for _, proto := range d.serverProtocols {
		// Reply with failures to every message
		host2.SetStreamHandler(proto, func(s network.Stream) {
			defer s.Close()

			pbr := ggio.NewDelimitedReader(s, network.MessageSizeMax)
			pbw := ggio.NewDelimitedWriter(s)

			pmes := new(pb.Message)
			if err := pbr.ReadMsg(pmes); err != nil {
				// user gave up
				return
			}

			resp := &pb.Message{
				Type: pmes.Type,
			}
			_ = pbw.WriteMsg(resp)
		})
	}

	// This one should fail with NotFound.
	// long context timeout to ensure we dont end too early.
	// the dht should be exhausting its query and returning not found.
	// (was 3 seconds before which should be _plenty_ of time, but maybe
	// travis machines really have a hard time...)
	ctx2, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	_, err = d.GetValue(ctx2, "test")
	if err != nil {
		if merr, ok := err.(u.MultiErr); ok && len(merr) > 0 {
			err = merr[0]
		}
		if err != routing.ErrNotFound {
			t.Fatalf("Expected ErrNotFound, got: %s", err)
		}
	} else {
		t.Fatal("expected error, got none.")
	}

	t.Log("ErrNotFound check passed!")

	// Now we test this DHT's handleGetValue failure
	{
		typ := pb.Message_GET_VALUE
		str := "hello"

		rec := record.MakePutRecord(str, []byte("blah"))
		req := pb.Message{
			Type:   typ,
			Key:    []byte(str),
			Record: rec,
		}

		s, err := host2.NewStream(context.Background(), host1.ID(), d.protocols...)
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()

		pbr := ggio.NewDelimitedReader(s, network.MessageSizeMax)
		pbw := ggio.NewDelimitedWriter(s)

		if err := pbw.WriteMsg(&req); err != nil {
			t.Fatal(err)
		}

		pmes := new(pb.Message)
		if err := pbr.ReadMsg(pmes); err != nil {
			t.Fatal(err)
		}
		if pmes.GetRecord() != nil {
			t.Fatal("shouldnt have value")
		}
		if pmes.GetProviderPeers() != nil {
			t.Fatal("shouldnt have provider peers")
		}
	}

	if d.routingTable.Size() == 0 {
		// make sure we didn't just disconnect
		t.Fatal("expected peers in the routing table")
	}
}

func TestNotFound(t *testing.T) {
	// t.Skip("skipping test to debug another")
	if testing.Short() {
		t.SkipNow()
	}

	ctx := context.Background()
	mn, err := mocknet.FullMeshConnected(ctx, 16)
	if err != nil {
		t.Fatal(err)
	}
	hosts := mn.Hosts()

	os := []Option{testPrefix, DisableAutoRefresh(), Mode(ModeServer)}
	d, err := New(ctx, hosts[0], os...)
	if err != nil {
		t.Fatal(err)
	}

	// Reply with random peers to every message
	for _, host := range hosts {
		host := host // shadow loop var
		for _, proto := range d.serverProtocols {
			host.SetStreamHandler(proto, func(s network.Stream) {
				defer s.Close()

				pbr := ggio.NewDelimitedReader(s, network.MessageSizeMax)
				pbw := ggio.NewDelimitedWriter(s)

				pmes := new(pb.Message)
				if err := pbr.ReadMsg(pmes); err != nil {
					// this isn't an error, it just means the stream has died.
					return
				}

				switch pmes.GetType() {
				case pb.Message_GET_VALUE:
					resp := &pb.Message{Type: pmes.Type}

					ps := []peer.AddrInfo{}
					for i := 0; i < 7; i++ {
						p := hosts[rand.Intn(len(hosts))].ID()
						pi := host.Peerstore().PeerInfo(p)
						ps = append(ps, pi)
					}

					resp.CloserPeers = pb.PeerInfosToPBPeers(d.host.Network(), ps)
					if err := pbw.WriteMsg(resp); err != nil {
						return
					}
				default:
					panic("Shouldnt recieve this.")
				}
			})
		}
		for _, peer := range hosts {
			if host == peer {
				continue
			}
			_ = peer.Peerstore().AddProtocols(host.ID(), protocol.ConvertToStrings(d.serverProtocols)...)
		}
	}

	for _, p := range hosts {
		d.peerFound(ctx, p.ID(), true)
	}

	// long timeout to ensure timing is not at play.
	ctx, cancel := context.WithTimeout(ctx, time.Second*20)
	defer cancel()
	v, err := d.GetValue(ctx, "hello")
	logger.Debugf("get value got %v", v)
	if err != nil {
		if merr, ok := err.(u.MultiErr); ok && len(merr) > 0 {
			err = merr[0]
		}
		switch err {
		case routing.ErrNotFound:
			if d.routingTable.Size() == 0 {
				// make sure we didn't just disconnect
				t.Fatal("expected peers in the routing table")
			}
			//Success!
			return
		case u.ErrTimeout:
			t.Fatal("Should not have gotten timeout!")
		default:
			t.Fatalf("Got unexpected error: %s", err)
		}
	}
	t.Fatal("Expected to recieve an error.")
}

// If less than K nodes are in the entire network, it should fail when we make
// a GET rpc and nobody has the value
func TestLessThanKResponses(t *testing.T) {
	// t.Skip("skipping test to debug another")
	// t.Skip("skipping test because it makes a lot of output")

	ctx := context.Background()
	mn, err := mocknet.FullMeshConnected(ctx, 6)
	if err != nil {
		t.Fatal(err)
	}
	hosts := mn.Hosts()

	os := []Option{testPrefix, DisableAutoRefresh(), Mode(ModeServer)}
	d, err := New(ctx, hosts[0], os...)
	if err != nil {
		t.Fatal(err)
	}

	for i := 1; i < 5; i++ {
		d.peerFound(ctx, hosts[i].ID(), true)
	}

	// Reply with random peers to every message
	for _, host := range hosts {
		host := host // shadow loop var
		for _, proto := range d.serverProtocols {
			host.SetStreamHandler(proto, func(s network.Stream) {
				defer s.Close()

				pbr := ggio.NewDelimitedReader(s, network.MessageSizeMax)
				pbw := ggio.NewDelimitedWriter(s)

				pmes := new(pb.Message)
				if err := pbr.ReadMsg(pmes); err != nil {
					panic(err)
				}

				switch pmes.GetType() {
				case pb.Message_GET_VALUE:
					pi := host.Peerstore().PeerInfo(hosts[1].ID())
					resp := &pb.Message{
						Type:        pmes.Type,
						CloserPeers: pb.PeerInfosToPBPeers(d.host.Network(), []peer.AddrInfo{pi}),
					}

					if err := pbw.WriteMsg(resp); err != nil {
						panic(err)
					}
				default:
					panic("Shouldnt recieve this.")
				}

			})
		}
	}

	ctx, cancel := context.WithTimeout(ctx, time.Second*30)
	defer cancel()
	if _, err := d.GetValue(ctx, "hello"); err != nil {
		switch err {
		case routing.ErrNotFound:
			//Success!
			return
		case u.ErrTimeout:
			t.Fatal("Should not have gotten timeout!")
		default:
			t.Fatalf("Got unexpected error: %s", err)
		}
	}
	t.Fatal("Expected to recieve an error.")
}

// Test multiple queries against a node that closes its stream after every query.
func TestMultipleQueries(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	ctx := context.Background()
	mn, err := mocknet.FullMeshConnected(ctx, 2)
	if err != nil {
		t.Fatal(err)
	}
	hosts := mn.Hosts()
	os := []Option{testPrefix, DisableAutoRefresh(), Mode(ModeServer)}
	d, err := New(ctx, hosts[0], os...)
	if err != nil {
		t.Fatal(err)
	}

	d.peerFound(ctx, hosts[1].ID(), true)

	for _, proto := range d.serverProtocols {
		// It would be nice to be able to just get a value and succeed but then
		// we'd need to deal with selectors and validators...
		hosts[1].SetStreamHandler(proto, func(s network.Stream) {
			defer s.Close()

			pbr := ggio.NewDelimitedReader(s, network.MessageSizeMax)
			pbw := ggio.NewDelimitedWriter(s)

			pmes := new(pb.Message)
			if err := pbr.ReadMsg(pmes); err != nil {
				panic(err)
			}

			switch pmes.GetType() {
			case pb.Message_GET_VALUE:
				pi := hosts[1].Peerstore().PeerInfo(hosts[0].ID())
				resp := &pb.Message{
					Type:        pmes.Type,
					CloserPeers: pb.PeerInfosToPBPeers(d.host.Network(), []peer.AddrInfo{pi}),
				}

				if err := pbw.WriteMsg(resp); err != nil {
					panic(err)
				}
			default:
				panic("Shouldnt recieve this.")
			}
		})
	}

	// long timeout to ensure timing is not at play.
	ctx, cancel := context.WithTimeout(ctx, time.Second*20)
	defer cancel()
	for i := 0; i < 10; i++ {
		if _, err := d.GetValue(ctx, "hello"); err != nil {
			switch err {
			case routing.ErrNotFound:
				//Success!
				continue
			case u.ErrTimeout:
				t.Fatal("Should not have gotten timeout!")
			default:
				t.Fatalf("Got unexpected error: %s", err)
			}
		}
		t.Fatal("Expected to recieve an error.")
	}
}
