package dual

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	u "github.com/ipfs/go-ipfs-util"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	test "github.com/libp2p/go-libp2p-kad-dht/internal/testing"
	peerstore "github.com/libp2p/go-libp2p-peerstore"
	record "github.com/libp2p/go-libp2p-record"
	swarmt "github.com/libp2p/go-libp2p-swarm/testing"
	bhost "github.com/libp2p/go-libp2p/p2p/host/basic"
)

var wancid, lancid cid.Cid

func init() {
	wancid = cid.NewCidV1(cid.DagCBOR, u.Hash([]byte("wan cid -- value")))
	lancid = cid.NewCidV1(cid.DagCBOR, u.Hash([]byte("lan cid -- value")))
}

type blankValidator struct{}

func (blankValidator) Validate(_ string, _ []byte) error        { return nil }
func (blankValidator) Select(_ string, _ [][]byte) (int, error) { return 0, nil }

type customRtHelper struct {
	allow peer.ID
}

func MkFilterForPeer() (func(d *dht.IpfsDHT, conns []network.Conn) bool, *customRtHelper) {
	helper := customRtHelper{}
	f := func(_ *dht.IpfsDHT, conns []network.Conn) bool {
		for _, c := range conns {
			if c.RemotePeer() == helper.allow {
				fmt.Fprintf(os.Stderr, "allowed conn per filter\n")
				return true
			}
		}
		fmt.Fprintf(os.Stderr, "rejected conn per rt filter\n")
		return false
	}
	return f, &helper
}

func setupDHTWithFilters(ctx context.Context, t *testing.T, options ...dht.Option) (*DHT, []*customRtHelper) {
	h := bhost.New(swarmt.GenSwarm(t, ctx, swarmt.OptDisableReuseport))

	wanFilter, wanRef := MkFilterForPeer()
	wanOpts := []dht.Option{
		dht.NamespacedValidator("v", blankValidator{}),
		dht.ProtocolPrefix("/test"),
		dht.DisableAutoRefresh(),
		dht.RoutingTableFilter(wanFilter),
	}
	wan, err := dht.New(ctx, h, wanOpts...)
	if err != nil {
		t.Fatal(err)
	}

	lanFilter, lanRef := MkFilterForPeer()
	lanOpts := []dht.Option{
		dht.NamespacedValidator("v", blankValidator{}),
		dht.ProtocolPrefix("/test"),
		dht.ProtocolExtension(DefaultLanExtension),
		dht.DisableAutoRefresh(),
		dht.RoutingTableFilter(lanFilter),
		dht.Mode(dht.ModeServer),
	}
	lan, err := dht.New(ctx, h, lanOpts...)
	if err != nil {
		t.Fatal(err)
	}

	impl := DHT{wan, lan}
	return &impl, []*customRtHelper{wanRef, lanRef}
}

func setupDHT(ctx context.Context, t *testing.T, options ...dht.Option) *DHT {
	t.Helper()
	baseOpts := []dht.Option{
		dht.NamespacedValidator("v", blankValidator{}),
		dht.ProtocolPrefix("/test"),
		dht.DisableAutoRefresh(),
	}

	d, err := New(
		ctx,
		bhost.New(swarmt.GenSwarm(t, ctx, swarmt.OptDisableReuseport)),
		append(baseOpts, options...)...,
	)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func connect(ctx context.Context, t *testing.T, a, b *dht.IpfsDHT) {
	t.Helper()
	bid := b.PeerID()
	baddr := b.Host().Peerstore().Addrs(bid)
	if len(baddr) == 0 {
		t.Fatal("no addresses for connection.")
	}
	a.Host().Peerstore().AddAddrs(bid, baddr, peerstore.TempAddrTTL)
	if err := a.Host().Connect(ctx, peer.AddrInfo{ID: bid}); err != nil {
		t.Fatal(err)
	}
	wait(ctx, t, a, b)
}

func wait(ctx context.Context, t *testing.T, a, b *dht.IpfsDHT) {
	t.Helper()
	for a.RoutingTable().Find(b.PeerID()) == "" {
		//fmt.Fprintf(os.Stderr, "%v\n", a.RoutingTable().GetPeerInfos())
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(time.Millisecond * 5):
		}
	}
}

func setupTier(ctx context.Context, t *testing.T) (*DHT, *dht.IpfsDHT, *dht.IpfsDHT) {
	t.Helper()
	baseOpts := []dht.Option{
		dht.NamespacedValidator("v", blankValidator{}),
		dht.ProtocolPrefix("/test"),
		dht.DisableAutoRefresh(),
	}

	d, hlprs := setupDHTWithFilters(ctx, t)

	wan, err := dht.New(
		ctx,
		bhost.New(swarmt.GenSwarm(t, ctx, swarmt.OptDisableReuseport)),
		append(baseOpts, dht.Mode(dht.ModeServer))...,
	)
	if err != nil {
		t.Fatal(err)
	}
	hlprs[0].allow = wan.PeerID()
	connect(ctx, t, d.WAN, wan)

	lan, err := dht.New(
		ctx,
		bhost.New(swarmt.GenSwarm(t, ctx, swarmt.OptDisableReuseport)),
		append(baseOpts, dht.Mode(dht.ModeServer), dht.ProtocolExtension("/lan"))...,
	)
	if err != nil {
		t.Fatal(err)
	}
	hlprs[1].allow = lan.PeerID()
	connect(ctx, t, d.LAN, lan)

	return d, wan, lan
}

func TestDualModes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	d := setupDHT(ctx, t)
	defer d.Close()

	if d.WAN.Mode() != dht.ModeAuto {
		t.Fatal("wrong default mode for wan")
	} else if d.LAN.Mode() != dht.ModeServer {
		t.Fatal("wrong default mode for lan")
	}

	d2 := setupDHT(ctx, t, dht.Mode(dht.ModeClient))
	defer d2.Close()
	if d2.WAN.Mode() != dht.ModeClient ||
		d2.LAN.Mode() != dht.ModeClient {
		t.Fatal("wrong client mode operation")
	}
}

func TestFindProviderAsync(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	d, wan, lan := setupTier(ctx, t)
	defer d.Close()
	defer wan.Close()
	defer lan.Close()

	if err := wan.Provide(ctx, wancid, false); err != nil {
		t.Fatal(err)
	}

	if err := lan.Provide(ctx, lancid, true); err != nil {
		t.Fatal(err)
	}

	wpc := d.FindProvidersAsync(ctx, wancid, 1)
	select {
	case p := <-wpc:
		if p.ID != wan.PeerID() {
			t.Fatal("wrong wan provider")
		}
	case <-ctx.Done():
		t.Fatal("find provider timeout.")
	}

	lpc := d.FindProvidersAsync(ctx, lancid, 1)
	select {
	case p := <-lpc:
		if p.ID != lan.PeerID() {
			t.Fatal("wrong lan provider")
		}
	case <-ctx.Done():
		t.Fatal("find provider timeout.")
	}
}

func TestValueGetSet(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	d, wan, lan := setupTier(ctx, t)
	defer d.Close()
	defer wan.Close()
	defer lan.Close()

	err := d.PutValue(ctx, "/v/hello", []byte("valid"))
	if err != nil {
		t.Fatal(err)
	}
	val, err := wan.GetValue(ctx, "/v/hello")
	if err != nil {
		t.Fatal(err)
	}
	if string(val) != "valid" {
		t.Fatal("failed to get expected string.")
	}

	val, err = lan.GetValue(ctx, "/v/hello")
	if err == nil {
		t.Fatal(err)
	}
}

func TestSearchValue(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	d, wan, lan := setupTier(ctx, t)
	defer d.Close()
	defer wan.Close()
	defer lan.Close()

	d.WAN.Validator.(record.NamespacedValidator)["v"] = test.TestValidator{}
	d.LAN.Validator.(record.NamespacedValidator)["v"] = test.TestValidator{}

	err := wan.PutValue(ctx, "/v/hello", []byte("valid"))

	valCh, err := d.SearchValue(ctx, "/v/hello", dht.Quorum(0))
	if err != nil {
		t.Fatal(err)
	}

	select {
	case v := <-valCh:
		if string(v) != "valid" {
			t.Errorf("expected 'valid', got '%s'", string(v))
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}

	err = lan.PutValue(ctx, "/v/hello", []byte("newer"))
	if err != nil {
		t.Error(err)
	}

	select {
	case v, ok := <-valCh:
		if string(v) != "newer" {
			t.Errorf("expected 'newer', got '%s'", string(v))
		}
		if !ok {
			t.Errorf("chan closed early")
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}
