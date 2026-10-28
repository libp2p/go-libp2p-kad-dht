package dht

import (
	"context"
	"errors"
	"fmt"
	"go/version"
	"math/rand/v2"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	detectrace "github.com/ipfs/go-detect-race"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/test"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/require"
)

const (
	testPeerA = "QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN"
	testPeerB = "QmQCU2EcMqAqQPR2i9bChDtGNJchTbq5TbXJJ16u19uLTa"
)

// goldenSnapshot pins the on-disk format: changing it breaks snapshots written
// by released versions.
const goldenSnapshot = `{"version":1,"savedAt":"2026-10-15T12:00:00Z","peers":[{"id":"` + testPeerA +
	`","addrs":["/ip4/1.2.3.4/tcp/4001","/ip6/2001:db8::1/udp/4001/quic-v1"]}]}`

func TestSnapshotFormat(t *testing.T) {
	s := &rtSnapshot{
		savedAt: time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC),
		peers: []peer.AddrInfo{{
			ID: peer.ID(mustDecodePeer(t, testPeerA)),
			Addrs: []ma.Multiaddr{
				ma.StringCast("/ip4/1.2.3.4/tcp/4001"),
				ma.StringCast("/ip6/2001:db8::1/udp/4001/quic-v1"),
			},
		}},
	}

	b, err := encodeSnapshot(s)
	require.NoError(t, err)
	require.Equal(t, goldenSnapshot, string(b))

	decoded, err := decodeSnapshot([]byte(goldenSnapshot))
	require.NoError(t, err)
	require.True(t, s.savedAt.Equal(decoded.savedAt))
	require.Equal(t, formatPeers(s.peers), formatPeers(decoded.peers))
}

func TestSnapshotKey(t *testing.T) {
	require.Equal(t, "/kad-dht/routing-table/ipfs/kad/1.0.0", snapshotKey("/ipfs/kad/1.0.0").String())
	require.Equal(t, "/kad-dht/routing-table/ipfs/lan/kad/1.0.0", snapshotKey("/ipfs/lan/kad/1.0.0").String())
}

func TestDecodeSnapshot(t *testing.T) {
	tests := []struct {
		name      string
		data      string
		wantErr   bool
		wantPeers []string
	}{
		{
			name:    "unknown version",
			data:    `{"version":2,"savedAt":"2026-10-15T12:00:00Z","peers":[]}`,
			wantErr: true,
		},
		{
			name:    "corrupt",
			data:    `{"version":1,"peers":[`,
			wantErr: true,
		},
		{
			name: "bad entries are skipped one by one",
			data: `{"version":1,"savedAt":"2026-10-15T12:00:00Z","peers":[
				{"id":"not-a-peer-id","addrs":["/ip4/1.2.3.4/tcp/4001"]},
				{"id":"` + testPeerA + `","addrs":["/unknown-protocol/1","/ip4/1.2.3.4/tcp/4001"]},
				{"id":"` + testPeerB + `","addrs":["/unknown-protocol/1"]}]}`,
			wantPeers: []string{testPeerA + " /ip4/1.2.3.4/tcp/4001"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := decodeSnapshot([]byte(tt.data))
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.wantPeers, formatPeers(s.peers))
		})
	}
}

func TestSavableAddrs(t *testing.T) {
	direct := "/ip4/1.2.3.4/tcp/4001"
	relayed := "/ip4/5.6.7.8/tcp/4001/p2p/" + testPeerB + "/p2p-circuit"
	certhash := "/ip4/1.2.3.4/udp/4001/quic-v1/webtransport/certhash/uEiDDq4_xNyDorZBH3TlGazyJdOWSwvo4PUo5YHFMrvDE8g"

	undialable := "/ip4/1.2.3.4/udp/4001/quic-v1"
	canDial := func(a ma.Multiaddr) bool { return a.String() != undialable }

	got := savableAddrs([]ma.Multiaddr{
		ma.StringCast(relayed), ma.StringCast(direct), ma.StringCast(certhash), ma.StringCast(undialable), ma.StringCast(direct),
	}, canDial)
	require.Equal(t, []string{direct}, addrStrings(got))

	many := make([]ma.Multiaddr, maxSavedAddrsPerPeer+5)
	for i := range many {
		many[i] = ma.StringCast(fmt.Sprintf("/ip4/1.2.3.4/tcp/%d", 4000+i))
	}
	require.Equal(t, addrStrings(many[:maxSavedAddrsPerPeer]), addrStrings(savableAddrs(many, canDial)), "the first addresses are kept")

	skipped := slices.Concat(slices.Repeat([]ma.Multiaddr{ma.StringCast(undialable)}, 5), many)
	require.Equal(t, addrStrings(many[:maxSavedAddrsPerPeer]), addrStrings(savableAddrs(skipped, canDial)), "addresses the host can't dial don't count")
}

func TestSnapshotPeersAddrs(t *testing.T) {
	ctx := t.Context()
	d := setupDHT(ctx, t, false)
	dialed := setupDHT(ctx, t, false)
	dialing := setupDHT(ctx, t, false)
	overWebTransport := setupDHT(ctx, t, false)
	connect(t, ctx, d, dialed)
	// Over QUIC, an inbound connection comes from the port the peer listens
	// on. Over TCP, it comes from an ephemeral port.
	require.NoError(t, dialing.host.Connect(ctx, peer.AddrInfo{ID: d.self, Addrs: addrsWith(d.host.Addrs(), ma.P_TCP)}))
	wait(t, ctx, d, dialing)
	// A WebTransport connection's address has no certhash, so it can't be
	// dialed again.
	require.NoError(t, d.host.Connect(ctx, peer.AddrInfo{ID: overWebTransport.self, Addrs: addrsWith(overWebTransport.host.Addrs(), ma.P_WEBTRANSPORT)}))
	wait(t, ctx, d, overWebTransport)

	for i := range 100 {
		d.peerstore.AddAddr(dialed.self, ma.StringCast(fmt.Sprintf("/ip4/1.2.3.4/tcp/%d", 5000+i)), time.Hour)
	}
	// Few enough addresses that all of them would be saved if the host's
	// dialability weren't checked.
	linkLocal := ma.StringCast("/ip6/fe80::1/tcp/4001")
	d.peerstore.AddAddr(overWebTransport.self, linkLocal, time.Hour)

	saved := make(map[peer.ID][]ma.Multiaddr)
	for _, ai := range d.snapshotPeers() {
		saved[ai.ID] = ai.Addrs
	}
	conn := d.host.Network().ConnsToPeer(dialed.self)[0].RemoteMultiaddr()
	require.Truef(t, saved[dialed.self][0].Equal(conn), "the address of a connection comes first, got %v", saved[dialed.self])
	require.Lenf(t, saved[dialed.self], maxSavedAddrsPerPeer, "addresses saved for %s", dialed.self)

	inbound := d.host.Network().ConnsToPeer(dialing.self)[0].RemoteMultiaddr()
	require.Falsef(t, slices.ContainsFunc(saved[dialing.self], inbound.Equal), "saved the source address of an inbound connection: %v", saved[dialing.self])

	webTransport := d.host.Network().ConnsToPeer(overWebTransport.self)[0].RemoteMultiaddr()
	require.Falsef(t, slices.ContainsFunc(saved[overWebTransport.self], webTransport.Equal), "saved a dialed address the peer doesn't advertise: %v", saved[overWebTransport.self])
	require.Falsef(t, slices.ContainsFunc(saved[overWebTransport.self], linkLocal.Equal), "saved an address the host can't dial: %v", saved[overWebTransport.self])
	require.NotEmptyf(t, saved[overWebTransport.self], "saved no address for %s", overWebTransport.self)
}

func addrsWith(addrs []ma.Multiaddr, code int) []ma.Multiaddr {
	return ma.FilterAddrs(addrs, func(a ma.Multiaddr) bool {
		_, err := a.ValueForProtocol(code)
		return err == nil
	})
}

func TestWorthSaving(t *testing.T) {
	tests := []struct {
		peers, previous int
		want            bool
	}{
		{peers: minRTRefreshThreshold - 1, previous: 0, want: false},
		{peers: minRTRefreshThreshold, previous: 0, want: true},
		{peers: minRTRefreshThreshold, previous: 2 * minRTRefreshThreshold, want: true},
		{peers: 124, previous: 250, want: false},
		{peers: 125, previous: 250, want: true},
		{peers: 300, previous: 250, want: true},
	}
	for _, tt := range tests {
		require.Equalf(t, tt.want, worthSaving(tt.peers, tt.previous), "%d peers, previous snapshot %d", tt.peers, tt.previous)
	}
}

func TestSaveBaseline(t *testing.T) {
	peers := make([]peer.AddrInfo, 250)
	require.Zero(t, saveBaseline(nil))
	require.Equal(t, 250, saveBaseline(&rtSnapshot{peers: peers, savedAt: time.Now()}))
	require.Equal(t, 250, saveBaseline(&rtSnapshot{peers: peers, savedAt: time.Now().Add(-maxBaselineAge + time.Hour)}))
	require.Zerof(t, saveBaseline(&rtSnapshot{peers: peers, savedAt: time.Now().Add(-maxBaselineAge - time.Hour)}),
		"an old snapshot no longer holds back smaller ones")
	require.Zerof(t, saveBaseline(&rtSnapshot{peers: peers, savedAt: time.Now().Add(time.Hour)}),
		"a snapshot saved while the clock was ahead doesn't hold back smaller ones")
}

func TestLoadRoutingTable(t *testing.T) {
	saved := test.RandPeerIDFatal(t)
	key := snapshotKey("/test/kad/1.0.0")
	addr := ma.StringCast("/ip4/1.2.3.4/tcp/4001")

	tests := []struct {
		name      string
		savedAt   time.Time
		data      []byte // overrides the encoded snapshot when set
		wantPeers []peer.ID
	}{
		{name: "fresh snapshot", savedAt: time.Now(), wantPeers: []peer.ID{saved}},
		{name: "year-old snapshot", savedAt: time.Now().AddDate(-1, 0, 0), wantPeers: []peer.ID{saved}},
		{name: "corrupt snapshot", data: []byte("{")},
		{name: "no snapshot"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dstore := dssync.MutexWrap(ds.NewMapDatastore())
			data := tt.data
			if data == nil && !tt.savedAt.IsZero() {
				var err error
				data, err = encodeSnapshot(&rtSnapshot{savedAt: tt.savedAt, peers: []peer.AddrInfo{
					{ID: saved, Addrs: []ma.Multiaddr{addr}},
				}})
				require.NoError(t, err)
			}
			if data != nil {
				require.NoError(t, dstore.Put(t.Context(), key, data))
			}

			d := &IpfsDHT{datastore: dstore, persistence: rtPersistence{key: key}}
			d.loadRoutingTable(t.Context())

			s := d.persistence.snapshot.Load()
			if tt.wantPeers == nil {
				require.Nil(t, s)
				return
			}
			require.NotNil(t, s)
			var got []peer.ID
			for _, ai := range s.peers {
				got = append(got, ai.ID)
			}
			require.Equal(t, tt.wantPeers, got)
		})
	}
}

func TestRoutingTablePersistenceOptions(t *testing.T) {
	tests := []struct {
		name string
		opts []Option
		want time.Duration
	}{
		{name: "default", want: time.Hour},
		{name: "custom interval", opts: []Option{RoutingTablePersistence(time.Minute)}, want: time.Minute},
		{name: "disabled", opts: []Option{RoutingTablePersistence(0)}, want: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := setupDHT(t.Context(), t, false, tt.opts...)
			require.Equal(t, tt.want, d.persistence.interval)
		})
	}

	_, err := New(nil, RoutingTablePersistence(-time.Second))
	require.Error(t, err)
	_, err = New(nil, BootstrapOrder(Concurrent+1))
	require.Error(t, err)
}

func TestRoutingTableSurvivesRestart(t *testing.T) {
	bubble(t, func(t *testing.T) {
		dhts := newMockDHTs(t)
		dstore := dssync.MutexWrap(ds.NewMapDatastore())
		// Nobody connects to the bootstrap peer, so only the saved peers can
		// refill the routing table.
		boot := dhts.add()
		bootstrap := BootstrapPeers(peer.AddrInfo{ID: boot.self, Addrs: boot.host.Addrs()})

		first, others := dhts.addConnected(12, Datastore(dstore))
		require.NoError(t, first.Close())
		require.Len(t, savedSnapshot(t, dstore, first.persistence.key).peers, len(others))

		second := dhts.add(Datastore(dstore), bootstrap, BootstrapOrder(PersistedFirst))
		// Past the fallback deadline and the bootstrap dials it would start.
		time.Sleep(2 * seedFallbackDelay)
		synctest.Wait()
		require.Equal(t, len(others), second.routingTable.Size())
		require.NotEqualf(t, network.Connected, second.host.Network().Connectedness(boot.self),
			"the bootstrap peer was dialed although %d saved peers answered", len(others))
	})
}

func TestRoutingTableSurvivesRestartWithoutBootstrapPeers(t *testing.T) {
	bubble(t, func(t *testing.T) {
		dhts := newMockDHTs(t)
		dstore := dssync.MutexWrap(ds.NewMapDatastore())
		first, others := dhts.addConnected(12, Datastore(dstore))
		require.NoError(t, first.Close())

		second := dhts.add(Datastore(dstore))
		synctest.Wait()
		require.Equal(t, len(others), second.routingTable.Size())
	})
}

func TestCloseSavesOnce(t *testing.T) {
	bubble(t, func(t *testing.T) {
		dhts := newMockDHTs(t)
		dstore := dssync.MutexWrap(ds.NewMapDatastore())
		d, _ := dhts.addConnected(12, Datastore(dstore))
		require.NoError(t, d.Close())
		require.NoError(t, dstore.Delete(t.Context(), d.persistence.key))

		// By a second Close, the caller may have closed the datastore.
		require.NoError(t, d.Close())
		_, err := dstore.Get(t.Context(), d.persistence.key)
		require.ErrorIs(t, err, ds.ErrNotFound, "a second Close saved again")
	})
}

func TestRoutingTableSavedPeriodically(t *testing.T) {
	tests := []struct {
		name      string
		opts      []Option
		firstSave time.Duration
	}{
		{name: "first save after the interval", opts: []Option{RoutingTablePersistence(time.Minute)}, firstSave: time.Minute},
		{
			name:      "first save after one refresh period",
			opts:      []Option{RoutingTablePersistence(time.Minute), RoutingTableRefreshPeriod(30 * time.Second)},
			firstSave: 30 * time.Second,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bubble(t, func(t *testing.T) {
				dhts := newMockDHTs(t)
				dstore := dssync.MutexWrap(ds.NewMapDatastore())
				d, others := dhts.addConnected(12, append(tt.opts, Datastore(dstore))...)
				savedPeers := func() int {
					if ok, _ := dstore.Has(t.Context(), d.persistence.key); !ok {
						return 0
					}
					return len(savedSnapshot(t, dstore, d.persistence.key).peers)
				}

				time.Sleep(tt.firstSave - time.Second)
				synctest.Wait()
				require.Zero(t, savedPeers(), "saved too early")

				time.Sleep(time.Second)
				synctest.Wait()
				require.Equal(t, len(others), savedPeers())

				for range 3 {
					dhts.connect(d, dhts.add())
				}
				time.Sleep(time.Minute)
				synctest.Wait()
				require.Equal(t, len(others)+3, savedPeers(), "no save after the first")
			})
		})
	}
}

func TestHeldRefreshRunsWhenHoldEnds(t *testing.T) {
	bubble(t, func(t *testing.T) {
		dhts := newMockDHTs(t)
		// d knows only hub, so only a refresh finds hub's peers.
		hub, _ := dhts.addConnected(12)
		d := dhts.add()
		d.persistence.refreshHeld.Store(true)
		dhts.connect(d, hub)

		d.requestRefresh()
		synctest.Wait()
		require.Equal(t, 1, d.routingTable.Size(), "refreshed during the hold")

		d.releaseRefreshHold()
		synctest.Wait()
		require.Greater(t, d.routingTable.Size(), 1, "the held refresh didn't run")
	})
}

// bubble runs f in a synctest bubble. Before Go 1.27, the race detector can
// crash when timers in a bubble fire at the same instant, as the restore's dial
// timeouts do (https://github.com/golang/go/issues/76691), so race runs on
// older Go are skipped.
//
// TODO: replace bubble with synctest.Test once go.mod requires Go 1.27.
func bubble(t *testing.T, f func(t *testing.T)) {
	t.Helper()
	if detectrace.WithRace() && version.Compare(runtime.Version(), "go1.27") < 0 {
		t.Skip("synctest crashes under the race detector before Go 1.27: https://github.com/golang/go/issues/76691")
	}
	synctest.Test(t, f)
}

func savedSnapshot(t *testing.T, dstore ds.Datastore, key ds.Key) *rtSnapshot {
	t.Helper()
	b, err := dstore.Get(t.Context(), key)
	require.NoError(t, err)
	s, err := decodeSnapshot(b)
	require.NoError(t, err)
	return s
}

// mockDHTs builds DHTs over an in-memory network. Unlike real sockets, it
// lets tests run in a synctest bubble.
type mockDHTs struct {
	t  *testing.T
	mn mocknet.Mocknet
}

func newMockDHTs(t *testing.T) *mockDHTs {
	mn := mocknet.New()
	t.Cleanup(func() { mn.Close() })
	return &mockDHTs{t: t, mn: mn}
}

func (m *mockDHTs) add(opts ...Option) *IpfsDHT {
	m.t.Helper()
	h, err := m.mn.GenPeer()
	require.NoError(m.t, err)
	for _, p := range m.mn.Peers() {
		if p != h.ID() {
			_, err := m.mn.LinkPeers(h.ID(), p)
			require.NoError(m.t, err)
		}
	}
	d, err := New(h, append([]Option{testPrefix, DisableAutoRefresh(), Mode(ModeServer)}, opts...)...)
	require.NoError(m.t, err)
	m.t.Cleanup(func() { d.Close() })
	return d
}

// addConnected adds a DHT with opts, connected to n new DHTs with default
// options, and checks they all entered its routing table.
func (m *mockDHTs) addConnected(n int, opts ...Option) (*IpfsDHT, []*IpfsDHT) {
	m.t.Helper()
	d := m.add(opts...)
	others := make([]*IpfsDHT, n)
	for i := range others {
		others[i] = m.add()
		m.connect(d, others[i])
	}
	require.Equal(m.t, n, d.routingTable.Size())
	return d, others
}

// connect connects a to b and waits for the bubble to settle.
func (m *mockDHTs) connect(a, b *IpfsDHT) {
	m.t.Helper()
	_, err := m.mn.ConnectPeers(a.self, b.self)
	require.NoError(m.t, err)
	synctest.Wait()
}

func mustDecodePeer(t *testing.T, s string) peer.ID {
	id, err := peer.Decode(s)
	require.NoError(t, err)
	return id
}

// formatPeers renders peers as "id addr addr…" for comparison.
func formatPeers(peers []peer.AddrInfo) []string {
	var out []string
	for _, ai := range peers {
		out = append(out, strings.Join(append([]string{ai.ID.String()}, addrStrings(ai.Addrs)...), " "))
	}
	return out
}

func addrStrings(addrs []ma.Multiaddr) []string {
	out := make([]string, len(addrs))
	for i, a := range addrs {
		out[i] = a.String()
	}
	return out
}

// fakeSeedNet simulates dialing seed peers. Each peer answers after its
// latency; a successful dial adds it to the routing table.
type fakeSeedNet struct {
	mu          sync.Mutex
	peers       map[peer.ID]fakeSeedPeer
	dialedAt    map[peer.ID]time.Time
	connected   int
	inFlight    int
	maxInFlight int
}

type fakeSeedPeer struct {
	latency time.Duration
	fails   bool
}

var (
	livePeer    = fakeSeedPeer{latency: 100 * time.Millisecond}
	refusing    = fakeSeedPeer{fails: true}
	hangingPeer = fakeSeedPeer{latency: time.Hour}
)

func newFakeSeedNet() *fakeSeedNet {
	return &fakeSeedNet{peers: map[peer.ID]fakeSeedPeer{}, dialedAt: map[peer.ID]time.Time{}}
}

// add registers count peers named prefix0, prefix1… that all behave like p.
func (n *fakeSeedNet) add(prefix string, count int, p fakeSeedPeer) []peer.AddrInfo {
	ais := make([]peer.AddrInfo, count)
	for i := range count {
		id := peer.ID(fmt.Sprintf("%s%d", prefix, i))
		n.peers[id] = p
		ais[i] = peer.AddrInfo{ID: id}
	}
	return ais
}

func (n *fakeSeedNet) dial(ctx context.Context, ai peer.AddrInfo) error {
	n.mu.Lock()
	p := n.peers[ai.ID]
	n.dialedAt[ai.ID] = time.Now()
	n.inFlight++
	n.maxInFlight = max(n.maxInFlight, n.inFlight)
	n.mu.Unlock()
	defer func() {
		n.mu.Lock()
		n.inFlight--
		n.mu.Unlock()
	}()

	select {
	case <-time.After(p.latency):
	case <-ctx.Done():
		return ctx.Err()
	}
	if p.fails {
		return errors.New("connection refused")
	}
	n.mu.Lock()
	n.connected++
	n.mu.Unlock()
	return nil
}

func (n *fakeSeedNet) rtSize() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.connected
}

func (n *fakeSeedNet) dialedCount(ais []peer.AddrInfo) int {
	n.mu.Lock()
	defer n.mu.Unlock()
	count := 0
	for _, ai := range ais {
		if _, ok := n.dialedAt[ai.ID]; ok {
			count++
		}
	}
	return count
}

// firstDial returns when the first of ais was dialed, or the zero time.
func (n *fakeSeedNet) firstDial(ais []peer.AddrInfo) time.Time {
	n.mu.Lock()
	defer n.mu.Unlock()
	var first time.Time
	for _, ai := range ais {
		if at, ok := n.dialedAt[ai.ID]; ok && (first.IsZero() || at.Before(first)) {
			first = at
		}
	}
	return first
}

// refreshHold records how seeding holds and releases refreshes.
type refreshHold struct {
	mu         sync.Mutex
	held       bool
	holds      int
	releasedAt time.Time
}

func (h *refreshHold) hold() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.held = true
	h.holds++
}

func (h *refreshHold) release() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.held {
		h.held = false
		h.releasedAt = time.Now()
	}
}

const testRestoreThreshold = 10 // alpha on the Amino DHT

// maxDialsPerSlot bounds the dials in flight to
// seedDialConcurrency*maxDialsPerSlot when saved peers are slow to answer.
const maxDialsPerSlot = int((seedDialTimeout + seedDialSoftTimeout - 1) / seedDialSoftTimeout)

func newTestSeeding(n *fakeSeedNet, h *refreshHold) *seeding {
	return &seeding{
		dial:      n.dial,
		threshold: testRestoreThreshold,
		hold:      h.hold,
		release:   h.release,
	}
}

func TestSeedingOrder(t *testing.T) {
	tests := []struct {
		name          string
		order         BootstrapOrderOpt
		saved         fakeSeedPeer
		savedCount    int
		bootstrap     fakeSeedPeer
		bootstrappers int
		wantBootstrap int // bootstrap peers dialed
		wantSaved     int // saved peers dialed
		wantHolds     int
	}{
		{
			name:  "persisted first skips bootstrappers when saved peers refill the table",
			order: PersistedFirst, saved: livePeer, savedCount: 40, bootstrap: livePeer, bootstrappers: 2,
			wantBootstrap: 0, wantSaved: 40, wantHolds: 1,
		},
		{
			name:  "persisted first skips bootstrappers once alpha saved peers answer",
			order: PersistedFirst, saved: livePeer, savedCount: testRestoreThreshold, bootstrap: livePeer, bootstrappers: 2,
			wantBootstrap: 0, wantSaved: testRestoreThreshold, wantHolds: 1,
		},
		{
			name:  "persisted first falls back when fewer than alpha saved peers answer",
			order: PersistedFirst, saved: livePeer, savedCount: testRestoreThreshold - 1, bootstrap: livePeer, bootstrappers: 2,
			wantBootstrap: 2, wantSaved: testRestoreThreshold - 1, wantHolds: 1,
		},
		{
			name:  "persisted first falls back when saved peers are gone",
			order: PersistedFirst, saved: refusing, savedCount: 10, bootstrap: livePeer, bootstrappers: 2,
			wantBootstrap: 2, wantSaved: 10, wantHolds: 1,
		},
		{
			name:  "persisted first without saved peers dials bootstrappers",
			order: PersistedFirst, bootstrap: livePeer, bootstrappers: 2,
			wantBootstrap: 2,
		},
		{
			name:  "bootstrap first stops at two bootstrappers",
			order: BootstrapFirst, saved: livePeer, savedCount: 40, bootstrap: livePeer, bootstrappers: 3,
			wantBootstrap: 2,
		},
		{
			name:  "bootstrap first accepts a single configured bootstrapper",
			order: BootstrapFirst, saved: livePeer, savedCount: 40, bootstrap: livePeer, bootstrappers: 1,
			wantBootstrap: 1,
		},
		{
			name:  "bootstrap first falls back to saved peers",
			order: BootstrapFirst, saved: livePeer, savedCount: 40, bootstrap: refusing, bootstrappers: 2,
			wantBootstrap: 2, wantSaved: 40, wantHolds: 1,
		},
		{
			name:  "concurrent dials both",
			order: Concurrent, saved: livePeer, savedCount: 40, bootstrap: livePeer, bootstrappers: 2,
			wantBootstrap: 2, wantSaved: 40, wantHolds: 1,
		},
		{
			name:  "persisted first without bootstrap peers dials saved peers",
			order: PersistedFirst, saved: livePeer, savedCount: 40,
			wantSaved: 40, wantHolds: 1,
		},
		{
			name:  "bootstrap first without bootstrap peers dials saved peers",
			order: BootstrapFirst, saved: livePeer, savedCount: 40,
			wantSaved: 40, wantHolds: 1,
		},
		{
			name:  "concurrent without bootstrap peers dials saved peers",
			order: Concurrent, saved: livePeer, savedCount: 40,
			wantSaved: 40, wantHolds: 1,
		},
		{
			name:  "nothing saved and no bootstrap peers dials nothing",
			order: BootstrapFirst,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bubble(t, func(t *testing.T) {
				n := newFakeSeedNet()
				saved := n.add("saved", tt.savedCount, tt.saved)
				bootstrap := n.add("bootstrap", tt.bootstrappers, tt.bootstrap)
				h := &refreshHold{}

				newTestSeeding(n, h).run(t.Context(), tt.order, saved, bootstrap)

				require.Equalf(t, tt.wantBootstrap, n.dialedCount(bootstrap), "bootstrap peers dialed")
				require.Equalf(t, tt.wantSaved, n.dialedCount(saved), "saved peers dialed")
				require.Equalf(t, tt.wantHolds, h.holds, "refresh holds")
				require.Falsef(t, h.held, "refreshes still held after seeding")
			})
		})
	}
}

func TestRestoreThreshold(t *testing.T) {
	for _, tt := range []struct {
		alpha, want int
	}{
		{alpha: 10, want: 10},
		{alpha: 2, want: 2},
		{alpha: 1, want: maxNBoostrappers},
		{alpha: 0, want: maxNBoostrappers},
		{alpha: -1, want: maxNBoostrappers},
	} {
		d := &IpfsDHT{alpha: tt.alpha}
		require.Equalf(t, tt.want, d.restoreThreshold(), "alpha %d", tt.alpha)
	}
}

func TestReleaseRefreshHoldAtThreshold(t *testing.T) {
	d := setupDHT(t.Context(), t, false)
	require.Equal(t, d.alpha, d.restoreThreshold(), "a restore needs as many peers as a lookup queries at once")
	d.persistence.refreshHeld.Store(true)

	d.requestRefresh()
	require.True(t, d.persistence.refreshDeferred.Load(), "a refresh requested during the hold waits for it")

	for d.routingTable.Size() < d.restoreThreshold()-1 {
		_, _ = d.routingTable.TryAddPeer(test.RandPeerIDFatal(t), true, false)
	}
	d.releaseRefreshHoldAtThreshold()
	require.True(t, d.persistence.refreshHeld.Load(), "released below the threshold")

	for d.routingTable.Size() < d.restoreThreshold() {
		_, _ = d.routingTable.TryAddPeer(test.RandPeerIDFatal(t), true, false)
	}
	d.releaseRefreshHoldAtThreshold()
	require.False(t, d.persistence.refreshHeld.Load())
	require.False(t, d.persistence.refreshDeferred.Load())
}

func TestSeedingFallsBackAtDeadline(t *testing.T) {
	bubble(t, func(t *testing.T) {
		n := newFakeSeedNet()
		// One live peer in thirty: enough to rule out the early abort, too
		// few to reach the threshold before the deadline, which comes long
		// before the sweep ends.
		var saved []peer.AddrInfo
		for i := range 10 {
			saved = append(saved, n.add(fmt.Sprintf("live%d-", i), 1, livePeer)...)
			saved = append(saved, n.add(fmt.Sprintf("hanging%d-", i), 29, hangingPeer)...)
		}
		bootstrap := n.add("bootstrap", 2, livePeer)
		h := &refreshHold{}
		start := time.Now()

		newTestSeeding(n, h).run(t.Context(), PersistedFirst, saved, bootstrap)

		require.Equal(t, seedFallbackDelay, n.firstDial(bootstrap).Sub(start))
		require.Equalf(t, seedFallbackDelay+2*livePeer.latency, h.releasedAt.Sub(start), "refreshes wait for the bootstrap peers")
		require.Equal(t, len(saved), n.dialedCount(saved), "the sweep goes on after the fallback")
		require.LessOrEqualf(t, n.maxInFlight, seedDialConcurrency*maxDialsPerSlot+1, "saved peers, and one bootstrap peer at a time")
	})
}

func TestSeedingSoftTimeoutFreesSlots(t *testing.T) {
	bubble(t, func(t *testing.T) {
		n := newFakeSeedNet()
		// Every peer answers between the soft and the hard timeout.
		latency := seedDialSoftTimeout * 3 / 2
		require.Less(t, latency, seedDialTimeout)
		saved := n.add("saved", 40, fakeSeedPeer{latency: latency})
		bootstrap := n.add("bootstrap", 2, livePeer)
		h := &refreshHold{}
		start := time.Now()

		newTestSeeding(n, h).run(t.Context(), PersistedFirst, saved, bootstrap)

		require.Equal(t, 40, n.rtSize(), "late answers count")
		require.Zero(t, n.dialedCount(bootstrap))
		// Three waves, each starting at the soft timeout of the previous one;
		// only two overlap.
		require.Equal(t, 2*seedDialSoftTimeout+latency, h.releasedAt.Sub(start))
		require.Equal(t, 2*seedDialConcurrency, n.maxInFlight)
	})
}

func TestSeedingAbortsWhenNothingAnswers(t *testing.T) {
	bubble(t, func(t *testing.T) {
		n := newFakeSeedNet()
		saved := n.add("saved", 200, hangingPeer)
		bootstrap := n.add("bootstrap", 2, livePeer)

		newTestSeeding(n, &refreshHold{}).run(t.Context(), PersistedFirst, saved, bootstrap)

		// The first two waves time out at seedDialTimeout and one soft timeout
		// later, by when two more have started.
		dialed := n.dialedCount(saved)
		require.GreaterOrEqual(t, dialed, 2*seedDialConcurrency)
		require.LessOrEqualf(t, dialed, 4*seedDialConcurrency, "gave up after %d failures", 2*seedDialConcurrency)
		require.Equal(t, 2, n.dialedCount(bootstrap))
	})
}

func TestSeedingGivesUpAfterDialTimeout(t *testing.T) {
	bubble(t, func(t *testing.T) {
		n := newFakeSeedNet()
		// The refusals alone exceed 2*seedDialConcurrency failures at once,
		// but the sweep only gives up when the first hanging dials time out.
		saved := n.add("refusing", 4*seedDialConcurrency, refusing)
		saved = append(saved, n.add("hanging", 200, hangingPeer)...)
		bootstrap := n.add("bootstrap", 2, livePeer)
		start := time.Now()

		newTestSeeding(n, &refreshHold{}).run(t.Context(), PersistedFirst, saved, bootstrap)

		require.Equal(t, seedDialTimeout, n.firstDial(bootstrap).Sub(start))
		require.Less(t, n.dialedCount(saved), len(saved))
	})
}

func TestSeedingSurvivesQuickRefusals(t *testing.T) {
	bubble(t, func(t *testing.T) {
		n := newFakeSeedNet()
		// The live peers take a second to answer, while far more than
		// 2*seedDialConcurrency dead ones refuse at once.
		saved := n.add("live", 3, fakeSeedPeer{latency: time.Second})
		saved = append(saved, n.add("refusing", 4*seedDialConcurrency, refusing)...)
		bootstrap := n.add("bootstrap", 2, refusing)

		newTestSeeding(n, &refreshHold{}).run(t.Context(), PersistedFirst, saved, bootstrap)

		require.Equal(t, 3, n.rtSize(), "quick refusals cut the live peers off")
	})
}

func TestSeedingStopsOnCancel(t *testing.T) {
	bubble(t, func(t *testing.T) {
		n := newFakeSeedNet()
		saved := n.add("saved", 100, hangingPeer)
		bootstrap := n.add("bootstrap", 1, refusing)
		s := newTestSeeding(n, &refreshHold{})
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan struct{})
		go func() {
			defer close(done)
			s.run(ctx, PersistedFirst, saved, bootstrap)
		}()

		synctest.Wait()
		cancel()
		<-done
		require.Equal(t, seedDialConcurrency, n.dialedCount(saved), "no dial after cancellation")
		require.Zero(t, n.dialedCount(bootstrap), "no fallback after cancellation")
	})
}

func TestSeedingConcurrentStartsBothAtOnce(t *testing.T) {
	bubble(t, func(t *testing.T) {
		n := newFakeSeedNet()
		saved := n.add("saved", 40, livePeer)
		bootstrap := n.add("bootstrap", 2, livePeer)
		start := time.Now()

		newTestSeeding(n, &refreshHold{}).run(t.Context(), Concurrent, saved, bootstrap)

		require.Equal(t, start, n.firstDial(saved))
		require.Equal(t, start, n.firstDial(bootstrap))
	})
}

func TestSavedSeeds(t *testing.T) {
	a, b, c := peer.AddrInfo{ID: "a"}, peer.AddrInfo{ID: "b"}, peer.AddrInfo{ID: "c"}
	d := &IpfsDHT{shuffle: rand.Shuffle}
	require.Empty(t, d.savedSeeds(nil), "no snapshot")

	snapshot := &rtSnapshot{peers: []peer.AddrInfo{a, b, c}}
	d.persistence.snapshot.Store(snapshot)

	seeds := d.savedSeeds([]peer.AddrInfo{b})
	ids := make([]peer.ID, len(seeds))
	for i, s := range seeds {
		ids[i] = s.ID
	}
	slices.Sort(ids)
	require.Equal(t, []peer.ID{"a", "c"}, ids, "bootstrap peers are left out")

	seeds[0] = c
	require.Equal(t, []peer.AddrInfo{a, b, c}, snapshot.peers, "the snapshot is not modified")
}
