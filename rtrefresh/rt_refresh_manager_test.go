package rtrefresh

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p-core/test"

	kb "github.com/libp2p/go-libp2p-kbucket"
	pstore "github.com/libp2p/go-libp2p-peerstore"

	"github.com/stretchr/testify/require"
)

func TestSkipRefreshOnGapCpls(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	local := test.RandPeerIDFatal(t)

	// adds a peer for a cpl
	qFuncWithIgnore := func(rt *kb.RoutingTable, ignoreCpl uint) func(c context.Context, key string) error {
		return func(c context.Context, key string) error {
			if key == string(local) {
				return nil
			}

			u, err := strconv.ParseInt(key, 10, 64)
			require.NoError(t, err)

			if uint(u) == ignoreCpl {
				return nil
			}

			p, err := rt.GenRandPeerID(uint(u))
			require.NoError(t, err)
			b, err := rt.TryAddPeer(p, true)
			require.True(t, b)
			require.NoError(t, err)
			return nil
		}
	}

	kfnc := func(cpl uint) (string, error) {
		return strconv.FormatInt(int64(cpl), 10), nil
	}

	// when 2*gapcpl < maxCpl
	// gap is 2 and max is 10
	rt, err := kb.NewRoutingTable(2, kb.ConvertPeerID(local), time.Hour, pstore.NewMetrics(), 100*time.Hour)
	require.NoError(t, err)
	r := &RtRefreshManager{ctx: ctx, rt: rt, refreshKeyGenFnc: kfnc, dhtPeerId: local}
	icpl := uint(2)
	p, err := rt.GenRandPeerID(10)
	require.NoError(t, err)
	b, _ := rt.TryAddPeer(p, true)
	require.True(t, b)
	r.refreshQueryFnc = qFuncWithIgnore(rt, icpl)
	require.NoError(t, r.doRefresh(true))

	for i := uint(0); i < 2*icpl+1; i++ {
		if i == icpl {
			require.Equal(t, 0, rt.NPeersForCpl(i))
			continue
		}
		require.Equal(t, 1, rt.NPeersForCpl(uint(i)))
	}
	for i := 2*icpl + 1; i < 10; i++ {
		require.Equal(t, 0, rt.NPeersForCpl(i))
	}

	// when 2 * gapcpl > maxCpl
	rt, err = kb.NewRoutingTable(2, kb.ConvertPeerID(local), time.Hour, pstore.NewMetrics(), 100*time.Hour)
	require.NoError(t, err)
	r = &RtRefreshManager{ctx: ctx, rt: rt, refreshKeyGenFnc: kfnc, dhtPeerId: local}
	icpl = uint(6)
	p, err = rt.GenRandPeerID(10)
	require.NoError(t, err)
	b, _ = rt.TryAddPeer(p, true)
	require.True(t, b)
	r.refreshQueryFnc = qFuncWithIgnore(rt, icpl)
	require.NoError(t, r.doRefresh(true))

	for i := uint(0); i < 10; i++ {
		if i == icpl {
			require.Equal(t, 0, rt.NPeersForCpl(i))
			continue
		}

		require.Equal(t, 1, rt.NPeersForCpl(uint(i)))
	}
	require.Equal(t, 2, rt.NPeersForCpl(10))
}
