package kpeerset

import (
	"math/big"
	"sort"
	"time"

	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/peerstore"
)

type peerLatencyMetric struct {
	peerMetric
	connectedness network.Connectedness
	latency       time.Duration
}

type peerLatencyMetricList []peerLatencyMetric

func (p peerLatencyMetricList) Len() int { return len(p) }
func (p peerLatencyMetricList) Less(i, j int) bool {
	pm1, pm2 := p[i], p[j]
	return calculationLess(pm1, pm2)
}
func (p peerLatencyMetricList) Swap(i, j int)           { p[i], p[j] = p[j], p[i] }
func (p peerLatencyMetricList) GetPeerID(i int) peer.ID { return p[i].peer }

func calculationLess(pm1, pm2 peerLatencyMetric) bool {
	return calc(pm1).Cmp(calc(pm2)) == -1
}

func calc(pm peerLatencyMetric) *big.Int {
	var c int64
	switch pm.connectedness {
	case network.Connected:
		c = 1
	case network.CanConnect:
		c = 5
	case network.CannotConnect:
		c = 10000
	default:
		c = 20
	}

	l := int64(pm.latency)
	if l <= 0 {
		l = int64(time.Second) * 10
	}

	res := big.NewInt(c)
	tmp := big.NewInt(l)
	res.Mul(res, tmp)
	res.Mul(res, pm.metric)

	return res
}

var _ SortablePeers = (*peerLatencyMetricList)(nil)

func PeersSortedByLatency(peers []IPeerMetric, net network.Network, metrics peerstore.Metrics) SortablePeers {
	lst := make(peerLatencyMetricList, len(peers))
	for i := range lst {
		p := peers[i].Peer()
		lst[i] = peerLatencyMetric{
			peerMetric:    peerMetric{peer: p, metric: peers[i].Metric()},
			connectedness: net.Connectedness(p),
			latency:       metrics.LatencyEWMA(p),
		}
	}
	sort.Sort(lst)
	return lst
}
