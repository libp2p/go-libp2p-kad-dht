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

func less(pm1, pm2 *peerLatencyMetric) bool {
	p1Connectedness, p2Connectedness := pm1.connectedness, pm2.connectedness
	p1Latency, p2Latency := pm1.latency, pm2.latency

	// Compare latency assuming that connected is lower latency than unconnected
	if p1Connectedness == network.Connected {
		if p2Connectedness == network.Connected {
			return p1Latency < p2Latency
		}
		return true
	}
	if p2Connectedness == network.Connected {
		return false
	}

	// Compare latency assuming recent connection is lower latency than older connection.
	// TODO: This assumption largely stems from our latency library showing peers we know nothing about as
	// having zero latency
	if p1Connectedness == network.CanConnect {
		if p2Connectedness == network.CanConnect {
			return p1Latency > p2Latency
		}
		return true
	}
	if p2Connectedness == network.CanConnect {
		return false
	}

	// Check if either peer has proven to be unconnectable, if so rank them low
	if p1Connectedness == network.CannotConnect && p2Connectedness != network.CannotConnect {
		return false
	}
	if p2Connectedness == network.CannotConnect && p1Connectedness != network.CannotConnect {
		return true
	}

	return pm1.metric.Cmp(pm2.metric) == -1
}

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

func SortByLatency(net network.Network, metrics peerstore.Metrics) func(peers []*peerMetric) []peer.ID {
	return func(peers []*peerMetric) []peer.ID {
		metricLst := NewPeerMetricList(peers, func(p1, p2 *peerMetric) bool {
			p1Connectedness := net.Connectedness(p1.peer)
			p2Connectedness := net.Connectedness(p2.peer)

			// Compare latency assuming that connected is lower latency than unconnected
			if p1Connectedness == network.Connected {
				if p2Connectedness == network.Connected {
					return metrics.LatencyEWMA(p1.peer) > metrics.LatencyEWMA(p2.peer)
				}
				return true
			}
			if p2Connectedness == network.Connected {
				return false
			}

			// Compare latency assuming recent connection is lower latency than older connection.
			// TODO: This assumption largely stems from our latency library showing peers we know nothing about as
			// having zero latency
			if p1Connectedness == network.CanConnect {
				if p2Connectedness == network.CanConnect {
					return metrics.LatencyEWMA(p1.peer) > metrics.LatencyEWMA(p2.peer)
				}
				return true
			}
			if p2Connectedness == network.CanConnect {
				return false
			}

			// Check if either peer has proven to be unconnectable, if so rank them low
			if p1Connectedness == network.CannotConnect && p2Connectedness != network.CannotConnect {
				return false
			}
			if p2Connectedness == network.CannotConnect && p1Connectedness != network.CannotConnect {
				return true
			}

			return p1.metric.Cmp(p2.metric) == -1
		})

		sort.Stable(metricLst)
		peerLst := make([]peer.ID, metricLst.Len())
		for i := range peerLst {
			peerLst[i] = metricLst.GetPeerID(i)
		}

		return peerLst
	}
}
