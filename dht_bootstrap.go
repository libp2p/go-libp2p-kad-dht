package dht

import (
	"context"
	"github.com/multiformats/go-multiaddr"
)

// DefaultBootstrapPeers is a set of public DHT bootstrap peers provided by libp2p.
var DefaultBootstrapPeers []multiaddr.Multiaddr

// Minimum number of peers in the routing table. If we drop below this and we
// see a new peer, we trigger a bootstrap round.
var minRTRefreshThreshold = 10


// timeout for pinging one peer
const peerPingTimeout = 10 * time.Second
const (
	periodicBootstrapInterval = 2 * time.Minute
	maxNBoostrappers          = 2
)

func init() {
	for _, s := range []string{
		"/dnsaddr/bootstrap.libp2p.io/p2p/QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN",
		"/dnsaddr/bootstrap.libp2p.io/p2p/QmQCU2EcMqAqQPR2i9bChDtGNJchTbq5TbXJJ16u19uLTa",
		"/dnsaddr/bootstrap.libp2p.io/p2p/QmbLHAnMoJPWSCR5Zhtx6BHJX9KiKNN6tpvbUcqanj75Nb",
		"/dnsaddr/bootstrap.libp2p.io/p2p/QmcZf59bWwK5XFi76CZX8cbJ4BhTzzA3gU1ZjYZcYW3dwt",
		"/ip4/104.131.131.82/tcp/4001/p2p/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ", // mars.i.ipfs.io
	} {
		ma, err := multiaddr.NewMultiaddr(s)
		if err != nil {
			panic(err)
		}
		DefaultBootstrapPeers = append(DefaultBootstrapPeers, ma)
	}
}

// Bootstrap tells the DHT to get into a bootstrapped state satisfying the
// IpfsRouter interface.
func (dht *IpfsDHT) Bootstrap(ctx context.Context) error {
	dht.rtRefreshManager.RefreshNoWait()
	return nil
}

// RefreshRoutingTable tells the DHT to refresh it's routing tables.
//
// The returned channel will block until the refresh finishes, then yield the
// error and close. The channel is buffered and safe to ignore.
func (dht *IpfsDHT) RefreshRoutingTable() <-chan error {
	return dht.rtRefreshManager.Refresh(false)
}

// ForceRefresh acts like RefreshRoutingTable but forces the DHT to refresh all
// buckets in the Routing Table irrespective of when they were last refreshed.
//
// The returned channel will block until the refresh finishes, then yield the
// error and close. The channel is buffered and safe to ignore.
func (dht *IpfsDHT) ForceRefresh() <-chan error {
	return dht.rtRefreshManager.Refresh(true)
}
