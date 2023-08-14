package dht

import (
	"github.com/libp2p/go-libp2p/core/peer"
)

// defaultBootstrapPeers is a set of hard-coded public DHT bootstrap peers
// operated by Protocol Labs. This slice is filled in the init() method.
var defaultBootstrapPeers []peer.AddrInfo

func init() {
	for _, s := range []string{
		"/dnsaddr/bootstrap.libp2p.io/p2p/QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN",
		"/dnsaddr/bootstrap.libp2p.io/p2p/QmQCU2EcMqAqQPR2i9bChDtGNJchTbq5TbXJJ16u19uLTa",
		"/dnsaddr/bootstrap.libp2p.io/p2p/QmbLHAnMoJPWSCR5Zhtx6BHJX9KiKNN6tpvbUcqanj75Nb",
		"/dnsaddr/bootstrap.libp2p.io/p2p/QmcZf59bWwK5XFi76CZX8cbJ4BhTzzA3gU1ZjYZcYW3dwt",
		"/ip4/104.131.131.82/tcp/4001/p2p/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ", // mars.i.ipfs.io
	} {
		addrInfo, err := peer.AddrInfoFromString(s)
		if err != nil {
			panic(err)
		}
		defaultBootstrapPeers = append(defaultBootstrapPeers, *addrInfo)
	}
}

// DefaultBootstrapPeers returns hard-coded public DHT bootstrap peers operated
// by Protocol Labs. You can configure your own set of bootstrap peers by
// overwriting the corresponding Config field.
func DefaultBootstrapPeers() []peer.AddrInfo {
	peers := make([]peer.AddrInfo, len(defaultBootstrapPeers))
	copy(peers, defaultBootstrapPeers)
	return peers
}
