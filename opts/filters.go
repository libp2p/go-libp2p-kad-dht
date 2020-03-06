package dhtopts

import (
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"

	ma "github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr-net"
)

// QueryFilterFunc is a filter applied when considering peers to dial when querying
type QueryFilterFunc func(h host.Host, ai peer.AddrInfo) bool

// RouteTableFilterFunc is a filter applied when considering connections to keep in
// the local route table.
type RouteTableFilterFunc func(conns []network.Conn) bool

// PublicQueryFilter returns true if the peer is suspected of being publicly accessible
func PublicQueryFilter(h host.Host, ai peer.AddrInfo) bool {
	if len(ai.Addrs) == 0 {
		return false
	}

	var hasPublicAddr bool
	for _, a := range ai.Addrs {
		if isRelayAddr(a) {
			return false
		}
		if manet.IsPublicAddr(a) {
			hasPublicAddr = true
		}
	}
	return hasPublicAddr
}

// PublicRoutingTableFilter allows a peer to be added to the routing table if the connections to that peer indicate
// that it is on a public network
func PublicRoutingTableFilter(conns []network.Conn) bool {
	for _, c := range conns {
		addr := c.RemoteMultiaddr()
		if !isRelayAddr(addr) && manet.IsPublicAddr(addr) {
			return true
		}
	}
	return false
}

// PrivateQueryFilter returns true if the peer is suspected of being accessible over a shared private network
func PrivateQueryFilter(h host.Host, ai peer.AddrInfo) bool {
	conns := h.Network().ConnsToPeer(ai.ID)
	if len(conns) > 0 {
		for _, c := range conns {
			if manet.IsPrivateAddr(c.RemoteMultiaddr()) {
				return true
			}
		}
		return false
	}

	if len(ai.Addrs) == 0 {
		return false
	}

	var hasPrivateAddr bool
	for _, a := range ai.Addrs {
		if manet.IsPublicAddr(a) {
			if !isRelayAddr(a) {
				return false
			}
		} else {
			hasPrivateAddr = true
		}
	}

	return hasPrivateAddr
}

// PrivateRoutingTableFilter allows a peer to be added to the routing table if the connections to that peer indicate
// that it is on a private network
func PrivateRoutingTableFilter(conns []network.Conn) bool {
	for _, c := range conns {
		if manet.IsPrivateAddr(c.RemoteMultiaddr()) {
			return true
		}
	}
	return false
}

func isRelayAddr(a ma.Multiaddr) bool {
	val, err := a.ValueForProtocol(ma.P_CIRCUIT)
	return err != nil && val != ""
}
