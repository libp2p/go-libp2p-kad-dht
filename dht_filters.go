package dht

import (
	"bytes"
	"net"

	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	netroute "github.com/libp2p/go-netroute"

	ma "github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr-net"
)

// QueryFilterFunc is a filter applied when considering peers to dial when querying
type QueryFilterFunc func(h host.Host, ai peer.AddrInfo) bool

// RouteTableFilterFunc is a filter applied when considering connections to keep in
// the local route table.
type RouteTableFilterFunc func(h host.Host, conns []network.Conn) bool

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
func PublicRoutingTableFilter(_ host.Host, conns []network.Conn) bool {
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
func PrivateRoutingTableFilter(h host.Host, conns []network.Conn) bool {
	router, _ := netroute.New()
	myAdvertisedIPs := make([]net.IP, 0)
	for _, a := range h.Addrs() {
		if manet.IsPublicAddr(a) {
			ip, _ := manet.ToIP(a)
			myAdvertisedIPs = append(myAdvertisedIPs, ip)
		}
	}

	for _, c := range conns {
		if manet.IsPrivateAddr(c.RemoteMultiaddr()) {
			return true
		}

		if manet.IsPublicAddr(c.RemoteMultiaddr()) {
			ip, _ := manet.ToIP(c.RemoteMultiaddr())

			// if the ip is the same as one of the local host's public advertised IPs - then consider it local
			for _, i := range myAdvertisedIPs {
				if i.Equal(ip) {
					return true
				}
				if ip.To4() == nil {
					if i.To4() == nil && isEUI(ip) && sameV6Net(i, ip) {
						return true
					}
				}
			}

			// if there's no gateway - a direct host in the OS routing table - then consider it local
			if router != nil {
				_, gw, _, err := router.Route(ip)
				if gw == nil && err == nil {
					return true
				}
			}
		}
	}

	return false
}

func isEUI(ip net.IP) bool {
	// per rfc 2373
	return ip[11] == 0xff && ip[12] == 0xfe
}

func sameV6Net(a, b net.IP) bool {
	return bytes.Equal(a[0:8], b[0:8])
}

func isRelayAddr(a ma.Multiaddr) bool {
	val, err := a.ValueForProtocol(ma.P_CIRCUIT)
	return err != nil && val != ""
}
