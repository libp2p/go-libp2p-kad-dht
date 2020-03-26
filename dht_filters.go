package dht

import (
	"bytes"
	"net"

	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	netroute "github.com/libp2p/go-netroute"

	ma "github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr-net"
)

// QueryFilterFunc is a filter applied when considering peers to dial when querying
type QueryFilterFunc func(dht *IpfsDHT, ai peer.AddrInfo) bool

// RouteTableFilterFunc is a filter applied when considering connections to keep in
// the local route table.
type RouteTableFilterFunc func(dht *IpfsDHT, conns []network.Conn) bool

// PublicQueryFilter returns true if the peer is suspected of being publicly accessible
func PublicQueryFilter(_ *IpfsDHT, ai peer.AddrInfo) bool {
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

var _ QueryFilterFunc = PublicQueryFilter

// PublicRoutingTableFilter allows a peer to be added to the routing table if the connections to that peer indicate
// that it is on a public network
func PublicRoutingTableFilter(dht *IpfsDHT, conns []network.Conn) bool {
	if len(conns) == 0 {
		return false
	}

	// Is one of these connections to a public address?
	for _, c := range conns {
		addr := c.RemoteMultiaddr()
		if !isRelayAddr(addr) && manet.IsPublicAddr(addr) {
			return true
		}
	}

	// Do we have a public address for this peer?
	id := conns[0].RemotePeer()
	known := dht.peerstore.PeerInfo(id)
	for _, a := range known.Addrs {
		if !isRelayAddr(a) && manet.IsPublicAddr(a) {
			return true
		}
	}

	return false
}

var _ RouteTableFilterFunc = PublicRoutingTableFilter

// PrivateQueryFilter doens't currently restrict which peers we are willing to query from the local DHT.
func PrivateQueryFilter(dht *IpfsDHT, ai peer.AddrInfo) bool {
	return len(ai.Addrs) > 0
}

var _ QueryFilterFunc = PrivateQueryFilter

// PrivateRoutingTableFilter allows a peer to be added to the routing table if the connections to that peer indicate
// that it is on a private network
func PrivateRoutingTableFilter(dht *IpfsDHT, conns []network.Conn) bool {
	router, _ := netroute.New()
	myAdvertisedIPs := make([]net.IP, 0)
	for _, a := range dht.Host().Addrs() {
		if manet.IsPublicAddr(a) && !isRelayAddr(a) {
			ip, err := manet.ToIP(a)
			if err != nil {
				continue
			}
			myAdvertisedIPs = append(myAdvertisedIPs, ip)
		}
	}

	for _, c := range conns {
		ra := c.RemoteMultiaddr()
		if manet.IsPrivateAddr(ra) || isRelayAddr(ra) {
			return true
		}

		if manet.IsPublicAddr(ra) {
			ip, err := manet.ToIP(ra)
			if err != nil {
				continue
			}

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

var _ RouteTableFilterFunc = PrivateRoutingTableFilter

func isEUI(ip net.IP) bool {
	// per rfc 2373
	return ip[11] == 0xff && ip[12] == 0xfe
}

func sameV6Net(a, b net.IP) bool {
	return bytes.Equal(a[0:8], b[0:8])
}

func isRelayAddr(a ma.Multiaddr) bool {
	for _, p := range a.Protocols() {
		if p.Code == ma.P_CIRCUIT {
			return true
		}
	}
	return false
}
