package dht_pb

import (
	"errors"

	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/record"

	logging "github.com/ipfs/go-log"
	b58 "github.com/mr-tron/base58/base58"
	ma "github.com/multiformats/go-multiaddr"
)

var log = logging.Logger("dht.pb")

type PeerRoutingInfo struct {
	peer.AddrInfo
	network.Connectedness
}

// NewMessage constructs a new dht message with given type, key, and level
func NewMessage(typ Message_MessageType, key []byte, level int) *Message {
	m := &Message{
		Type: typ,
		Key:  key,
	}
	m.SetClusterLevel(level)
	return m
}

func peerRoutingInfoToPBPeer(p PeerRoutingInfo) *Message_Peer {
	pbp := new(Message_Peer)

	pbp.Addrs = make([][]byte, len(p.Addrs))
	for i, maddr := range p.Addrs {
		pbp.Addrs[i] = maddr.Bytes() // Bytes, not String. Compressed.
	}
	s := string(p.ID)
	pbp.Id = []byte(s)
	c := ConnectionType(p.Connectedness)
	pbp.Connection = c
	return pbp
}

func peerInfoToPBPeer(p peer.AddrInfo) *Message_Peer {
	pbp := new(Message_Peer)

	pbp.Addrs = make([][]byte, len(p.Addrs))
	for i, maddr := range p.Addrs {
		pbp.Addrs[i] = maddr.Bytes() // Bytes, not String. Compressed.
	}
	pbp.Id = []byte(p.ID)
	return pbp
}

func peerRecordToPbSignedPeerRecord(peerRecord *record.Envelope) (*Message_SignedPeerRecord, error) {
	bz, err := peerRecord.Marshal()
	if err != nil {
		return nil, err
	}
	return &Message_SignedPeerRecord{SignedPeerRecord: bz}, nil
}

// PBPeerToPeer turns a *Message_Peer into its peer.AddrInfo counterpart
func PBPeerToPeerInfo(pbp *Message_Peer) *peer.AddrInfo {
	return &peer.AddrInfo{
		ID:    peer.ID(pbp.GetId()),
		Addrs: pbp.Addresses(),
	}
}

// RawPeerInfosToPBPeers converts a slice of Peers into a slice of *Message_Peers,
// ready to go out on the wire.
func RawPeerInfosToPBPeers(peers []peer.AddrInfo) []*Message_Peer {
	pbpeers := make([]*Message_Peer, len(peers))
	for i, p := range peers {
		pbpeers[i] = peerInfoToPBPeer(p)
	}
	return pbpeers
}

// PeersToPBPeers converts given []peer.Peer into a set of []*Message_Peer,
// which can be written to a message and sent out. the key thing this function
// does (in addition to PeersToPBPeers) is set the ConnectionType with
// information from the given network.Network.
func PeerInfosToPBPeers(n network.Network, peers []peer.AddrInfo) []*Message_Peer {
	pbps := RawPeerInfosToPBPeers(peers)
	for i, pbp := range pbps {
		c := ConnectionType(n.Connectedness(peers[i].ID))
		pbp.Connection = c
	}
	return pbps
}

// PeerSignedRecordsToPBSignedPeerRecords transforms signed peer records to a set
// of []*Message_SignedPeerRecord which is their Proto representation.
func PeerSignedRecordsToPBSignedPeerRecords(n network.Network,
	peerRecords []*record.Envelope) ([]*Message_SignedPeerRecord, error) {

	prs := make([]*Message_SignedPeerRecord, len(peerRecords))
	for i := range peerRecords {
		rec, err := peerRecords[i].Record()
		if err != nil {
			return nil, err
		}
		peerrec, ok := rec.(*peer.PeerRecord)
		if !ok {
			return nil, errors.New("record should be a peer record")
		}
		c := ConnectionType(n.Connectedness(peerrec.PeerID))

		pbrec, err := peerRecordToPbSignedPeerRecord(peerRecords[i])
		if err != nil {
			return nil, err
		}
		pbrec.Connection = c
		prs[i] = pbrec
	}
	return prs, nil
}

func PeerRoutingInfosToPBPeers(peers []PeerRoutingInfo) []*Message_Peer {
	pbpeers := make([]*Message_Peer, len(peers))
	for i, p := range peers {
		pbpeers[i] = peerRoutingInfoToPBPeer(p)
	}
	return pbpeers
}

// PBPeersToPeerInfos converts given []*Message_Peer into []peer.AddrInfo
// Invalid addresses will be silently omitted.
func PBPeersToPeerInfos(pbps []*Message_Peer) []*peer.AddrInfo {
	peers := make([]*peer.AddrInfo, 0, len(pbps))
	for _, pbp := range pbps {
		peers = append(peers, PBPeerToPeerInfo(pbp))
	}
	return peers
}

// RawSignedPeerRecordConn is a representation/parsed version of a PB
// signed peer record.
type RawSignedPeerRecordConn struct {
	Envelope *record.Envelope
	Addrs    []ma.Multiaddr
	Conn     network.Connectedness
}

// PBSignedPeerRecordsToPeerRecords converts PB signed peer records to envelopes
func PBSignedPeerRecordsToPeerRecords(pbsp []*Message_SignedPeerRecord) (map[peer.ID]*RawSignedPeerRecordConn, error) {
	evs := make(map[peer.ID]*RawSignedPeerRecordConn)

	for i := range pbsp {
		if pbsp[i] == nil || len(pbsp[i].SignedPeerRecord) == 0 {
			continue
		}
		signedBytes := pbsp[i].SignedPeerRecord
		ev, rec, err := record.ConsumeEnvelope(signedBytes, peer.PeerRecordEnvelopeDomain)
		if err != nil {
			return nil, err
		}

		peer := rec.(*peer.PeerRecord)

		evs[peer.PeerID] = &RawSignedPeerRecordConn{ev, peer.Addrs, Connectedness(pbsp[i].Connection)}
	}
	return evs, nil
}

// Addresses returns a multiaddr associated with the Message_Peer entry
func (m *Message_Peer) Addresses() []ma.Multiaddr {
	if m == nil {
		return nil
	}

	maddrs := make([]ma.Multiaddr, 0, len(m.Addrs))
	for _, addr := range m.Addrs {
		maddr, err := ma.NewMultiaddrBytes(addr)
		if err != nil {
			log.Warningf("error decoding Multiaddr for peer: %s", m.GetId())
			continue
		}

		maddrs = append(maddrs, maddr)
	}
	return maddrs
}

// GetClusterLevel gets and adjusts the cluster level on the message.
// a +/- 1 adjustment is needed to distinguish a valid first level (1) and
// default "no value" protobuf behavior (0)
func (m *Message) GetClusterLevel() int {
	level := m.GetClusterLevelRaw() - 1
	if level < 0 {
		return 0
	}
	return int(level)
}

// SetClusterLevel adjusts and sets the cluster level on the message.
// a +/- 1 adjustment is needed to distinguish a valid first level (1) and
// default "no value" protobuf behavior (0)
func (m *Message) SetClusterLevel(level int) {
	lvl := int32(level)
	m.ClusterLevelRaw = lvl + 1
}

// Loggable turns a Message into machine-readable log output
func (m *Message) Loggable() map[string]interface{} {
	return map[string]interface{}{
		"message": map[string]string{
			"type": m.Type.String(),
			"key":  b58.Encode([]byte(m.GetKey())),
		},
	}
}

// ConnectionType returns a Message_ConnectionType associated with the
// network.Connectedness.
func ConnectionType(c network.Connectedness) Message_ConnectionType {
	switch c {
	default:
		return Message_NOT_CONNECTED
	case network.NotConnected:
		return Message_NOT_CONNECTED
	case network.Connected:
		return Message_CONNECTED
	case network.CanConnect:
		return Message_CAN_CONNECT
	case network.CannotConnect:
		return Message_CANNOT_CONNECT
	}
}

// Connectedness returns an network.Connectedness associated with the
// Message_ConnectionType.
func Connectedness(c Message_ConnectionType) network.Connectedness {
	switch c {
	default:
		return network.NotConnected
	case Message_NOT_CONNECTED:
		return network.NotConnected
	case Message_CONNECTED:
		return network.Connected
	case Message_CAN_CONNECT:
		return network.CanConnect
	case Message_CANNOT_CONNECT:
		return network.CannotConnect
	}
}
