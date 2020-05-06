package dht_pb

import (
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/record"

	ma "github.com/multiformats/go-multiaddr"
)

// PeerSignedRecordsToPBSignedPeerRecords transforms signed peer records to their
// protobuf representation.
func PeerSignedRecordsToPBSignedPeerRecords(n network.Network,
	peerRecords []*record.Envelope) []Message_SignedPeerRecord {

	prs := make([]Message_SignedPeerRecord, len(peerRecords))
	for i := range peerRecords {
		rec, err := peerRecords[i].Record()
		if err != nil {
			log.Warnf("failed to get signed record from envelope, err=%s", err)
			continue
		}

		peerrec, ok := rec.(*peer.PeerRecord)
		if !ok {
			log.Warnf("record %+v is not a peer record", rec)
			continue
		}
		c := ConnectionType(n.Connectedness(peerrec.PeerID))

		bz, err := peerRecords[i].Marshal()
		if err != nil {
			log.Warnf("failed to marshal signed record, err=%s", err)
			continue
		}

		pbrec := Message_SignedPeerRecord{}
		pbrec.SignedPeerRecord = bz
		pbrec.Connection = c
		prs[i] = pbrec
	}
	return prs
}

// RawSignedPeerRecordConn is a representation/parsed version of a protobuf
// signed peer record.
type RawSignedPeerRecordConn struct {
	Envelope *record.Envelope
	Addrs    []ma.Multiaddr
	Conn     network.Connectedness
}

// PBSignedPeerRecordsToPeerRecords converts protobuf signed peer records
// to `RawSignedPeerRecordConn` which is out internal representation.
func PBSignedPeerRecordsToPeerRecords(pbsp []Message_SignedPeerRecord) map[peer.ID]RawSignedPeerRecordConn {
	evs := make(map[peer.ID]RawSignedPeerRecordConn)

	for i := range pbsp {
		if len(pbsp[i].SignedPeerRecord) == 0 {
			continue
		}
		signedBytes := pbsp[i].SignedPeerRecord
		ev, rec, err := record.ConsumeEnvelope(signedBytes, peer.PeerRecordEnvelopeDomain)
		if err != nil {
			log.Warnf("failed to consume and validate signed record, err=%s", err)
			continue
		}

		peer, ok := rec.(*peer.PeerRecord)
		if !ok {
			log.Warnf("record %+v was not a peer record", rec)
			continue
		}

		evs[peer.PeerID] = RawSignedPeerRecordConn{ev, peer.Addrs, Connectedness(pbsp[i].Connection)}
	}
	return evs
}
