package dht

import (
	"github.com/libp2p/go-libp2p-core/protocol"
)

// Deprecated: The old format did not support more than one message per stream, and is not supported
// or relevant with stream pooling. ProtocolDHT should be used instead.
const ProtocolDHTOld protocol.ID = "/ipfs/dht"

var (
	ProtocolDHT      protocol.ID = "/ipfs/kad/1.0.0"
	DefaultProtocols             = []protocol.ID{ProtocolDHT}
)
