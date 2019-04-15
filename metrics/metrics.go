package metrics

import (
	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
)

var (
	defaultBytesDistribution        = view.Distribution(1024, 2048, 4096, 16384, 65536, 262144, 1048576, 4194304, 16777216, 67108864, 268435456, 1073741824, 4294967296)
	defaultMillisecondsDistribution = view.Distribution(0.01, 0.05, 0.1, 0.3, 0.6, 0.8, 1, 2, 3, 4, 5, 6, 8, 10, 13, 16, 20, 25, 30, 40, 50, 65, 80, 100, 130, 160, 200, 250, 300, 400, 500, 650, 800, 1000, 2000, 5000, 10000, 20000, 50000, 100000)
)

// Keys
var (
	KeyMessageType, _ = tag.NewKey("message_type")
	KeyPeerID, _      = tag.NewKey("peer_id")
	// KeyInstanceID identifies a dht instance by the pointer address.
	// Useful for differentiating between different dhts that have the same peer id.
	KeyInstanceID, _ = tag.NewKey("instance_id")
	KeyRpcType, _    = tag.NewKey("rpc_type")
	KeyError, _      = tag.NewKey("error")
)

// UpsertMessageType is a convenience upserts the message type
// of a pb.Message into the KeyMessageType.
func UpsertMessageType(m *pb.Message) tag.Mutator {
	return tag.Upsert(KeyMessageType, m.Type.String())
}

// Measures
var (
	ReceivedMessages      = stats.Int64("libp2p.io/dht/kad/received_messages", "Total number of messages received per RPC", stats.UnitDimensionless)
	ReceivedMessageErrors = stats.Int64("libp2p.io/dht/kad/received_message_errors", "Total number of errors for messages received per RPC", stats.UnitDimensionless)
	ReceivedBytes         = stats.Int64("libp2p.io/dht/kad/received_bytes", "Total received bytes per RPC", stats.UnitBytes)
	InboundRequestLatency = stats.Float64("libp2p.io/dht/kad/inbound_request_latency", "Latency per RPC", stats.UnitMilliseconds)
	OutboundRpcLatencyMs  = stats.Float64("libp2p.io/dht/kad/outbound_rpc_latency_ms", "Time to send message and receive response, if any", stats.UnitMilliseconds)
	OutboundMessageBytes  = stats.Int64("libp2p.io/dht/kad/outbound_message_bytes", "Outbound message sizes", stats.UnitBytes)
)

// Views
var (
	ReceivedMessagesView = &view.View{
		Measure:     ReceivedMessages,
		TagKeys:     []tag.Key{KeyMessageType, KeyPeerID, KeyInstanceID},
		Aggregation: view.Count(),
	}
	ReceivedMessageErrorsView = &view.View{
		Measure:     ReceivedMessageErrors,
		TagKeys:     []tag.Key{KeyMessageType, KeyPeerID, KeyInstanceID},
		Aggregation: view.Count(),
	}
	ReceivedBytesView = &view.View{
		Measure:     ReceivedBytes,
		TagKeys:     []tag.Key{KeyMessageType, KeyPeerID, KeyInstanceID},
		Aggregation: defaultBytesDistribution,
	}
	InboundRequestLatencyView = &view.View{
		Measure:     InboundRequestLatency,
		TagKeys:     []tag.Key{KeyMessageType, KeyPeerID, KeyInstanceID},
		Aggregation: defaultMillisecondsDistribution,
	}

	OutboundRpcLatencyMsView = &view.View{
		Measure:     OutboundRpcLatencyMs,
		TagKeys:     []tag.Key{KeyMessageType, KeyPeerID, KeyInstanceID, KeyRpcType, KeyError},
		Aggregation: defaultMillisecondsDistribution,
	}
	OutboundMessageBytesView = &view.View{
		Measure:     OutboundMessageBytes,
		TagKeys:     []tag.Key{KeyMessageType, KeyPeerID, KeyInstanceID, KeyRpcType, KeyError},
		Aggregation: defaultBytesDistribution,
	}
)
