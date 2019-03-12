package metrics

import (
	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
	"go.opencensus.io/metric"
	"go.opencensus.io/metric/metricdata"
	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
)

var (
	messageSizeBytesDistribution    = view.Distribution(0, 1, 10, 30, 70, 100, 200, 1000, 10000)
	latencyMillisecondsDistribution = view.Distribution(0, 1, 10, 100, 300, 1000, 10000, 100000)
)

// Keys
var (
	KeyMessageType, _ = tag.NewKey("message_type")
	KeyLocalPeerID, _ = tag.NewKey("local_peer_id")
	// KeyInstanceID identifies a DHT instance by the pointer address. Useful for differentiating
	// between different DHTs that have the same peer id.
	KeyInstanceID, _ = tag.NewKey("instance_id")
)

func UpsertMessageType(m *pb.Message) tag.Mutator {
	return tag.Upsert(KeyMessageType, m.Type.String())
}

const namePrefix = "libp2p_dht_kad_"

var (
	ReceivedMessages             = stats.Int64(namePrefix+"received_messages", "Total number of messages received", stats.UnitDimensionless)
	ReceivedMessageSizeBytes     = stats.Int64(namePrefix+"received_message_size_bytes", "Received message sizes", stats.UnitBytes)
	InboundRequestHandlingTimeMs = stats.Float64(namePrefix+"inbound_request_handling_time_ms", "Inbound request processing time", stats.UnitMilliseconds)

	SentMessages                     = stats.Int64(namePrefix+"sent_messages", "Total number of messages sent", stats.UnitDimensionless)
	SentMessageSizeBytes             = stats.Int64(namePrefix+"sent_message_size_bytes", "Sent message sizes", stats.UnitBytes)
	OutboundRequestResponseLatencyMs = stats.Float64(namePrefix+"outbound_request_response_latency_ms", "Outbound request latency", stats.UnitMilliseconds)
	MessageWriteLatencyMs            = stats.Float64(
		namePrefix+"message_write_latency_ms",
		"Time between wanting to send a message, and writing it",
		stats.UnitMilliseconds,
	)

	RoutingTablePeersAdded   = stats.Int64(namePrefix+"routing_table_peers_added", "", stats.UnitDimensionless)
	RoutingTablePeersRemoved = stats.Int64(namePrefix+"routing_table_peers_removed", "", stats.UnitDimensionless)

	GaugeRegistry             = metric.NewRegistry()
	RoutingTableNumEntries, _ = GaugeRegistry.AddInt64DerivedGauge(
		"routing_table_num_entries", "", metricdata.UnitDimensionless,
		KeyLocalPeerID.Name(), KeyInstanceID.Name())
)

var Views = []*view.View{{
	Measure:     ReceivedMessages,
	TagKeys:     []tag.Key{KeyMessageType, KeyLocalPeerID, KeyInstanceID},
	Aggregation: view.Count(),
}, {
	Measure:     ReceivedMessageSizeBytes,
	TagKeys:     []tag.Key{KeyMessageType, KeyLocalPeerID, KeyInstanceID},
	Aggregation: messageSizeBytesDistribution,
}, {
	Measure:     InboundRequestHandlingTimeMs,
	TagKeys:     []tag.Key{KeyMessageType, KeyLocalPeerID, KeyInstanceID},
	Aggregation: latencyMillisecondsDistribution,
}, {
	Measure:     SentMessages,
	TagKeys:     []tag.Key{KeyMessageType, KeyLocalPeerID, KeyInstanceID},
	Aggregation: view.Count(),
}, {
	Measure:     OutboundRequestResponseLatencyMs,
	TagKeys:     []tag.Key{KeyMessageType, KeyLocalPeerID, KeyInstanceID},
	Aggregation: latencyMillisecondsDistribution,
}, {
	Measure:     SentMessageSizeBytes,
	TagKeys:     []tag.Key{KeyMessageType, KeyLocalPeerID, KeyInstanceID},
	Aggregation: messageSizeBytesDistribution,
}, {
	Measure:     MessageWriteLatencyMs,
	TagKeys:     []tag.Key{KeyMessageType, KeyLocalPeerID, KeyInstanceID},
	Aggregation: view.Distribution(0, 1, 10, 100),
}, {
	Measure:     RoutingTablePeersAdded,
	TagKeys:     []tag.Key{KeyLocalPeerID, KeyInstanceID},
	Aggregation: view.Count(),
}, {
	Measure:     RoutingTablePeersRemoved,
	TagKeys:     []tag.Key{KeyLocalPeerID, KeyInstanceID},
	Aggregation: view.Count(),
}}
