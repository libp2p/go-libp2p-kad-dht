package metrics

import (
	"context"

	pb "github.com/libp2p/go-libp2p-kad-dht/pb"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Unit dimensions for stats
const (
	unitMessage      = "{message}"
	unitCount        = "{count}"
	unitError        = "{error}"
	unitBytes        = "By"
	unitMilliseconds = "ms"
)

// Attribute Keys
const (
	KeyMessageType = "message_type"
	KeyPeerID      = "peer_id"
	// KeyInstanceID identifies a dht instance by the pointer address.
	// Useful for differentiating between different dhts that have the same peer id.
	KeyInstanceID = "instance_id"
)

// UpsertMessageType is a convenience upserts the message type
// of a pb.Message into the KeyMessageType.
func UpsertMessageType(m *pb.Message) attribute.KeyValue {
	return attribute.Key(KeyMessageType).String(m.Type.String())
}

var (
	meter = otel.Meter("github.com/libp2p/go-libp2p-kad-dht")

	// dht net metrics
	receivedMessages, _ = meter.Int64Counter(
		"rpc.inbound.messages",
		metric.WithDescription("Total number of messages received per RPC"),
		metric.WithUnit(unitMessage),
	)
	receivedMessageErrors, _ = meter.Int64Counter(
		"rpc.inbound.message_errors",
		metric.WithDescription("Total number of errors for messages received per RPC"),
		metric.WithUnit(unitError),
	)
	receivedBytes, _ = meter.Int64Counter(
		"rpc.inbound.bytes",
		metric.WithDescription("Total received bytes per RPC"),
		metric.WithUnit(unitBytes),
	)
	inboundRequestLatency, _ = meter.Float64Counter(
		"rpc.inbound.request_latency",
		metric.WithDescription("Latency per RPC"),
		metric.WithUnit(unitMilliseconds),
	)

	// message manager metrics
	outboundRequestLatency, _ = meter.Float64Counter(
		"rpc.outbound.request_latency",
		metric.WithDescription("Latency per RPC"),
		metric.WithUnit(unitMilliseconds),
	)
	sentMessages, _ = meter.Int64Counter(
		"rpc.outbound.messages",
		metric.WithDescription("Total number of messages sent per RPC"),
		metric.WithUnit(unitMessage),
	)
	sentMessageErrors, _ = meter.Int64Counter(
		"rpc.outbound.message_errors",
		metric.WithDescription("Total number of errors for messages sent per RPC"),
		metric.WithUnit(unitError),
	)
	sentRequests, _ = meter.Int64Counter(
		"rpc.outbound.requests",
		metric.WithDescription("Total number of requests sent per RPC"),
		metric.WithUnit(unitCount),
	)
	sentRequestErrors, _ = meter.Int64Counter(
		"rpc.outbound.request_errors",
		metric.WithDescription("Total number of errors for requests sent per RPC"),
		metric.WithUnit(unitError),
	)
	sentBytes, _ = meter.Int64Counter(
		"rpc.outbound.bytes",
		metric.WithDescription("Total sent bytes per RPC"),
		metric.WithUnit(unitBytes),
	)
	networkSize, _ = meter.Int64Gauge(
		"network.size",
		metric.WithDescription("Network size estimation"),
		metric.WithUnit(unitCount),
	)
)

func RecordMessageRecvOK(ctx context.Context, msgLen int64) {
	ctxAttrSet := AttributesFromContext(ctx)
	attrSetOpt := metric.WithAttributeSet(ctxAttrSet)

	receivedMessages.Add(ctx, 1, attrSetOpt)
	receivedBytes.Add(ctx, msgLen, attrSetOpt)
}

func RecordMessageRecvErr(ctx context.Context, messageType string, msgLen int64) {
	ctxAttrSet := AttributesFromContext(ctx)
	attrSetOpt := metric.WithAttributeSet(ctxAttrSet)
	attrOpt := metric.WithAttributes(attribute.Key(KeyMessageType).String("UNKNOWN"))

	receivedMessages.Add(ctx, 1, attrSetOpt, attrOpt)
	receivedMessageErrors.Add(ctx, 1, attrSetOpt, attrOpt)
	receivedBytes.Add(ctx, msgLen, attrSetOpt, attrOpt)
}

func RecordMessageHandleErr(ctx context.Context) {
	ctxAttrSet := AttributesFromContext(ctx)
	attrSetOpt := metric.WithAttributeSet(ctxAttrSet)

	receivedMessageErrors.Add(ctx, 1, attrSetOpt)
}

func RecordRequestLatency(ctx context.Context, latencyMs float64) {
	ctxAttrSet := AttributesFromContext(ctx)
	attrSetOpt := metric.WithAttributeSet(ctxAttrSet)

	inboundRequestLatency.Add(ctx, latencyMs, attrSetOpt)
}

func RecordRequestSendErr(ctx context.Context) {
	ctxAttrSet := AttributesFromContext(ctx)
	attrSetOpt := metric.WithAttributeSet(ctxAttrSet)

	sentMessages.Add(ctx, 1, attrSetOpt)
	sentMessageErrors.Add(ctx, 1, attrSetOpt)
}

func RecordRequestSendOK(ctx context.Context, sentBytesLen int64, latencyMs float64) {
ctxAttrSet := AttributesFromContext(ctx)
	attrSetOpt := metric.WithAttributeSet(ctxAttrSet)
	attrOpt := metric.WithAttributes(attribute.Key(KeyMessageType).String("UNKNOWN"))
	
	sentRequests.Add(ctx, 1, attrSetOpt, attrOpt)
	sentBytes.Add(ctx, 1, attrSetOpt, attrOpt)
	outboundRequestLatency.Add(ctx,latencyMs, attrSetOpt, attrOpt)
}

func RecordMessageSendOK(ctx context.Context, sentBytesLen int64) {
	ctxAttrSet := AttributesFromContext(ctx)
	attrSetOpt := metric.WithAttributeSet(ctxAttrSet)

	sentMessages.Add(ctx, 1, attrSetOpt)
	sentBytes.Add(ctx, sentBytesLen, attrSetOpt)
}

func RecordMessageSendErr(ctx context.Context) {
	ctxAttrSet := AttributesFromContext(ctx)
	attrSetOpt := metric.WithAttributeSet(ctxAttrSet)

	sentMessages.Add(ctx, 1, attrSetOpt)
	sentMessageErrors.Add(ctx, 1, attrSetOpt)
}

func RecordNetworkSize(ns int64) {
	networkSize.Record(context.Background(), int64(ns))
}
