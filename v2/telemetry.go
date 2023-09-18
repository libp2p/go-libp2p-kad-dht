package dht

import (
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/libp2p/go-libp2p-kad-dht/v2/tele"
)

// Telemetry is the struct that holds a reference to all metrics and the tracer.
// Initialize this struct with [NewTelemetry]. Make sure
// to also register the [MeterProviderOpts] with your custom or the global
// [metric.MeterProvider].
//
// To see the documentation for each metric below, check out [NewTelemetry] and the
// metric.WithDescription() calls when initializing each metric.
type Telemetry struct {
	Tracer                 trace.Tracer
	ReceivedMessages       metric.Int64Counter
	ReceivedMessageErrors  metric.Int64Counter
	ReceivedBytes          metric.Int64Histogram
	InboundRequestLatency  metric.Float64Histogram
	OutboundRequestLatency metric.Float64Histogram
	SentMessages           metric.Int64Counter
	SentMessageErrors      metric.Int64Counter
	SentRequests           metric.Int64Counter
	SentRequestErrors      metric.Int64Counter
	SentBytes              metric.Int64Histogram
	LRUCache               metric.Int64Counter
	NetworkSize            metric.Int64Counter
}

// NewWithGlobalProviders uses the global meter and tracer providers from
// opentelemetry. Check out the documentation of [MeterProviderOpts] for
// implications of using this constructor.
func NewWithGlobalProviders() (*Telemetry, error) {
	return NewTelemetry(otel.GetMeterProvider(), otel.GetTracerProvider())
}

// NewTelemetry initializes a Telemetry struct with the given meter and tracer providers.
// It constructs the different metric counters and histograms. The histograms
// have custom boundaries. Therefore, the given [metric.MeterProvider] should
// have the custom view registered that [MeterProviderOpts] returns.
func NewTelemetry(meterProvider metric.MeterProvider, tracerProvider trace.TracerProvider) (*Telemetry, error) {
	var err error

	if meterProvider == nil {
		meterProvider = otel.GetMeterProvider()
	}

	if tracerProvider == nil {
		tracerProvider = otel.GetTracerProvider()
	}

	t := &Telemetry{
		Tracer: tracerProvider.Tracer(tele.TracerName),
	}

	meter := meterProvider.Meter(tele.MeterName)

	// Initalize metrics for the DHT

	t.ReceivedMessages, err = meter.Int64Counter("received_messages", metric.WithDescription("Total number of messages received per RPC"), metric.WithUnit("1"))
	if err != nil {
		return nil, fmt.Errorf("received_messages counter: %w", err)
	}

	t.ReceivedMessageErrors, err = meter.Int64Counter("received_message_errors", metric.WithDescription("Total number of errors for messages received per RPC"), metric.WithUnit("1"))
	if err != nil {
		return nil, fmt.Errorf("received_message_errors counter: %w", err)
	}

	t.ReceivedBytes, err = meter.Int64Histogram("received_bytes", metric.WithDescription("Total received bytes per RPC"), metric.WithUnit("By"))
	if err != nil {
		return nil, fmt.Errorf("received_bytes histogram: %w", err)
	}

	t.InboundRequestLatency, err = meter.Float64Histogram("inbound_request_latency", metric.WithDescription("Latency per RPC"), metric.WithUnit("ms"))
	if err != nil {
		return nil, fmt.Errorf("inbound_request_latency histogram: %w", err)
	}

	t.OutboundRequestLatency, err = meter.Float64Histogram("outbound_request_latency", metric.WithDescription("Latency per RPC"), metric.WithUnit("ms"))
	if err != nil {
		return nil, fmt.Errorf("outbound_request_latency histogram: %w", err)
	}

	t.SentMessages, err = meter.Int64Counter("sent_messages", metric.WithDescription("Total number of messages sent per RPC"), metric.WithUnit("1"))
	if err != nil {
		return nil, fmt.Errorf("sent_messages counter: %w", err)
	}

	t.SentMessageErrors, err = meter.Int64Counter("sent_message_errors", metric.WithDescription("Total number of errors for messages sent per RPC"), metric.WithUnit("1"))
	if err != nil {
		return nil, fmt.Errorf("sent_message_errors counter: %w", err)
	}

	t.SentRequests, err = meter.Int64Counter("sent_requests", metric.WithDescription("Total number of requests sent per RPC"), metric.WithUnit("1"))
	if err != nil {
		return nil, fmt.Errorf("sent_requests counter: %w", err)
	}

	t.SentRequestErrors, err = meter.Int64Counter("sent_request_errors", metric.WithDescription("Total number of errors for requests sent per RPC"), metric.WithUnit("1"))
	if err != nil {
		return nil, fmt.Errorf("sent_request_errors counter: %w", err)
	}

	t.SentBytes, err = meter.Int64Histogram("sent_bytes", metric.WithDescription("Total sent bytes per RPC"), metric.WithUnit("By"))
	if err != nil {
		return nil, fmt.Errorf("sent_bytes histogram: %w", err)
	}

	t.LRUCache, err = meter.Int64Counter("lru_cache", metric.WithDescription("Cache hit or miss counter"), metric.WithUnit("1"))
	if err != nil {
		return nil, fmt.Errorf("lru_cache counter: %w", err)
	}

	t.NetworkSize, err = meter.Int64Counter("network_size", metric.WithDescription("Network size estimation"), metric.WithUnit("1"))
	if err != nil {
		return nil, fmt.Errorf("network_size counter: %w", err)
	}

	return t, nil
}
