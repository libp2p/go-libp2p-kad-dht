package tele

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/sdk/instrumentation"
	motel "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/trace"
)

// ctxKey is an unexported type alias for the value of a context key. This is
// used to attach metric values to a context and get them out of a context.
type ctxKey struct{}

var (
	meterName  = "github.com/libp2p/go-libp2p-kad-dht/v2"
	tracerName = "go-libp2p-kad-dht"

	// attrsCtxKey is the actual context key value that's used as a key for
	// metric values that are attached to a context.
	attrsCtxKey = ctxKey{}
)

// Telemetry is the struct that holds a reference to all metrics and the tracer.
// Initialize this struct with [New] or [NewWithGlobalProviders]. Make sure
// to also register the [MeterProviderOpts] with your custom or the global
// [metric.MeterProvider].
//
// To see the documentation for each metric below, check out [New] and the
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
	return New(otel.GetMeterProvider(), otel.GetTracerProvider())
}

// New initializes a Telemetry struct with the given meter and tracer providers.
// It constructs the different metric counters and histograms. The histograms
// have custom boundaries. Therefore, the given [metric.MeterProvider] should
// have the custom view registered that [MeterProviderOpts] returns.
func New(meterProvider metric.MeterProvider, tracerProvider trace.TracerProvider) (*Telemetry, error) {
	var err error

	if meterProvider == nil {
		meterProvider = otel.GetMeterProvider()
	}

	if tracerProvider == nil {
		tracerProvider = otel.GetTracerProvider()
	}

	t := &Telemetry{
		Tracer: tracerProvider.Tracer(tracerName),
	}

	meter := meterProvider.Meter(meterName)
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

// MeterProviderOpts is a method that returns metric options. Make sure
// to register these options to your [metric.MeterProvider]. Unfortunately,
// attaching these options to an already existing [metric.MeterProvider]
// is not possible. Therefore, you can't just register the options with the
// global MeterProvider that is returned by [otel.GetMeterProvider].
// One example to register a new [metric.MeterProvider] would be:
//
//	provider := metric.NewMeterProvider(tele.MeterProviderOpts()...) // <-- also add your options, like a metric reader
//	otel.SetMeterProvider(provider)
//
// Then you can use [NewWithGlobalProviders] and it will use a correctly
// configured meter provider.
//
// The options that MeterProviderOpts returns are just custom histogram
// boundaries for a few metrics. In the future, we could reconsider these
// boundaries because we just blindly ported them from v1 to v2 of
// go-libp2p-kad-dht.
var MeterProviderOpts = []motel.Option{
	motel.WithView(motel.NewView(
		motel.Instrument{Name: "*_bytes", Scope: instrumentation.Scope{Name: meterName}},
		motel.Stream{
			Aggregation: motel.AggregationExplicitBucketHistogram{
				Boundaries: []float64{1024, 2048, 4096, 16384, 65536, 262144, 1048576, 4194304, 16777216, 67108864, 268435456, 1073741824, 4294967296},
			},
		},
	)),
	motel.WithView(motel.NewView(
		motel.Instrument{Name: "*_request_latency", Scope: instrumentation.Scope{Name: meterName}},
		motel.Stream{
			Aggregation: motel.AggregationExplicitBucketHistogram{
				Boundaries: []float64{0.01, 0.05, 0.1, 0.3, 0.6, 0.8, 1, 2, 3, 4, 5, 6, 8, 10, 13, 16, 20, 25, 30, 40, 50, 65, 80, 100, 130, 160, 200, 250, 300, 400, 500, 650, 800, 1000, 2000, 5000, 10000, 20000, 50000, 100000},
			},
		},
	)),
}

// AttrInstanceID identifies a dht instance by the pointer address.
// Useful for differentiating between different DHTs that have the same peer id.
func AttrInstanceID(instanceID string) attribute.KeyValue {
	return attribute.String("instance_id", instanceID)
}

func AttrPeerID(pid string) attribute.KeyValue {
	return attribute.String("peer_id", pid)
}

func AttrCacheHit(hit bool) attribute.KeyValue {
	return attribute.Bool("hit", hit)
}

// AttrRecordType is currently only used for the provider backend LRU cache
func AttrRecordType(val string) attribute.KeyValue {
	return attribute.String("record_type", val)
}

func AttrMessageType(val string) attribute.KeyValue {
	return attribute.String("message_type", val)
}

func AttrKey(val string) attribute.KeyValue {
	return attribute.String("key", val)
}

// WithAttributes is a function that attaches the provided attributes to the
// given context. The given attributes will overwrite any already existing ones.
func WithAttributes(ctx context.Context, attrs ...attribute.KeyValue) context.Context {
	set := attribute.NewSet(attrs...)
	val := ctx.Value(attrsCtxKey)
	if val != nil {
		existing, ok := val.(attribute.Set)
		if ok {
			set = attribute.NewSet(append(existing.ToSlice(), attrs...)...)
		}
	}
	return context.WithValue(ctx, attrsCtxKey, set)
}

// FromContext returns the attributes that were previously associated with the
// given context via [WithAttributes] plus any attributes that are also passed
// into this function. The given attributes will take precedence over any
// attributes stored in the context.
func FromContext(ctx context.Context, attrs ...attribute.KeyValue) attribute.Set {
	val := ctx.Value(attrsCtxKey)
	if val == nil {
		return attribute.NewSet(attrs...)
	}

	set, ok := val.(attribute.Set)
	if !ok {
		return attribute.NewSet(attrs...)
	}

	return attribute.NewSet(append(set.ToSlice(), attrs...)...)
}
