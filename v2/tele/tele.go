package tele

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/instrumentation"
	motel "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/trace"
)

// ctxKey is an unexported type alias for the value of a context key. This is
// used to attach metric values to a context and get them out of a context.
type ctxKey struct{}

const (
	MeterName  = "github.com/libp2p/go-libp2p-kad-dht/v2"
	TracerName = "go-libp2p-kad-dht"
)

// attrsCtxKey is the actual context key value that's used as a key for
// metric values that are attached to a context.
var attrsCtxKey = ctxKey{}

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
		motel.Instrument{Name: "*_bytes", Scope: instrumentation.Scope{Name: MeterName}},
		motel.Stream{
			Aggregation: motel.AggregationExplicitBucketHistogram{
				Boundaries: []float64{1024, 2048, 4096, 16384, 65536, 262144, 1048576, 4194304, 16777216, 67108864, 268435456, 1073741824, 4294967296},
			},
		},
	)),
	motel.WithView(motel.NewView(
		motel.Instrument{Name: "*_request_latency", Scope: instrumentation.Scope{Name: MeterName}},
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

// AttrEvent creates an attribute that records the name of an event
func AttrEvent(val string) attribute.KeyValue {
	return attribute.String("event", val)
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

// StartSpan creates a span and a [context.Context] containing the newly-created span.
func StartSpan(ctx context.Context, name string) (context.Context, trace.Span) {
	return otel.Tracer(TracerName).Start(ctx, name)
}
