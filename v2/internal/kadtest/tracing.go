package kadtest

import (
	"context"
	"flag"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/libp2p/go-libp2p-kad-dht/v2/tele"
)

var (
	tracing     = flag.Bool("tracing", false, "Enable or disable tracing")
	tracingHost = flag.String("tracinghost", "127.0.0.1", "Hostname of OTLP tracing collector endpoint")
	tracingPort = flag.Int("tracingport", 4317, "Port number of OTLP gRPC tracing collector endpoint")
)

// MaybeTrace returns a context containing a new root span named after the test.
// It creates a new tracing provider and installs it as the global provider,
// restoring the previous provider at the end of the test. This function cannot
// be called from tests that are run in parallel.
//
// To activate test tracing pass the `-tracing` flag to the test command.
// Assuming you chose the defaults above, run the following to collect traces:
//
//	docker run --rm --name jaeger -p 16686:16686 -p 4317:4317 jaegertracing/all-in-one:1.49
//
// Then navigate to localhost:16686 and inspect the traces.
func MaybeTrace(t testing.TB, ctx context.Context) (context.Context, trace.TracerProvider) {
	if !*tracing {
		return ctx, otel.GetTracerProvider()
	}

	tp := OtelTracerProvider(ctx, t)
	t.Logf("Tracing enabled and exporting to %s:%d", *tracingHost, *tracingPort)

	ctx, span := tp.Tracer("kadtest").Start(ctx, t.Name(), trace.WithNewRoot())
	t.Cleanup(func() {
		span.End()
	})

	return ctx, tp
}

// OtelTracerProvider creates a tracer provider that exports traces to, e.g., a
// Jaeger instance running on localhost on port 14268
func OtelTracerProvider(ctx context.Context, t testing.TB) trace.TracerProvider {
	t.Helper()

	exp, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(fmt.Sprintf("%s:%d", *tracingHost, *tracingPort)),
		otlptracegrpc.WithInsecure(),
	)
	require.NoError(t, err, "failed to create otel exporter")

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String(tele.TracerName),
			semconv.DeploymentEnvironmentKey.String("testing"),
		)),
	)

	t.Cleanup(func() {
		if err = tp.ForceFlush(ctx); err != nil {
			t.Log("failed to shut down trace provider")
		}

		if err = tp.Shutdown(ctx); err != nil {
			t.Log("failed to shut down trace provider")
		}
	})

	return tp
}
