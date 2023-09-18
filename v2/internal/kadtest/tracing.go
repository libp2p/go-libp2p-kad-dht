package kadtest

import (
	"context"
	"flag"
	"fmt"
	"testing"

	"github.com/libp2p/go-libp2p-kad-dht/v2/tele"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"go.opentelemetry.io/otel/trace"
)

var (
	tracing     = flag.Bool("tracing", false, "Enable or disable tracing")
	tracingHost = flag.String("tracinghost", "127.0.0.1", "Hostname of tracing collector endpoint")
	tracingPort = flag.Int("tracingport", 14268, "Port number of tracing collector endpoint")
)

// MaybeTrace returns a context containing a new root span named after the test. It creates an new
// tracing provider and installs it as the global provider, restoring the previous provider at the
// end of the test. This function cannot be called from tests that are run in parallel.
func MaybeTrace(t *testing.T, ctx context.Context) (context.Context, trace.TracerProvider) {
	if !*tracing {
		return ctx, otel.GetTracerProvider()
	}

	tp := JaegerTracerProvider(t)
	t.Logf("Tracing enabled and exporting to %s:%d", *tracingHost, *tracingPort)

	ctx, span := tp.Tracer("kadtest").Start(ctx, t.Name(), trace.WithNewRoot())
	t.Cleanup(func() {
		span.End()
	})

	return ctx, tp
}

// JaegerTracerProvider creates a tracer provider that exports traces to a Jaeger instance running
// on localhost on port 14268
func JaegerTracerProvider(t *testing.T) trace.TracerProvider {
	t.Helper()

	endpoint := fmt.Sprintf("http://%s:%d/api/traces", *tracingHost, *tracingPort)
	exp, err := jaeger.New(jaeger.WithCollectorEndpoint(jaeger.WithEndpoint(endpoint)))
	if err != nil {
		t.Fatalf("failed to create jaeger exporter: %v", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String(tele.TracerName),
			semconv.DeploymentEnvironmentKey.String("testing"),
		)),
	)

	t.Cleanup(func() {
		tp.Shutdown(context.Background())
	})

	return tp
}
