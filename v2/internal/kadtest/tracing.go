package kadtest

import (
	"context"
	"fmt"
	"testing"

	"go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/sdk/trace"
)

// JaegerTracerProvider creates a tracer provider that exports traces to a Jaeger instance running
// on localhost on port 14268
func JaegerTracerProvider(t *testing.T) *trace.TracerProvider {
	t.Helper()

	traceHost := "127.0.0.1"
	tracePort := 14268

	endpoint := fmt.Sprintf("http://%s:%d/api/traces", traceHost, tracePort)
	exp, err := jaeger.New(jaeger.WithCollectorEndpoint(jaeger.WithEndpoint(endpoint)))
	if err != nil {
		t.Fatalf("failed to create jaeger exporter: %v", err)
	}

	tp := trace.NewTracerProvider(trace.WithBatcher(exp))

	t.Cleanup(func() {
		tp.Shutdown(context.Background())
	})

	return tp
}
