package dht

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

func startSpan(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	return otel.Tracer("go-libp2p-kad-dht").Start(ctx, fmt.Sprintf("Libp2pDHT.%s", name), opts...)
}
