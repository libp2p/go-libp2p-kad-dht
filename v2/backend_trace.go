package dht

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// tracedBackend wraps a [Backend] in calls to open telemetry tracing
// directives. In [New] all backends configured in [Config] or automatically
// configured if none are given will be wrapped with this tracedBackend.
type tracedBackend struct {
	namespace string       // the namespace the backend operates in. Used as a tracing attribute.
	backend   Backend      // the [Backend] to be traced
	tracer    trace.Tracer // the tracer to be used
}

var _ Backend = (*tracedBackend)(nil)

func traceWrapBackend(namespace string, backend Backend, tracer trace.Tracer) Backend {
	return &tracedBackend{
		namespace: namespace,
		backend:   backend,
		tracer:    tracer,
	}
}

// Store implements the [Backend] interface, forwards the call to the wrapped
// backend and manages the trace span.
func (t *tracedBackend) Store(ctx context.Context, key string, value any) (any, error) {
	ctx, span := t.tracer.Start(ctx, "Store", t.traceAttributes(key))
	defer span.End()

	result, err := t.backend.Store(ctx, key, value)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}

	return result, err
}

// Fetch implements the [Backend] interface, forwards the call to the wrapped
// backend and manages the trace span.
func (t *tracedBackend) Fetch(ctx context.Context, key string) (any, error) {
	ctx, span := t.tracer.Start(ctx, "Fetch", t.traceAttributes(key))
	defer span.End()

	result, err := t.backend.Fetch(ctx, key)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}

	return result, err
}

func (t *tracedBackend) Validate(ctx context.Context, key string, values ...any) (int, error) {
	ctx, span := t.tracer.Start(ctx, "Validate", t.traceAttributes(key))
	defer span.End()

	idx, err := t.backend.Validate(ctx, key, values...)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	} else {
		span.SetAttributes(attribute.Int("idx", idx))
	}

	return idx, err
}

// traceAttributes is a helper to build the trace attributes.
func (t *tracedBackend) traceAttributes(key string) trace.SpanStartEventOption {
	return trace.WithAttributes(attribute.String("namespace", t.namespace), attribute.String("key", key))
}
