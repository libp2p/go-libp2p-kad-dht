package dht

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type tracedBackend struct {
	namespace string
	backend   Backend
	tracer    trace.Tracer
}

var _ Backend = (*tracedBackend)(nil)

func traceWrapBackend(namespace string, backend Backend, tracer trace.Tracer) Backend {
	return &tracedBackend{
		namespace: namespace,
		backend:   backend,
		tracer:    tracer,
	}
}

func (t tracedBackend) Store(ctx context.Context, key string, value any) (any, error) {
	ctx, span := t.tracer.Start(ctx, "Store", trace.WithAttributes(attribute.String("backend", t.namespace), attribute.String("key", key)))
	defer span.End()

	result, err := t.backend.Store(ctx, key, value)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}

	return result, err
}

func (t tracedBackend) Fetch(ctx context.Context, key string) (any, error) {
	ctx, span := t.tracer.Start(ctx, "Fetch", trace.WithAttributes(attribute.String("backend", t.namespace), attribute.String("key", key)))
	defer span.End()

	result, err := t.backend.Fetch(ctx, key)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}

	return result, err
}
