package dht

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	otel "go.opentelemetry.io/otel/trace"
)

type tracedBackend struct {
	namespace string
	backend   Backend
}

var _ Backend = (*tracedBackend)(nil)

func traceWrapBackend(namespace string, backend Backend) Backend {
	return &tracedBackend{
		namespace: namespace,
		backend:   backend,
	}
}

func (t tracedBackend) Store(ctx context.Context, key string, value any) (any, error) {
	ctx, span := tracer.Start(ctx, "Store", otel.WithAttributes(attribute.String("backend", t.namespace), attribute.String("key", key)))
	defer span.End()

	result, err := t.backend.Store(ctx, key, value)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}

	return result, err
}

func (t tracedBackend) Fetch(ctx context.Context, key string) (any, error) {
	ctx, span := tracer.Start(ctx, "Fetch", otel.WithAttributes(attribute.String("backend", t.namespace), attribute.String("key", key)))
	defer span.End()

	result, err := t.backend.Fetch(ctx, key)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}

	return result, err
}
