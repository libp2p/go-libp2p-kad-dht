package metrics_test

import (
	"context"
	"testing"

	"github.com/libp2p/go-libp2p-kad-dht/internal/metrics"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/attribute"
)

func TestContext(t *testing.T) {
	t.Run("empty value in context", func(t *testing.T) {
		ctx := context.Background()
		assert.Empty(t, metrics.AttributesFromContext(ctx))
	})

	t.Run("set/get value in context", func(t *testing.T) {
		attrs := []attribute.KeyValue{
			attribute.Key("key1").String("val1"),
			attribute.Key("key2").Int(12345),
			attribute.Key("key3").Bool(true),
		}
		ctx := context.Background()

		res := metrics.AttributesFromContext(ctx)
		assert.Equal(t, 0, res.Len())

		ctx = metrics.ContextWithAttributes(ctx, attrs...)

		res = metrics.AttributesFromContext(ctx)
		exp := attribute.NewSet(attrs...)
		assert.Equal(t, len(attrs), res.Len())
		assert.True(t, (&res).Equals(&exp))
	})

	t.Run("multiple sets in context", func(t *testing.T) {
		attrs := []attribute.KeyValue{
			attribute.Key("key1").String("val1"),
			attribute.Key("key2").Int(12345),
			attribute.Key("key3").Bool(true),
		}
		ctx := context.Background()

		for _, attr := range attrs {
			ctx = metrics.ContextWithAttributes(ctx, attr)
		}

		res := metrics.AttributesFromContext(ctx)
		exp := attribute.NewSet(attrs[len(attrs)-1])
		assert.Equal(t, 1, res.Len())
		assert.True(t, (&res).Equals(&exp))
	})
}
