package tele

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
)

func TestWithAttributes(t *testing.T) {
	ctx := context.Background()

	set := FromContext(ctx)
	assert.Equal(t, 0, set.Len())

	ctx = WithAttributes(ctx, attribute.Int("A", 1))
	ctx = WithAttributes(ctx, attribute.Int("B", 1))
	ctx = WithAttributes(ctx, attribute.Int("A", 1))
	ctx = WithAttributes(ctx, attribute.Int("B", 2))
	ctx = WithAttributes(ctx, attribute.Int("C", 1))

	set = FromContext(ctx, attribute.Int("A", 2))

	val, found := set.Value("A")
	require.True(t, found)
	assert.EqualValues(t, 2, val.AsInt64())

	val, found = set.Value("B")
	require.True(t, found)
	assert.EqualValues(t, 2, val.AsInt64())

	val, found = set.Value("C")
	require.True(t, found)
	assert.EqualValues(t, 1, val.AsInt64())

	ctx = context.WithValue(ctx, attrsCtxKey, "not an attribute set")
	set = FromContext(ctx)
	assert.Equal(t, 0, set.Len())
}
