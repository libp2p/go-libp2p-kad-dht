package dht

import (
	"fmt"
	"strconv"
	"strings"
	"testing"

	record "github.com/libp2p/go-libp2p-record"
	"github.com/stretchr/testify/assert"

	"github.com/libp2p/go-libp2p-kad-dht/v2/internal/kadtest"
)

// testValidator is a validator that considers all values valid that have a
// "valid-" prefix. Then the suffix will determine which value is better. For
// example, "valid-2" is better than "valid-1".
type testValidator struct{}

var _ record.Validator = (*testValidator)(nil)

func (t testValidator) Validate(key string, value []byte) error {
	if strings.HasPrefix(string(value), "valid-") {
		return nil
	}
	return fmt.Errorf("invalid value")
}

func (t testValidator) Select(key string, values [][]byte) (int, error) {
	idx := -1
	best := -1
	for i, val := range values {
		if !strings.HasPrefix(string(val), "valid-") {
			continue
		}
		newBest, err := strconv.Atoi(string(val)[6:])
		if err != nil {
			continue
		}
		if newBest > best {
			idx = i
			best = newBest
		}
	}

	if idx == -1 {
		return idx, fmt.Errorf("no valid value")
	}

	return idx, nil
}

func TestRecordBackend_Validate(t *testing.T) {
	ctx := kadtest.CtxShort(t)

	b := &RecordBackend{
		namespace: "test",
		validator: &testValidator{},
	}

	t.Run("no values", func(t *testing.T) {
		idx, err := b.Validate(ctx, "some-key")
		assert.Error(t, err)
		assert.Equal(t, -1, idx)
	})

	t.Run("nil value", func(t *testing.T) {
		idx, err := b.Validate(ctx, "some-key", nil)
		assert.Error(t, err)
		assert.Equal(t, -1, idx)
	})

	t.Run("nil values", func(t *testing.T) {
		idx, err := b.Validate(ctx, "some-key", nil, nil)
		assert.Error(t, err)
		assert.Equal(t, -1, idx)
	})

	t.Run("single valid value", func(t *testing.T) {
		idx, err := b.Validate(ctx, "some-key", []byte("valid-0"))
		assert.NoError(t, err)
		assert.Equal(t, 0, idx)
	})

	t.Run("increasing better values", func(t *testing.T) {
		idx, err := b.Validate(ctx, "some-key", []byte("valid-0"), []byte("valid-1"), []byte("valid-2"))
		assert.NoError(t, err)
		assert.Equal(t, 2, idx)
	})

	t.Run("mixed better values", func(t *testing.T) {
		idx, err := b.Validate(ctx, "some-key", []byte("valid-0"), []byte("valid-2"), []byte("valid-1"))
		assert.NoError(t, err)
		assert.Equal(t, 1, idx)
	})

	t.Run("mixed invalid values", func(t *testing.T) {
		idx, err := b.Validate(ctx, "some-key", []byte("valid-0"), []byte("invalid"), []byte("valid-2"), []byte("invalid"))
		assert.NoError(t, err)
		assert.Equal(t, 2, idx)
	})

	t.Run("only invalid values", func(t *testing.T) {
		idx, err := b.Validate(ctx, "some-key", []byte("invalid"), nil)
		assert.Error(t, err)
		assert.Equal(t, -1, idx)
	})
}
