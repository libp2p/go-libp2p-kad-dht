package coord

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPooledQueryConfigValidate(t *testing.T) {
	t.Run("default is valid", func(t *testing.T) {
		cfg := DefaultPooledQueryConfig()

		require.NoError(t, cfg.Validate())
	})

	t.Run("clock is not nil", func(t *testing.T) {
		cfg := DefaultPooledQueryConfig()

		cfg.Clock = nil
		require.Error(t, cfg.Validate())
	})

	t.Run("logger not nil", func(t *testing.T) {
		cfg := DefaultPooledQueryConfig()
		cfg.Logger = nil
		require.Error(t, cfg.Validate())
	})

	t.Run("tracer not nil", func(t *testing.T) {
		cfg := DefaultPooledQueryConfig()
		cfg.Tracer = nil
		require.Error(t, cfg.Validate())
	})

	t.Run("query concurrency positive", func(t *testing.T) {
		cfg := DefaultPooledQueryConfig()

		cfg.Concurrency = 0
		require.Error(t, cfg.Validate())
		cfg.Concurrency = -1
		require.Error(t, cfg.Validate())
	})

	t.Run("query timeout positive", func(t *testing.T) {
		cfg := DefaultPooledQueryConfig()

		cfg.Timeout = 0
		require.Error(t, cfg.Validate())
		cfg.Timeout = -1
		require.Error(t, cfg.Validate())
	})

	t.Run("request concurrency positive", func(t *testing.T) {
		cfg := DefaultPooledQueryConfig()

		cfg.RequestConcurrency = 0
		require.Error(t, cfg.Validate())
		cfg.RequestConcurrency = -1
		require.Error(t, cfg.Validate())
	})

	t.Run("request timeout positive", func(t *testing.T) {
		cfg := DefaultPooledQueryConfig()

		cfg.RequestTimeout = 0
		require.Error(t, cfg.Validate())
		cfg.RequestTimeout = -1
		require.Error(t, cfg.Validate())
	})
}
