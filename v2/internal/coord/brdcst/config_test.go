package brdcst

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPoolConfig_Validate(t *testing.T) {
	t.Run("nil pool config", func(t *testing.T) {
		cfg := DefaultPoolConfig()
		cfg.pCfg = nil
		assert.Error(t, cfg.Validate())
	})
}
