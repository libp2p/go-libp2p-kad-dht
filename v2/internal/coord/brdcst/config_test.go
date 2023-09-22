package brdcst

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfigPool_Validate(t *testing.T) {
	t.Run("default is valid", func(t *testing.T) {
		cfg := DefaultConfigPool()
		assert.NoError(t, cfg.Validate())
	})

	t.Run("nil pool config", func(t *testing.T) {
		cfg := DefaultConfigPool()
		cfg.pCfg = nil
		assert.Error(t, cfg.Validate())
	})
}

func TestConfigFollowUp_Validate(t *testing.T) {
	t.Run("default is valid", func(t *testing.T) {
		cfg := DefaultConfigFollowUp()
		assert.NoError(t, cfg.Validate())
	})
}

func TestConfigOptimistic_Validate(t *testing.T) {
	t.Run("default is valid", func(t *testing.T) {
		cfg := DefaultConfigOptimistic()
		assert.NoError(t, cfg.Validate())
	})
}

func TestConfig_interface_conformance(t *testing.T) {
	configs := []Config{
		&ConfigFollowUp{},
		&ConfigOptimistic{},
	}
	for _, c := range configs {
		c.broadcastConfig() // drives test coverage
	}
}
