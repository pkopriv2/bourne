package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfig_OptionalInt_empty(t *testing.T) {
	config := NewEmptyConfig()
	assert.Equal(t, 1, config.OptionalInt("key", 1))
}

