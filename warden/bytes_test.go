package warden

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLine00(t *testing.T) {
	line := line{big.NewInt(0), big.NewInt(0)}
	assert.Equal(t, big.NewInt(0), line.Height(big.NewInt(0)))
	assert.Equal(t, point{big.NewInt(0), big.NewInt(0)}, line.Point(big.NewInt(0)))

	parsed, err := parseLineBytes(line.Bytes())
	assert.Nil(t, err)
	assert.Equal(t, line, parsed)
}
