package warden

import (
	"crypto/rand"
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOracleKey_GenerateAndAccess(t *testing.T) {
	line := line{big.NewInt(1), big.NewInt(0)}

	key, err := generateOracleKey(rand.Reader, "id", "alias", line, []byte("pass"), buildOracleOptions())
	assert.Nil(t, err)

	pt, err := key.access([]byte("pass"))
	assert.Nil(t, err)
	assert.True(t, line.Contains(pt))
}

func TestOracle_GenerateAndAccess(t *testing.T) {
	oracle, line, err := generateOracle(rand.Reader, "id")
	assert.Nil(t, err)

	key, err := generateOracleKey(rand.Reader, "id", "alias", line, []byte("pass"), oracle.opts)
	assert.Nil(t, err)

	act, err := oracle.Unlock(key, []byte("pass"))
	assert.Nil(t, err)
	assert.Equal(t, line, act)
}
