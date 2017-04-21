package warden

import (
	"crypto/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOracle(t *testing.T) {
	o, l, e := genOracle(rand.Reader, buildOracleOptions())
	assert.Nil(t, e)

	t.Run("GenerateAndUnlock", func(t *testing.T) {
		k, e := genOracleKey(rand.Reader, l, []byte("pass"), o.Opts)
		assert.Nil(t, e)

		act, e := o.Unlock(k, []byte("pass"))
		assert.Nil(t, e)
		assert.Equal(t, l, act)
	})
}
