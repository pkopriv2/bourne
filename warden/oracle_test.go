package warden

import (
	"crypto/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOracle(t *testing.T) {
	o, l, e := generateOracle(rand.Reader, "id")
	assert.Nil(t, e)

	t.Run("GenerateAndUnlock", func(t *testing.T) {
		k, e := generateOracleKey(rand.Reader, o.Id, "id", l, []byte("pass"), o.opts)
		assert.Nil(t, e)

		act, e := o.Unlock(k, []byte("pass"))
		assert.Nil(t, e)
		assert.Equal(t, l, act)
	})
}
