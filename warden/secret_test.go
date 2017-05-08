package warden

import (
	"crypto/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOracle(t *testing.T) {
	o, l, e := genSharedSecret(rand.Reader, buildSecretOptions())
	assert.Nil(t, e)

	t.Run("GenerateAndUnlock", func(t *testing.T) {
		k, e := genPrivateShard(rand.Reader, l, []byte("pass"), o.Opts)
		assert.Nil(t, e)

		sh, e := k.Decrypt([]byte("pass"))
		assert.Nil(t, e)

		act, e := o.Pub.Derive(sh)
		assert.Nil(t, e)
		assert.Equal(t, l, act)
	})
}
