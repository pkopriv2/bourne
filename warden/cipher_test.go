package warden

import (
	"crypto/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEncrypt(t *testing.T) {
	t.Run("KeyTooSmall", func(t *testing.T) {
		_, err := symmetricEncrypt(rand.Reader, Aes128Gcm, []byte{}, []byte("msg"))
		assert.NotNil(t, err)
	})

	t.Run("KeyTooLarge", func(t *testing.T) {
		_, err := symmetricEncrypt(rand.Reader, Aes128Gcm, make([]byte, 24), []byte("msg"))
		assert.NotNil(t, err)
	})

	key, err := genRandomBytes(rand.Reader, bits128)
	assert.Nil(t, err)

	ct, err := symmetricEncrypt(rand.Reader, Aes128Gcm, key, []byte("msg"))
	assert.Nil(t, err)
	assert.NotNil(t, ct)
	assert.NotEmpty(t, ct)

	raw, err := ct.Decrypt(key)
	assert.Nil(t, err)
	assert.Equal(t, cryptoBytes([]byte("msg")), raw)

}
