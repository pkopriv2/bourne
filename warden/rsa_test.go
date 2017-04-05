package warden

import (
	"crypto/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRsaKey(t *testing.T) {
	key, err := GenRsaKey(rand.Reader, 1024)
	assert.Nil(t, err)

	t.Run("SignVerify", func(t *testing.T) {
		sig, err := key.Sign(rand.Reader, SHA256, []byte("msg"))
		assert.Nil(t, err)
		assert.Nil(t, sig.Verify(key.Public(), []byte("msg")))
	})

	t.Run("EncryptDecrypt", func(t *testing.T) {
		ciphertext, err := key.Public().Encrypt(rand.Reader, SHA256, []byte("msg"))
		assert.Nil(t, err)

		dec, err := key.Decrypt(rand.Reader, SHA256, ciphertext)
		assert.Nil(t, err)
		assert.Equal(t, []byte("msg"), dec)
	})

	t.Run("PrivateKey_WriteParse", func(t *testing.T) {
		priv, err := key.Algorithm().ParsePrivateKey(key.Bytes())
		assert.Nil(t, err)
		priv.(*rsaPrivateKey).raw.Precompute()
		assert.Equal(t, key, priv)
	})

	t.Run("PublicKey_WriteParse", func(t *testing.T) {
		pub, err := key.Algorithm().ParsePublicKey(key.Public().Bytes())
		assert.Nil(t, err)
		assert.Equal(t, key.Public(), pub)
	})
}
