package warden

import (
	"crypto/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRsaKey(t *testing.T) {
	key, err := GenRsaKey(rand.Reader, 1024)
	assert.Nil(t, err)

	t.Run("Id", func(t *testing.T) {
		id := key.Public().Id()
		for i := 0; i < 100; i++ {
			// fmt.Println("KEY: ", id)
			assert.Equal(t, id, key.Public().Id())
		}
	})

	t.Run("SignVerify", func(t *testing.T) {
		msg, err := genRandomBytes(rand.Reader, 1024)
		assert.Nil(t, err)

		sig, err := key.Sign(rand.Reader, SHA256, msg)
		assert.Nil(t, err)
		assert.Nil(t, sig.Verify(key.Public(), msg))
	})

	t.Run("EncryptDecrypt", func(t *testing.T) {
		ciphertext, err := key.Public().Encrypt(rand.Reader, SHA256, []byte("msg"))
		assert.Nil(t, err)

		dec, err := key.Decrypt(rand.Reader, SHA256, ciphertext)
		assert.Nil(t, err)
		assert.Equal(t, []byte("msg"), dec)
	})

	t.Run("PrivateKey_WriteParse", func(t *testing.T) {
		priv, err := key.Algorithm().parsePrivateKey(key.format())
		assert.Nil(t, err)

		msg := []byte("msg")

		privSig, err := priv.Sign(rand.Reader, SHA256, msg)
		assert.Nil(t, err)
		assert.Nil(t, key.Public().Verify(privSig.Hash, msg, privSig.Data))

		keySig, err := key.Sign(rand.Reader, SHA256, msg)
		assert.Nil(t, err)
		assert.Nil(t, priv.Public().Verify(keySig.Hash, msg, keySig.Data))
	})

	t.Run("PublicKey_WriteParse", func(t *testing.T) {
		pub, err := key.Algorithm().parsePublicKey(key.Public().format())
		assert.Nil(t, err)

		msg := []byte("msg")

		sig, err := key.Sign(rand.Reader, SHA256, msg)
		assert.Nil(t, err)
		assert.Nil(t, key.Public().Verify(sig.Hash, msg, sig.Data))
		assert.Nil(t, pub.Verify(sig.Hash, msg, sig.Data))
	})
}
