package krypto

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
		fmt, err := key.SigningFormat()
		if ! assert.Nil(t, err) {
			return
		}

		priv, err := key.Algorithm().parsePrivateKey(fmt)
		if ! assert.Nil(t, err) {
			return
		}

		msg := []byte("msg")

		privSig, err := priv.Sign(rand.Reader, SHA256, msg)
		assert.Nil(t, err)
		assert.Nil(t, key.Public().Verify(privSig.Hash, msg, privSig.Data))

		keySig, err := key.Sign(rand.Reader, SHA256, msg)
		assert.Nil(t, err)
		assert.Nil(t, priv.Public().Verify(keySig.Hash, msg, keySig.Data))
	})

	t.Run("PublicKey_WriteParse", func(t *testing.T) {
		fmt, err := key.SigningFormat()
		if ! assert.Nil(t, err) {
			return
		}

		pub, err := key.Algorithm().parsePublicKey(fmt)
		if ! assert.Nil(t, err) {
			return
		}

		msg := []byte("msg")

		sig, err := key.Sign(rand.Reader, SHA256, msg)
		if ! assert.Nil(t, err) {
			return
		}

		assert.Nil(t, key.Public().Verify(sig.Hash, msg, sig.Data))
		assert.Nil(t, pub.Verify(sig.Hash, msg, sig.Data))
	})
}
