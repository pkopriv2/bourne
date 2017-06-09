package krypto

import (
	"crypto/rand"
	"fmt"
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
		if !assert.Nil(t, err) {
			return
		}

		sig, err := key.Sign(rand.Reader, SHA256, msg)
		if !assert.Nil(t, err) {
			return
		}
		assert.Nil(t, sig.Verify(key.Public(), msg))
	})

	t.Run("EncryptDecrypt", func(t *testing.T) {
		ciphertext, err := key.Public().Encrypt(rand.Reader, SHA256, []byte("msg"))
		if !assert.Nil(t, err) {
			return
		}

		dec, err := key.Decrypt(rand.Reader, SHA256, ciphertext)
		if !assert.Nil(t, err) {
			return
		}
		assert.Equal(t, []byte("msg"), dec)
	})

	t.Run("Private_EncodeDecode", func(t *testing.T) {
		ciphertext, err := key.Public().Encrypt(rand.Reader, SHA256, []byte("msg"))
		if !assert.Nil(t, err) {
			return
		}

		encoded, err := Json.Encode(key)
		if !assert.Nil(t, err) {
			return
		}

		fmt.Println(string(encoded.Body))

		priv, err := key.Algorithm().InitPriv()
		if !assert.Nil(t, err) {
			return
		}

		if !assert.Nil(t, Json.Decode(encoded, priv)) {
			return
		}

		dec, err := priv.Decrypt(rand.Reader, SHA256, ciphertext)
		if !assert.Nil(t, err) {
			return
		}
		assert.Equal(t, []byte("msg"), dec)
	})

	t.Run("PublicKey_EncodeDecode", func(t *testing.T) {
		msg := []byte("msg")

		sig, err := key.Sign(rand.Reader, SHA256, msg)
		if !assert.Nil(t, err) {
			return
		}

		encoded, err := Gob.Encode(key.Public())
		if !assert.Nil(t, err) {
			return
		}

		pub, err := key.Algorithm().InitPub()
		if !assert.Nil(t, err) {
			return
		}

		if !assert.Nil(t, Gob.Decode(encoded, pub)) {
			return
		}

		assert.Nil(t, key.Public().Verify(sig.Hash, msg, sig.Data))
		assert.Nil(t, pub.Verify(sig.Hash, msg, sig.Data))
	})
}
