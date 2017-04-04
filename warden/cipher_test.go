package warden

import (
	"crypto/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEncrypt_KeyTooSmall(t *testing.T) {
	_, err := symmetricEncrypt(rand.Reader, AES_128_GCM, []byte{}, []byte("msg"))
	assert.NotNil(t, err)
}

func TestEncrypt_KeyTooLarge(t *testing.T) {
	_, err := symmetricEncrypt(rand.Reader, AES_128_GCM, make([]byte, 24), []byte("msg"))
	assert.NotNil(t, err)
}

func TestEncrypt_AES_128_GCM(t *testing.T) {
	key, err := generateRandomBytes(rand.Reader, bits_128)
	assert.Nil(t, err)

	ct, err := symmetricEncrypt(rand.Reader, AES_128_GCM, key, []byte("msg"))
	assert.Nil(t, err)
	assert.NotNil(t, ct)
	assert.NotEmpty(t, ct)

	raw, err := ct.Decrypt(key)
	assert.Nil(t, err)
	assert.Equal(t, Bytes([]byte("msg")), raw)
}

// func TestAsymmetricEncryp_Simple(t *testing.T) {
// msg := []byte("hello, world")
// key, err := rsa.GenerateKey(rand.Reader, 512)
// assert.Nil(t, err)
//
// enc, err := asymmetricEncrypt(rand.Reader, RSA_WITH_SHA1, AES_128_GCM, &key.PublicKey, msg)
// assert.Nil(t, err)
// assert.NotNil(t, enc)
//
// decrypted, err := enc.Decrypt(key)
// assert.Nil(t, err)
// assert.Equal(t, msg, decrypted)
// }
