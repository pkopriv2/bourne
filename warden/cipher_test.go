package warden

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEncrypt_KeyTooSmall(t *testing.T) {
	_, err := Encrypt(AES_128_GCM, []byte{}, []byte("msg"))
	assert.NotNil(t, err)
}

func TestEncrypt_KeyTooLarge(t *testing.T) {
	_, err := Encrypt(AES_128_GCM, make([]byte, 24), []byte("msg"))
	assert.NotNil(t, err)
}

func TestEncrypt_AES_128_GCM(t *testing.T) {
	key, err := generateRandomBytes(BYTES_128_BITS)
	assert.Nil(t, err)

	ct, err := Encrypt(AES_128_GCM, key, []byte("msg"))
	assert.Nil(t, err)
	assert.NotNil(t, ct)
	assert.NotEmpty(t, ct)

	raw, err := ct.Decrypt(key)
	assert.Nil(t, err)
	assert.Equal(t, []byte("msg"), raw)
}
