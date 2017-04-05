package warden

import (
	"crypto/rand"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCert(t *testing.T) {
	cert := newCertificate("domain", "issuer", "trustee", Verify, 30*time.Second)

	t.Run("String", func(t *testing.T) {
		fmt.Println(fmt.Sprintf("%v", cert.String()))
	})

	t.Run("Format", func(t *testing.T) {
		fmt := cert.Format()
		for i := 0; i < 10; i++ {
			assert.Equal(t, fmt, cert.Format())
		}
	})

	key, err  := GenRsaKey(rand.Reader, 512)
	assert.Nil(t, err)

	t.Run("Sign", func(t *testing.T) {
		sig, err := cert.Sign(rand.Reader, key, SHA256)
		assert.Nil(t, err)
		assert.Nil(t, cert.Verify(key.Public(), sig))
	})
}
