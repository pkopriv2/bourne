package warden

import (
	"crypto/rand"
	"fmt"
	"testing"
	"time"

	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
)

func TestCert(t *testing.T) {
	cert := newCertificate(uuid.NewV1(), uuid.NewV1(), uuid.NewV1(), Verify, 30*time.Second)

	t.Run("String", func(t *testing.T) {
		fmt.Println(fmt.Sprintf("%v", cert.String()))
	})

	t.Run("Format", func(t *testing.T) {
		fmt, err := cert.Format()
		assert.Nil(t, err)

		for i := 0; i < 10; i++ {
			cur, err := cert.Format()
			assert.Nil(t, err)
			assert.Equal(t, fmt, cur)
		}
	})

	key, err := GenRsaKey(rand.Reader, 512)
	assert.Nil(t, err)

	t.Run("Sign", func(t *testing.T) {
		sig, err := sign(rand.Reader, cert, key, SHA256)
		assert.Nil(t, err)
		assert.Nil(t, cert.Verify(key.Public(), sig))
	})
}
