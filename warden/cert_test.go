package warden

import (
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
		fmt, err := cert.Format()
		assert.Nil(t, err)

		for i := 0; i < 10; i++ {
			now, err := cert.Format()
			assert.Nil(t, err)
			assert.Equal(t, fmt, now)
		}
	})
}
