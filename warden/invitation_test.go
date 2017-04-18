package warden

import (
	"crypto/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInvitation(t *testing.T) {
	domainKey, err := GenRsaKey(rand.Reader, 1024)
	issuerKey, err := GenRsaKey(rand.Reader, 1024)
	trusteeKey, err := GenRsaKey(rand.Reader, 1024)

	oracle, line, err := genOracle(rand.Reader)
	assert.Nil(t, err)

	inv, err := generateInvitation(rand.Reader, line, domainKey, issuerKey, trusteeKey.Public())
	assert.Nil(t, err)

	t.Run("Verify_BadDomainKey", func(t *testing.T) {
		assert.NotNil(t, verifyInvitation(inv, issuerKey.Public(), issuerKey.Public()))
	})

	t.Run("Verify_BadIssuerKey", func(t *testing.T) {
		assert.NotNil(t, verifyInvitation(inv, domainKey.Public(), domainKey.Public()))
	})

	t.Run("Verify", func(t *testing.T) {
		assert.Nil(t, verifyInvitation(inv, domainKey.Public(), issuerKey.Public()))
	})

	t.Run("Accept", func(t *testing.T) {
		oracleKey, err := acceptInvitation(rand.Reader, inv, oracle, trusteeKey, []byte("pass"), buildOracleOptions())
		assert.Nil(t, err)

		pt, err := oracleKey.access([]byte("pass"))
		assert.Nil(t, err)
		assert.NotNil(t, pt)
	})
}
