package warden

import (
	"crypto/rand"
	"testing"

	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
)

func TestInvitation(t *testing.T) {
	domainKey, err := GenRsaKey(rand.Reader, 1024)
	issuerKey, err := GenRsaKey(rand.Reader, 1024)
	trusteeKey, err := GenRsaKey(rand.Reader, 1024)

	line, err := genSecret(rand.Reader, buildSecretOptions())
	// assert.Nil(t, err)

	cert := newCertificate(uuid.NewV1(), uuid.NewV1(), uuid.NewV1(), Own, OneHundredYears)

	inv, err := createInvitation(rand.Reader, line, cert, domainKey, issuerKey, trusteeKey.Public(), buildTrustOptions())
	assert.Nil(t, err)

	t.Run("Verify_BadDomainKey", func(t *testing.T) {
		assert.NotNil(t, inv.verify(issuerKey.Public(), issuerKey.Public()))
	})

	t.Run("Verify_BadIssuerKey", func(t *testing.T) {
		assert.NotNil(t, inv.verify(issuerKey.Public(), issuerKey.Public()))
	})

	// t.Run("Verify", func(t *testing.T) {
	// assert.Nil(t, inv.verify(issuerKey.Public(), issuerKey.Public()))
	// })

	// t.Run("Accept", func(t *testing.T) {
	// oracleKey, err := inv.accept(rand.Reader, oracle, trusteeKey, []byte("pass"), buildSecretOptions())
	// assert.Nil(t, err)
	//
	// pt, err := oracleKey.Extract([]byte("pass"))
	// assert.Nil(t, err)
	// assert.NotNil(t, pt)
	// })
}
