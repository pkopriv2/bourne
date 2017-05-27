package warden

import (
	"crypto/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMember(t *testing.T) {
	owner, err := GenRsaKey(rand.Reader, 1024)
	if err != nil {
		t.FailNow()
		return
	}

	t.Run("ExtractCreds_NoCredential", func(t *testing.T) {
		_, e := extractCreds(func(pad KeyPad) error {
			return nil
		})
		assert.NotNil(t, e)
	})

	t.Run("NewMember_WithSigner", func(t *testing.T) {
		login := func(pad KeyPad) error {
			return pad.BySignature(owner, SHA256)
		}

		creds, e := extractCreds(login)
		assert.Nil(t, e)

		sub, auth, e := newMember(rand.Reader, creds)
		assert.Nil(t, e)

		secret, e := sub.secret(rand.Reader, auth, login)
		assert.Nil(t, e)

		_, e = sub.encryptionSeed(secret)
		assert.Nil(t, e)

		_, e = sub.signingKey(secret)
		assert.Nil(t, e)

		_, e = sub.invitationKey(secret)
		assert.Nil(t, e)
	})

	t.Run("NewMember_WithPassword", func(t *testing.T) {
		login := func(pad KeyPad) error {
			return pad.ByPassword([]byte("user"), []byte("pass"))
		}

		creds, e := extractCreds(login)
		assert.Nil(t, e)


		sub, auth, e := newMember(rand.Reader, creds)
		assert.Nil(t, e)

		secret, e := sub.secret(rand.Reader, auth, login)
		assert.Nil(t, e)

		_, e = sub.encryptionSeed(secret)
		assert.Nil(t, e)

		_, e = sub.signingKey(secret)
		assert.Nil(t, e)

		_, e = sub.invitationKey(secret)
		assert.Nil(t, e)
	})
}
