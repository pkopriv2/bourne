package warden

import (
	"crypto/rand"
	"testing"
	"time"

	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
)

func TestMember(t *testing.T) {
	owner, err := GenRsaKey(rand.Reader, 1024)
	if err != nil {
		t.FailNow()
		return
	}

	t.Run("ExtractCreds_NoCredential", func(t *testing.T) {
		_, e := extractCreds(func() credential {
			return nil
		})
		assert.NotNil(t, e)
	})

	t.Run("NewMember_WithSigner", func(t *testing.T) {
		login := func() credential {
			return &signingCred{[]byte(owner.Public().Id()), owner, SHA256, 5 * time.Minute}
		}

		creds := login()
		defer creds.Destroy()

		sub, auth, e := newMember(rand.Reader, uuid.NewV1(), uuid.NewV1(), creds)
		assert.Nil(t, e)

		secret, e := sub.secret(auth, login)
		assert.Nil(t, e)

		_, e = sub.encryptionSeed(secret)
		assert.Nil(t, e)

		_, e = sub.signingKey(secret)
		assert.Nil(t, e)

		_, e = sub.invitationKey(secret)
		assert.Nil(t, e)
	})

	t.Run("NewMember_WithPassword", func(t *testing.T) {
		login := func() credential {
			return &passCred{[]byte(owner.Public().Id()), []byte("pass")}
		}

		creds := login()
		defer creds.Destroy()

		sub, auth, e := newMember(rand.Reader, uuid.NewV1(), uuid.NewV1(), creds)
		assert.Nil(t, e)

		secret, e := sub.secret(auth, login)
		assert.Nil(t, e)

		_, e = sub.encryptionSeed(secret)
		assert.Nil(t, e)

		_, e = sub.signingKey(secret)
		assert.Nil(t, e)

		_, e = sub.invitationKey(secret)
		assert.Nil(t, e)
	})
}
