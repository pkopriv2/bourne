package warden

import (
	"crypto/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSubscriber(t *testing.T) {
	owner, err := GenRsaKey(rand.Reader, 1024)
	if err != nil {
		t.FailNow()
		return
	}

	t.Run("NewSubscriber_NoCreds", func(t *testing.T) {
		creds, e := enterCreds(func(pad KeyPad) error {
			return nil
		})

		_, _, e = newMember(rand.Reader, creds)
		assert.NotNil(t, e)
	})

	t.Run("NewSubscriber_WithSigner", func(t *testing.T) {
		creds, e := enterCreds(func(pad KeyPad) error {
			return pad.BySignature(owner)
		})

		sub, auth, e := newMember(rand.Reader, creds)
		assert.Nil(t, e)

		secret, e := sub.secret(auth, func(pad KeyPad) error {
			return pad.BySignature(owner)
		})

		_, e = sub.encryptionSeed(secret)
		assert.Nil(t, e)

		_, e = sub.signingKey(secret)
		assert.Nil(t, e)

		_, e = sub.invitationKey(secret)
		assert.Nil(t, e)
	})
}
