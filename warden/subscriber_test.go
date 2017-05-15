package warden

import (
	"crypto/rand"
	"testing"

	"github.com/pkopriv2/bourne/common"
	"github.com/stretchr/testify/assert"
)

func TestSubscriber(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	log := ctx.Logger()

	owner, err := GenRsaKey(rand.Reader, 1024)
	if err != nil {
		t.FailNow()
		return
	}

	t.Run("NewSubscriber_NoCreds", func(t *testing.T) {
		_, _, e := NewSubscriber(rand.Reader, func(pad KeyPad) error {
			return nil
		})
		assert.NotNil(t, e)
	})

	t.Run("NewSubscriber_WithSigner", func(t *testing.T) {
		sub, auth, e := NewSubscriber(rand.Reader, func(pad KeyPad) error {
			return pad.BySignature(owner)
		})
		assert.Nil(t, e)

		secret, e := sub.mySecret(auth, func(pad KeyPad) error {
			return pad.BySignature(owner)
		})

		_, e = sub.mySigningKey(secret)
		assert.Nil(t, e)

		_, e = sub.myInvitationKey(secret)
		assert.Nil(t, e)
	})

	log.Info("Hola")

	// line, e := sub.Oracle.Unlock(key.oracleKey, []byte("pass"))
	// assert.Nil(t, e)
	//
	// _, e = sub.Sign.Decrypt(line.Bytes())
	// assert.Nil(t, e)
	//
	// _, e = sub.Invite.Decrypt(line.Bytes())
	// assert.Nil(t, e)
}
