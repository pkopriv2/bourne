package warden

import (
	"crypto/rand"
	"testing"
	"time"

	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/micro"
	"github.com/pkopriv2/bourne/net"
	"github.com/pkopriv2/bourne/stash"
	"github.com/stretchr/testify/assert"
)

func TestSession(t *testing.T) {
	ctx := common.NewEmptyContext()
	defer ctx.Close()

	db, err := stash.OpenTransient(ctx)
	if err != nil {
		t.FailNow()
		return
	}

	store, e := newBoltStorage(db)
	if e != nil {
		t.FailNow()
		return
	}

	serverKey, e := GenRsaKey(rand.Reader, 1024)
	if e != nil {
		t.FailNow()
		return
	}

	l, e := net.NewTcpNetwork().Listen(30*time.Second, ":0")
	if e != nil {
		t.FailNow()
		return
	}

	s, e := newServer(ctx, store, l, rand.Reader, serverKey, 5)
	if e != nil {
		t.FailNow()
		return
	}

	timer := ctx.Timer(30 * time.Second)
	defer timer.Close()

	cl, e := s.Client(micro.Gob)
	assert.Nil(t, e)

	addr := cl.Remote().String()

	t.Run("Subscribe", func(t *testing.T) {
		owner, err := GenRsaKey(rand.Reader, 1024)
		if err != nil {
			t.FailNow()
			return
		}

		session, err := Subscribe(ctx, addr, func(pad KeyPad) error {
			return pad.BySignature(owner)
		})
		assert.Nil(t, err)

		trust, err := session.NewSecureTrust(timer.Closed(), "test")
		assert.Nil(t, err)
		assert.NotNil(t, trust)

		mySecret, err := session.mySecret()
		assert.Nil(t, err)
		assert.NotNil(t, mySecret)

		trustSecret, err := trust.deriveSecret(mySecret)
		assert.Nil(t, err)
		assert.NotNil(t, trustSecret)

		trustSigningKey, err := trust.unlockSigningKey(trustSecret)
		assert.Nil(t, err)
		assert.NotNil(t, trustSigningKey)

	})
}
