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

	cl, e := s.Client(micro.Gob)
	assert.Nil(t, e)


	t.Run("Subscribe", func(t *testing.T) {
		owner, err := GenRsaKey(rand.Reader, 1024)
		if err != nil {
			t.FailNow()
			return
		}

		login := func(pad KeyPad) error {
			return pad.BySignature(owner)
		}

		session, err := Subscribe(ctx, cl.Remote().String(), login, func(o *SubscribeOptions) {
			o.TokenExpiration = 2 * time.Second
		})

		if err != nil {
			ctx.Logger().Error("Session: %v", err)
			t.FailNow()
			return
		}

		ctx.Logger().Info("KEY: %v", session.MyKey().Id())
		time.Sleep(60 * time.Second)
	})
}
