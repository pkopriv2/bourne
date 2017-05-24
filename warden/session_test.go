package warden

import (
	"crypto/rand"
	"testing"
	"time"

	"github.com/pkg/errors"
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

	subscribe := func(ctx common.Context) (PrivateKey, *Session, error) {
		owner, err := GenRsaKey(rand.Reader, 1024)
		if err != nil {
			return nil, nil, errors.WithStack(err)
		}

		session, err := Subscribe(ctx, addr, func(pad KeyPad) error {
			return pad.BySignature(owner)
		})
		if err != nil {
			return nil, nil, errors.WithStack(err)
		}

		ctx.Control().Defer(func(error) {
			session.Close()
		})

		return owner, session, nil
	}

	connect := func(ctx common.Context, signer Signer) (*Session, error) {
		session, err := Connect(ctx, addr, func(pad KeyPad) error {
			return pad.BySignature(signer)
		})
		if err != nil {
			return nil, errors.WithStack(err)
		}

		ctx.Control().Defer(func(error) {
			session.Close()
		})

		return session, nil
	}

	t.Run("Subscribe", func(t *testing.T) {
		ctx := common.NewEmptyContext()
		defer ctx.Close()

		key, session1, err := subscribe(ctx)
		assert.Nil(t, err)

		session2, err := connect(ctx, key)
		assert.Nil(t, err)
		assert.Equal(t, session1.MyKey().Id(), session2.MyKey().Id())
	})

	t.Run("NewTrust", func(t *testing.T) {
		ctx := common.NewEmptyContext()
		defer ctx.Close()

		_, session, err := subscribe(ctx)
		if err != nil {
			t.FailNow()
			return
		}

		trust, err := session.NewTrust(timer.Closed(), "test")
		assert.Nil(t, err)

		trust, o, err := session.LoadTrustById(timer.Closed(), trust.Id)
		assert.Nil(t, err)
		assert.True(t, o)

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

	t.Run("Invite", func(t *testing.T) {
		ctx := common.NewEmptyContext()
		defer ctx.Close()

		_, session1, err := subscribe(ctx)
		if err != nil {
			t.FailNow()
			return
		}

		_, session2, err := subscribe(ctx)
		if err != nil {
			t.FailNow()
			return
		}

		trust, err := session1.NewTrust(timer.Closed(), "invite")
		assert.Nil(t, err)
		assert.NotNil(t, trust)

		inv1, err := session1.Invite(timer.Closed(), trust, session2.MyId())
		assert.Nil(t, err)
		assert.NotNil(t, inv1)

		invites1, err := session1.MyInvitations(timer.Closed())
		assert.Nil(t, err)
		assert.Empty(t, invites1)

		invites2, err := session2.MyInvitations(timer.Closed())
		assert.Nil(t, err)
		assert.Equal(t, 1, len(invites2))

		inv2, ok, err := session1.LoadInvitationById(timer.Closed(), inv1.Id)
		assert.Nil(t, err)
		assert.True(t, ok)
		assert.Equal(t, inv1, inv2)
	})

	t.Run("InviteAndAccept", func(t *testing.T) {
		ctx := common.NewEmptyContext()
		defer ctx.Close()

		_, session1, err := subscribe(ctx)
		if err != nil {
			t.FailNow()
			return
		}

		_, session2, err := subscribe(ctx)
		if err != nil {
			t.FailNow()
			return
		}

		trust1, err := session1.NewTrust(timer.Closed(), "test")
		assert.Nil(t, err)
		assert.NotNil(t, trust1)

		inv112, err := session1.Invite(timer.Closed(), trust1, session2.MyId())
		assert.Nil(t, err)
		assert.Nil(t, session2.Accept(timer.Closed(), inv112))

		trust2, o, err := session2.LoadTrustById(timer.Closed(), trust1.Id)
		assert.Nil(t, err)
		assert.True(t, o)
		assert.Equal(t, Manager, trust2.trusteeCert.Level)
	})

	t.Run("Revoke", func(t *testing.T) {
		ctx := common.NewEmptyContext()
		defer ctx.Close()

		_, session1, err := subscribe(ctx)
		if err != nil {
			t.FailNow()
			return
		}

		_, session2, err := subscribe(ctx)
		if err != nil {
			t.FailNow()
			return
		}

		trust11, err := session1.NewTrust(timer.Closed(), "test")
		assert.Nil(t, err)
		assert.NotNil(t, trust11)

		inv112, err := session1.Invite(timer.Closed(), trust11, session2.MyId())
		assert.Nil(t, err)
		assert.Nil(t, session2.Accept(timer.Closed(), inv112))

		_, o, err := session2.LoadTrustById(timer.Closed(), trust11.Id)
		assert.Nil(t, err)
		assert.True(t, o)
		assert.Nil(t, session1.Revoke(timer.Closed(), trust11, session2.MyId()))

		trust21, o, err := session2.LoadTrustById(timer.Closed(), trust11.Id)
		assert.Equal(t, None,  trust21.trusteeCert.Level)
	})
}
