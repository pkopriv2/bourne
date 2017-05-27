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

	t.Run("BySignature", func(t *testing.T) {
		ctx := common.NewEmptyContext()
		defer ctx.Close()

		key, session, err := subscribe(ctx)
		if !assert.Nil(t, err) {
			return
		}

		t.Run("Connect", func(t *testing.T) {
			sub, err := connect(ctx, key)
			if !assert.Nil(t, err) {
				return
			}

			assert.Equal(t, session.MyId(), sub.MyId())
		})

		t.Run("ConnectBadKey", func(t *testing.T) {
			_, err := connect(ctx, serverKey)
			assert.NotNil(t, err)
		})

		t.Run("Secret", func(t *testing.T) {
			secret, err := session.mySecret()
			assert.Nil(t, err)
			assert.NotNil(t, secret)
		})

		t.Run("SigningKey", func(t *testing.T) {
			secret, err := session.mySecret()
			if !assert.Nil(t, err) {
				return
			}

			key, err := session.mySigningKey(secret)
			assert.Nil(t, err)
			assert.NotNil(t, key)
		})

		t.Run("InviteKey", func(t *testing.T) {
			secret, err := session.mySecret()
			if !assert.Nil(t, err) {
				return
			}

			key, err := session.myInvitationKey(secret)
			assert.Nil(t, err)
			assert.NotNil(t, key)
		})
	})

	t.Run("ByPassword", func(t *testing.T) {
		ctx := common.NewEmptyContext()
		defer ctx.Close()

		session, err := Subscribe(ctx, addr, func(pad KeyPad) error {
			return pad.ByPassword("user", "pass")
		})
		if !assert.Nil(t, err) {
			return
		}

		t.Run("Connect", func(t *testing.T) {
			sub, err := Connect(ctx, addr, func(pad KeyPad) error {
				return pad.ByPassword("user", "pass")
			})
			if !assert.Nil(t, err) {
				return
			}

			assert.Equal(t, session.MyId(), sub.MyId())
		})

		t.Run("ConnectBadUser", func(t *testing.T) {
			_, err := Connect(ctx, addr, func(pad KeyPad) error {
				return pad.ByPassword("bad", "pass")
			})
			assert.NotNil(t, err)
		})

		t.Run("ConnectBadPass", func(t *testing.T) {
			_, err := Connect(ctx, addr, func(pad KeyPad) error {
				return pad.ByPassword("user", "bad")
			})
			assert.NotNil(t, err)
		})

		t.Run("Secret", func(t *testing.T) {
			secret, err := session.mySecret()
			assert.Nil(t, err)
			assert.NotNil(t, secret)
		})

		t.Run("SigningKey", func(t *testing.T) {
			secret, err := session.mySecret()
			if !assert.Nil(t, err) {
				return
			}

			key, err := session.mySigningKey(secret)
			assert.Nil(t, err)
			assert.NotNil(t, key)
		})

		t.Run("InviteKey", func(t *testing.T) {
			secret, err := session.mySecret()
			if !assert.Nil(t, err) {
				return
			}

			key, err := session.myInvitationKey(secret)
			assert.Nil(t, err)
			assert.NotNil(t, key)
		})
	})

	t.Run("NewTrust", func(t *testing.T) {
		ctx := common.NewEmptyContext()
		defer ctx.Close()

		_, session, err := subscribe(ctx)
		if !assert.Nil(t, err) {
			return
		}

		trust, err := session.NewTrust(timer.Closed(), "test")
		if !assert.Nil(t, err) {
			return
		}

		mySecret, err := session.mySecret()
		if !assert.Nil(t, err) {
			return
		}

		secret, err := trust.deriveSecret(mySecret)
		if !assert.Nil(t, err) {
			return
		}

		t.Run("LoadTrust", func(t *testing.T) {
			loaded, found, err := session.LoadTrust(timer.Closed(), trust.Id)
			if !assert.Nil(t, err) || !assert.NotNil(t, loaded) {
				return
			}

			assert.True(t, found)
			assert.Equal(t, trust.trusteeCert, loaded.trusteeCert)
			assert.Equal(t, trust.trusteeShard, loaded.trusteeShard)

			loadedSecret, err := trust.deriveSecret(mySecret)
			if ! assert.Nil(t, err) {
				return
			}

			assert.Equal(t, secret, loadedSecret)
		})

		t.Run("LoadCertificates", func(t *testing.T) {
			certs, err := session.LoadCertificates(timer.Closed(), trust)
			if ! assert.Nil(t, err) {
				return
			}

			assert.Equal(t, 1, len(certs))
			assert.Equal(t, trust.trusteeCert, certs[0])
		})

		t.Run("MyCertificates", func(t *testing.T) {
			certs, err := session.MyCertificates(timer.Closed())
			if ! assert.Nil(t, err) {
				return
			}

			assert.Equal(t, 1, len(certs))
			assert.Equal(t, trust.trusteeCert, certs[0])
		})

	})

	// t.Run("Invite", func(t *testing.T) {
	// ctx := common.NewEmptyContext()
	// defer ctx.Close()
	//
	// _, session1, err := subscribe(ctx)
	// if err != nil {
	// t.FailNow()
	// return
	// }
	//
	// _, session2, err := subscribe(ctx)
	// if err != nil {
	// t.FailNow()
	// return
	// }
	//
	// trust1, err := session1.NewTrust(timer.Closed(), "invite")
	// assert.Nil(t, err)
	//
	// inv1, err := session1.Invite(timer.Closed(), trust1, session2.MyId())
	// assert.Nil(t, err)
	//
	// invites1, err := session1.MyInvitations(timer.Closed())
	// assert.Nil(t, err)
	// assert.Empty(t, invites1)
	//
	// invites2, err := session2.MyInvitations(timer.Closed())
	// assert.Nil(t, err)
	// assert.Equal(t, 1, len(invites2))
	// assert.Equal(t, inv1.String(), invites2[0].String())
	//
	// certs1, err := session1.MyCertificates(timer.Closed())
	// assert.Nil(t, err)
	// assert.Equal(t, 1, len(certs1))
	// assert.Equal(t, trust1.trusteeCert, certs1[0])
	//
	// certs2, err := session2.MyCertificates(timer.Closed())
	// assert.Nil(t, err)
	// assert.Empty(t, len(certs2))
	//
	// trusts1, err := session1.MyTrusts(timer.Closed())
	// assert.Nil(t, err)
	// assert.Equal(t, 1, len(trusts1))
	// assert.Equal(t, trust1.String(), trusts1[0].String())
	//
	// trusts2, err := session2.MyTrusts(timer.Closed())
	// assert.Nil(t, err)
	// assert.Empty(t, len(trusts2))
	// })
	//
	// t.Run("InviteAndAccept", func(t *testing.T) {
	// ctx := common.NewEmptyContext()
	// defer ctx.Close()
	//
	// _, session1, err := subscribe(ctx)
	// if err != nil {
	// t.FailNow()
	// return
	// }
	//
	// _, session2, err := subscribe(ctx)
	// if err != nil {
	// t.FailNow()
	// return
	// }
	//
	// trust1, err := session1.NewTrust(timer.Closed(), "test")
	// assert.Nil(t, err)
	// assert.NotNil(t, trust1)
	//
	// invite, err := session1.Invite(timer.Closed(), trust1, session2.MyId())
	// assert.Nil(t, err)
	// assert.Nil(t, session2.Accept(timer.Closed(), invite))
	//
	// trust2, o, err := session2.LoadTrust(timer.Closed(), trust1.Id)
	// assert.Nil(t, err)
	// assert.True(t, o)
	// assert.Equal(t, Manager, trust2.trusteeCert.Level)
	//
	// trusts1, err := session1.MyTrusts(timer.Closed())
	// assert.Nil(t, err)
	// assert.Equal(t, 1, len(trusts1))
	// assert.Equal(t, trust1.String(), trusts1[0].String())
	//
	// trusts2, err := session2.MyTrusts(timer.Closed())
	// assert.Nil(t, err)
	// assert.Equal(t, 1, len(trusts2))
	// assert.Equal(t, trust2.String(), trusts2[0].String())
	//
	// certs1, err := session1.LoadCertificates(timer.Closed(), trust1)
	// assert.Nil(t, err)
	// assert.Equal(t, 2, len(certs1))
	// assert.Equal(t, trust1.trusteeCert, certs1[0])
	//
	// certs2, err := session2.LoadCertificates(timer.Closed(), trust1)
	// assert.Nil(t, err)
	// assert.Equal(t, 2, len(certs2))
	// assert.Equal(t, trust1.trusteeCert, certs2[0])
	// })
	//
	// t.Run("Revoke", func(t *testing.T) {
	// ctx := common.NewEmptyContext()
	// defer ctx.Close()
	//
	// _, session1, err := subscribe(ctx)
	// if err != nil {
	// t.FailNow()
	// return
	// }
	//
	// _, session2, err := subscribe(ctx)
	// if err != nil {
	// t.FailNow()
	// return
	// }
	//
	// trust1, err := session1.NewTrust(timer.Closed(), "test")
	// assert.Nil(t, err)
	//
	// invite, err := session1.Invite(timer.Closed(), trust1, session2.MyId())
	// assert.Nil(t, err)
	// assert.Nil(t, session2.Accept(timer.Closed(), invite))
	// assert.Nil(t, session1.Revoke(timer.Closed(), trust1, session2.MyId()))
	//
	// trusts1, err := session1.MyTrusts(timer.Closed())
	// assert.Nil(t, err)
	// assert.Equal(t, 1, len(trusts1))
	// assert.Equal(t, trust1.String(), trusts1[0].String())
	//
	// trusts2, err := session2.MyTrusts(timer.Closed())
	// assert.Nil(t, err)
	// assert.Empty(t, len(trusts2))
	// })
}
