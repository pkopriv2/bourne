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
	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
)

func TestSession(t *testing.T) {
	ctx := common.NewEmptyContext()
	defer ctx.Close()

	db, e := stash.OpenTransient(ctx)
	if !assert.Nil(t, e) {
		return
	}

	store, e := newBoltStorage(db)
	if !assert.Nil(t, e) {
		return
	}

	serverKey, e := GenRsaKey(rand.Reader, 1024)
	if !assert.Nil(t, e) {
		return
	}

	l, e := net.NewTcpNetwork().Listen(30*time.Second, ":0")
	if !assert.Nil(t, e) {
		return
	}

	s, e := newServer(ctx, store, l, rand.Reader, serverKey, 5)
	if !assert.Nil(t, e) {
		return
	}

	timer := ctx.Timer(30 * time.Second)
	defer timer.Close()

	cl, e := s.Client(micro.Gob)
	if !assert.Nil(t, e) {
		return
	}

	addr := cl.Remote().String()

	subscribeBySigner := func(ctx common.Context) (PrivateKey, *session, error) {
		owner, err := GenRsaKey(rand.Reader, 1024)
		if err != nil {
			return nil, nil, errors.WithStack(err)
		}

		token, err := newToken(uuid.NewV1(), uuid.NewV1(), 30*time.Second, nil).Sign(rand.Reader, serverKey, SHA256)
		if err != nil {
			return nil, nil, errors.WithStack(err)
		}

		registrar, err := Register(ctx, addr, token)
		if err != nil {
			return nil, nil, errors.WithStack(err)
		}

		sessionn, err := registrar.RegisterByKey(owner.Public()).AuthBySignature(owner)
		if err != nil {
			return nil, nil, errors.WithStack(err)
		}

		ctx.Control().Defer(func(error) {
			sessionn.Close()
		})

		return owner, sessionn.(*session), nil
	}

	connectBySigner := func(ctx common.Context, signer Signer) (*session, error) {
		dir, err := Connect(ctx, addr)
		if err != nil {
			return nil, errors.WithStack(err)
		}

		sessionn, err := dir.LookupByKey(signer.Public()).AuthBySignature(signer)
		if err != nil {
			return nil, errors.WithStack(err)
		}

		ctx.Control().Defer(func(error) {
			sessionn.Close()
		})

		return sessionn.(*session), nil
	}

	t.Run("BySignature", func(t *testing.T) {
		ctx := common.NewEmptyContext()
		defer ctx.Close()

		key, session, err := subscribeBySigner(ctx)
		if !assert.Nil(t, err) {
			return
		}

		t.Run("Connect", func(t *testing.T) {
			sub, err := connectBySigner(ctx, key)
			if !assert.Nil(t, err) {
				return
			}

			assert.Equal(t, session.MyId(), sub.MyId())
		})

		t.Run("ConnectBadKey", func(t *testing.T) {
			_, err := connectBySigner(ctx, serverKey)
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

		token, err := newToken(uuid.NewV1(), uuid.NewV1(), 30*time.Second, nil).Sign(rand.Reader, serverKey, SHA256)
		if !assert.Nil(t, err) {
			return
		}

		registrar, err := Register(ctx, addr, token)
		if !assert.Nil(t, err) {
			return
		}

		sessionn, err := registrar.RegisterByEmail("user@example.com").AuthByPassphrase("pass")
		if !assert.Nil(t, err) {
			return
		}

		session := sessionn.(*session)

		t.Run("Connect", func(t *testing.T) {
			dir, err := Connect(ctx, addr)
			if !assert.Nil(t, err) {
				return
			}

			sub, err := dir.LookupByEmail("user@example.com").AuthByPassphrase("pass")
			if !assert.Nil(t, err) {
				return
			}

			assert.Equal(t, session.MyId(), sub.MyId())
		})

		t.Run("ConnectBadUser", func(t *testing.T) {
			dir, err := Connect(ctx, addr)
			if !assert.Nil(t, err) {
				return
			}

			_, err = dir.LookupByEmail("noexist@example.com").AuthByPassphrase("pass")
			assert.NotNil(t, err)
		})

		t.Run("ConnectBadPass", func(t *testing.T) {
			dir, err := Connect(ctx, addr)
			if !assert.Nil(t, err) {
				return
			}

			_, err = dir.LookupByEmail("user@example.com").AuthByPassphrase("badpass")
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

	t.Run("Trust", func(t *testing.T) {
		ctx := common.NewEmptyContext()
		defer ctx.Close()

		_, session, err := subscribeBySigner(ctx)
		if !assert.Nil(t, err) {
			return
		}

		trust, err := session.NewTrust(timer.Closed(), Minimal)
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

		t.Run("Load", func(t *testing.T) {
			loaded, found, err := session.LoadTrust(timer.Closed(), trust.Id)
			if !assert.Nil(t, err) || !assert.NotNil(t, loaded) {
				return
			}

			assert.True(t, found)
			assert.Equal(t, trust.trusteeCert, loaded.trusteeCert)
			assert.Equal(t, trust.trusteeShard, loaded.trusteeShard)

			loadedSecret, err := trust.deriveSecret(mySecret)
			if !assert.Nil(t, err) {
				return
			}

			assert.Equal(t, secret, loadedSecret)
		})

		t.Run("LoadCertificates", func(t *testing.T) {
			certs, err := session.LoadCertificatesByTrust(timer.Closed(), trust)
			if !assert.Nil(t, err) {
				return
			}

			assert.Equal(t, 1, len(certs))
			assert.Equal(t, trust.trusteeCert, certs[0])
		})

		t.Run("LoadInvitations", func(t *testing.T) {
			invites, err := session.LoadInvitationsByTrust(timer.Closed(), trust)
			if !assert.Nil(t, err) {
				return
			}
			assert.Empty(t, invites)
		})

		t.Run("MyCertificates", func(t *testing.T) {
			certs, err := session.MyCertificates(timer.Closed())
			if !assert.Nil(t, err) {
				return
			}

			assert.Equal(t, 1, len(certs))
			assert.Equal(t, trust.trusteeCert, certs[0])
		})

		t.Run("MyTrusts", func(t *testing.T) {
			trusts, err := session.MyTrusts(timer.Closed())
			if !assert.Nil(t, err) {
				return
			}

			assert.Equal(t, 1, len(trusts))
			assert.Equal(t, trust, trusts[0])
		})

		_, recipient, err := subscribeBySigner(ctx)
		if !assert.Nil(t, err) {
			return
		}

		t.Run("Invite", func(t *testing.T) {
			invite, err := session.Invite(timer.Closed(), trust, recipient.MyId())
			if !assert.Nil(t, err) {
				return
			}
			assert.NotNil(t, invite)
			assert.Equal(t, trust.Id, invite.Cert.TrustId)
			assert.Equal(t, session.MyId(), invite.Cert.IssuerId)
			assert.Equal(t, recipient.MyId(), invite.Cert.TrusteeId)

			// invites, err := session.LoadInvitations(timer.Closed(), trust)
			// if !assert.Nil(t, err) {
			// return
			// }
			// if ! assert.Equal(t, 1, len(invites)) {
			// return
			// }
			// assert.Equal(t, invite, invites[0])
		})

		t.Run("Accept", func(t *testing.T) {
			invites, err := recipient.MyInvitations(timer.Closed())
			if !assert.Nil(t, err) {
				return
			}
			assert.Equal(t, 1, len(invites))
			assert.Equal(t, trust.Id, invites[0].Cert.TrustId)
			assert.Equal(t, session.MyId(), invites[0].Cert.IssuerId)
			assert.Equal(t, recipient.MyId(), invites[0].Cert.TrusteeId)

			if !assert.Nil(t, recipient.Accept(timer.Closed(), invites[0])) {
				return
			}

			certs, err := recipient.MyCertificates(timer.Closed())
			if !assert.Nil(t, err) {
				return
			}
			assert.Equal(t, 1, len(certs))
			assert.Equal(t, trust.Id, certs[0].TrustId)

			trust, found, err := recipient.LoadTrust(timer.Closed(), invites[0].Cert.TrustId)
			if !assert.Nil(t, err) || !assert.True(t, found) {
				return
			}
			assert.Equal(t, trust.Id, certs[0].TrustId)
		})

		t.Run("Revoke", func(t *testing.T) {
			assert.Nil(t, session.Revoke(timer.Closed(), trust, recipient.MyId()))

			trusts, err := recipient.MyTrusts(timer.Closed())
			assert.Nil(t, err)
			assert.Empty(t, trusts)
		})
	})
}
