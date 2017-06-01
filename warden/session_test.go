package warden

import "testing"

func TestSession(t *testing.T) {
	/*  ctx := common.NewEmptyContext() */
	// defer ctx.Close()
	//
	// db, err := stash.OpenTransient(ctx)
	// if err != nil {
	// t.FailNow()
	// return
	// }
	//
	// store, e := newBoltStorage(db)
	// if e != nil {
	// t.FailNow()
	// return
	// }
	//
	// serverKey, e := GenRsaKey(rand.Reader, 1024)
	// if e != nil {
	// t.FailNow()
	// return
	// }
	//
	// l, e := net.NewTcpNetwork().Listen(30*time.Second, ":0")
	// if e != nil {
	// t.FailNow()
	// return
	// }
	//
	// s, e := newServer(ctx, store, l, rand.Reader, serverKey, 5)
	// if e != nil {
	// t.FailNow()
	// return
	// }
	//
	// timer := ctx.Timer(30 * time.Second)
	// defer timer.Close()
	//
	// cl, e := s.Client(micro.Gob)
	// assert.Nil(t, e)
	//
	// addr := cl.Remote().String()
	//
	// subscribe := func(ctx common.Context) (PrivateKey, *session, error) {
	// owner, err := GenRsaKey(rand.Reader, 1024)
	// if err != nil {
	// return nil, nil, errors.WithStack(err)
	// }
	//
	// session, err := Subscribe(ctx, addr, func(pad KeyPad) {
	// pad.BySignature([]byte{}, owner)
	// })
	// if err != nil {
	// return nil, nil, errors.WithStack(err)
	// }
	//
	// ctx.Control().Defer(func(error) {
	// session.Close()
	// })
	//
	// return owner, session, nil
	// }
	//
	// connect := func(ctx common.Context, signer Signer) (*session, error) {
	// session, err := Connect(ctx, addr, func(pad KeyPad) {
	// pad.BySignature(signer)
	// })
	// if err != nil {
	// return nil, errors.WithStack(err)
	// }
	//
	// ctx.Control().Defer(func(error) {
	// session.Close()
	// })
	//
	// return session, nil
	// }
	//
	// t.Run("BySignature", func(t *testing.T) {
	// ctx := common.NewEmptyContext()
	// defer ctx.Close()
	//
	// key, session, err := subscribe(ctx)
	// if !assert.Nil(t, err) {
	// return
	// }
	//
	// t.Run("Connect", func(t *testing.T) {
	// sub, err := connect(ctx, key)
	// if !assert.Nil(t, err) {
	// return
	// }
	//
	// assert.Equal(t, session.MyId(), sub.MyId())
	// })
	//
	// t.Run("ConnectBadKey", func(t *testing.T) {
	// _, err := connect(ctx, serverKey)
	// assert.NotNil(t, err)
	// })
	//
	// t.Run("Secret", func(t *testing.T) {
	// secret, err := session.mySecret()
	// assert.Nil(t, err)
	// assert.NotNil(t, secret)
	// })
	//
	// t.Run("SigningKey", func(t *testing.T) {
	// secret, err := session.mySecret()
	// if !assert.Nil(t, err) {
	// return
	// }
	//
	// key, err := session.mySigningKey(secret)
	// assert.Nil(t, err)
	// assert.NotNil(t, key)
	// })
	//
	// t.Run("InviteKey", func(t *testing.T) {
	// secret, err := session.mySecret()
	// if !assert.Nil(t, err) {
	// return
	// }
	//
	// key, err := session.myInvitationKey(secret)
	// assert.Nil(t, err)
	// assert.NotNil(t, key)
	// })
	// })
	//
	// t.Run("ByPassword", func(t *testing.T) {
	// ctx := common.NewEmptyContext()
	// defer ctx.Close()
	//
	// session, err := Subscribe(ctx, addr, func(pad KeyPad) error {
	// return pad.ByPassword("user", "pass")
	// })
	// if !assert.Nil(t, err) {
	// return
	// }
	//
	// t.Run("Connect", func(t *testing.T) {
	// sub, err := Connect(ctx, addr, func(pad KeyPad) error {
	// return pad.ByPassword("user", "pass")
	// })
	// if !assert.Nil(t, err) {
	// return
	// }
	//
	// assert.Equal(t, session.MyId(), sub.MyId())
	// })
	//
	// t.Run("ConnectBadUser", func(t *testing.T) {
	// _, err := Connect(ctx, addr, func(pad KeyPad) error {
	// return pad.ByPassword("bad", "pass")
	// })
	// assert.NotNil(t, err)
	// })
	//
	// t.Run("ConnectBadPass", func(t *testing.T) {
	// _, err := Connect(ctx, addr, func(pad KeyPad) error {
	// return pad.ByPassword("user", "bad")
	// })
	// assert.NotNil(t, err)
	// })
	//
	// t.Run("Secret", func(t *testing.T) {
	// secret, err := session.mySecret()
	// assert.Nil(t, err)
	// assert.NotNil(t, secret)
	// })
	//
	// t.Run("SigningKey", func(t *testing.T) {
	// secret, err := session.mySecret()
	// if !assert.Nil(t, err) {
	// return
	// }
	//
	// key, err := session.mySigningKey(secret)
	// assert.Nil(t, err)
	// assert.NotNil(t, key)
	// })
	//
	// t.Run("InviteKey", func(t *testing.T) {
	// secret, err := session.mySecret()
	// if !assert.Nil(t, err) {
	// return
	// }
	//
	// key, err := session.myInvitationKey(secret)
	// assert.Nil(t, err)
	// assert.NotNil(t, key)
	// })
	// })
	//
	// t.Run("Trust", func(t *testing.T) {
	// ctx := common.NewEmptyContext()
	// defer ctx.Close()
	//
	// _, session, err := subscribe(ctx)
	// if !assert.Nil(t, err) {
	// return
	// }
	//
	// trust, err := session.NewTrust(timer.Closed(), "test")
	// if !assert.Nil(t, err) {
	// return
	// }
	//
	// mySecret, err := session.mySecret()
	// if !assert.Nil(t, err) {
	// return
	// }
	//
	// secret, err := trust.deriveSecret(mySecret)
	// if !assert.Nil(t, err) {
	// return
	// }
	//
	// t.Run("Load", func(t *testing.T) {
	// loaded, found, err := session.LoadTrust(timer.Closed(), trust.Id)
	// if !assert.Nil(t, err) || !assert.NotNil(t, loaded) {
	// return
	// }
	//
	// assert.True(t, found)
	// assert.Equal(t, trust.trusteeCert, loaded.trusteeCert)
	// assert.Equal(t, trust.trusteeShard, loaded.trusteeShard)
	//
	// loadedSecret, err := trust.deriveSecret(mySecret)
	// if !assert.Nil(t, err) {
	// return
	// }
	//
	// assert.Equal(t, secret, loadedSecret)
	// })
	//
	// t.Run("LoadCertificates", func(t *testing.T) {
	// certs, err := session.LoadCertificates(timer.Closed(), trust)
	// if !assert.Nil(t, err) {
	// return
	// }
	//
	// assert.Equal(t, 1, len(certs))
	// assert.Equal(t, trust.trusteeCert, certs[0])
	// })
	//
	// t.Run("LoadInvitations", func(t *testing.T) {
	// invites, err := session.LoadInvitations(timer.Closed(), trust)
	// if !assert.Nil(t, err) {
	// return
	// }
	// assert.Empty(t, invites)
	// })
	//
	// t.Run("MyCertificates", func(t *testing.T) {
	// certs, err := session.MyCertificates(timer.Closed())
	// if !assert.Nil(t, err) {
	// return
	// }
	//
	// assert.Equal(t, 1, len(certs))
	// assert.Equal(t, trust.trusteeCert, certs[0])
	// })
	//
	// t.Run("MyTrusts", func(t *testing.T) {
	// trusts, err := session.MyTrusts(timer.Closed())
	// if !assert.Nil(t, err) {
	// return
	// }
	//
	// assert.Equal(t, 1, len(trusts))
	// assert.Equal(t, trust, trusts[0])
	// })
	//
	// _, recipient, err := subscribe(ctx)
	// if !assert.Nil(t, err) {
	// return
	// }
	//
	// t.Run("Invite", func(t *testing.T) {
	// invite, err := session.Invite(timer.Closed(), trust, recipient.MyId())
	// if !assert.Nil(t, err) {
	// return
	// }
	// assert.NotNil(t, invite)
	// assert.Equal(t, trust.Id, invite.Cert.TrustId)
	// assert.Equal(t, session.MyId(), invite.Cert.IssuerId)
	// assert.Equal(t, recipient.MyId(), invite.Cert.TrusteeId)
	//
	// // invites, err := session.LoadInvitations(timer.Closed(), trust)
	// // if !assert.Nil(t, err) {
	// // return
	// // }
	// // if ! assert.Equal(t, 1, len(invites)) {
	// // return
	// // }
	// // assert.Equal(t, invite, invites[0])
	// })
	//
	// t.Run("Accept", func(t *testing.T) {
	// invites, err := recipient.MyInvitations(timer.Closed())
	// if !assert.Nil(t, err) {
	// return
	// }
	// assert.Equal(t, 1, len(invites))
	// assert.Equal(t, trust.Id, invites[0].Cert.TrustId)
	// assert.Equal(t, session.MyId(), invites[0].Cert.IssuerId)
	// assert.Equal(t, recipient.MyId(), invites[0].Cert.TrusteeId)
	//
	// if !assert.Nil(t, recipient.Accept(timer.Closed(), invites[0])) {
	// return
	// }
	//
	// certs, err := recipient.MyCertificates(timer.Closed())
	// if !assert.Nil(t, err) {
	// return
	// }
	// assert.Equal(t, 1, len(certs))
	// assert.Equal(t, trust.Id, certs[0].TrustId)
	//
	// trust, found, err := recipient.LoadTrust(timer.Closed(), invites[0].Cert.TrustId)
	// if !assert.Nil(t, err) || !assert.True(t, found) {
	// return
	// }
	// assert.Equal(t, trust.Id, certs[0].TrustId)
	// })
	//
	// t.Run("Revoke", func(t *testing.T) {
	// assert.Nil(t, session.Revoke(timer.Closed(), trust, recipient.MyId()))
	//
	// trusts, err := recipient.MyTrusts(timer.Closed())
	// assert.Nil(t, err)
	// assert.Empty(t, trusts)
	// })
	/* }) */
}
