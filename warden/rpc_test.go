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

func TestRpc(t *testing.T) {
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

	rpc := newClient(cl)

	owner, err := GenRsaKey(rand.Reader, 1024)
	if err != nil {
		t.FailNow()
		return
	}

	t.Run("Register", func(t *testing.T) {
		creds, e := enterCreds(func(pad KeyPad) error {
			return pad.BySignature(owner)
		})
		assert.Nil(t, e)

		mem, ac, e := newMember(rand.Reader, creds)
		assert.Nil(t, e)
		assert.Nil(t, rpc.Register(nil, mem, ac))

		c, sig, e := newSigChallenge(rand.Reader, owner, SHA256)
		assert.Nil(t, e)

		token, e := rpc.TokenBySignature(nil, ac.Lookup(), c, sig, 30*time.Minute)
		assert.Nil(t, e)
		assert.NotNil(t, token)

		actMem, actAc, o, e := rpc.MemberByLookup(nil, token, ac.Lookup())
		assert.Nil(t, e)
		assert.True(t, o)
		assert.NotNil(t, actMem)
		assert.NotNil(t, actAc)

		expSecret, e := ac.Derive(mem.Pub, creds)
		assert.Nil(t, e)

		actSecret, e := actAc.Derive(actMem.Pub, creds)
		assert.Nil(t, e)
		assert.Equal(t, expSecret, actSecret)
	})

	// t.Run("LoadSubscriber_NoExist", func(t *testing.T) {
	// _, o, e := store.LoadSubscriber("noexist")
	// assert.Nil(t, e)
	// assert.False(t, o)
	// })
	//
	// t.Run("EnsureSubscriber_NoExist", func(t *testing.T) {
	// _, e := EnsureSubscriber(store, "noexist")
	// assert.NotNil(t, e)
	// })
	//
	// t.Run("StoreSubscriber_Exists", func(t *testing.T) {
	// priv, e := GenRsaKey(rand.Reader, 1024)
	// assert.Nil(t, e)
	//
	// sub, key, e := NewSubscriber(rand.Reader, priv)
	// assert.Nil(t, e)
	// assert.Nil(t, store.SaveSubscriber(sub, key))
	// assert.NotNil(t, store.SaveSubscriber(sub, key))
	// })
	//
	// t.Run("StoreSubscriber", func(t *testing.T) {
	// priv, e := GenRsaKey(rand.Reader, 1024)
	// assert.Nil(t, e)
	//
	// sub, key, e := NewSubscriber(rand.Reader, priv)
	// assert.Nil(t, e)
	// assert.Nil(t, store.SaveSubscriber(sub, key))
	//
	// actSub, o, e := store.LoadSubscriber(sub.Id())
	// assert.Nil(t, e)
	// assert.True(t, o)
	// assert.Equal(t, sub, actSub.Subscriber)
	//
	// actSub, e = EnsureSubscriber(store, sub.Id())
	// assert.Nil(t, e)
	// assert.True(t, o)
	// assert.Equal(t, sub, actSub.Subscriber)
	//
	// actAuth, o, e := store.LoadSubscriberAuth(sub.Id(), DefaultAuthMethod)
	// assert.Nil(t, e)
	// assert.True(t, o)
	// assert.Equal(t, key, actAuth.SignedOracleKey)
	// })
}
