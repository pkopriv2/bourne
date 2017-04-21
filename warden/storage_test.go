package warden

import (
	"crypto/rand"
	"testing"

	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/stash"
	"github.com/stretchr/testify/assert"
)

func TestStorage(t *testing.T) {
	db, err := stash.OpenTransient(common.NewEmptyContext())
	if err != nil {
		t.FailNow()
		return
	}

	if err := initBoltBuckets(db); err != nil {
		t.FailNow()
		return
	}

	t.Run("LoadSubscriber_NoExist", func(t *testing.T) {
		_, o, e := BoltLoadSubscriber(db, "noexist")
		assert.Nil(t, e)
		assert.False(t, o)
	})

	t.Run("EnsureSubscriber_NoExist", func(t *testing.T) {
		_, e := BoltEnsureSubscriber(db, "noexist")
		assert.NotNil(t, e)
	})

	t.Run("StoreSubscriber_Exists", func(t *testing.T) {
		priv, e := GenRsaKey(rand.Reader, 1024)
		assert.Nil(t, e)

		sub, key, e := NewSubscriber(rand.Reader, priv, []byte("password"))
		assert.Nil(t, e)
		assert.Nil(t, BoltStoreSubscriber(db, sub, key))
		assert.NotNil(t, BoltStoreSubscriber(db, sub, key))
	})

	t.Run("StoreSubscriber", func(t *testing.T) {
		priv, e := GenRsaKey(rand.Reader, 1024)
		assert.Nil(t, e)

		sub, key, e := NewSubscriber(rand.Reader, priv, []byte("password"))
		assert.Nil(t, e)
		assert.Nil(t, BoltStoreSubscriber(db, sub, key))

		actSub, o, e := BoltLoadSubscriber(db, sub.Id())
		assert.Nil(t, e)
		assert.True(t, o)
		assert.Equal(t, sub, actSub.Subscriber)

		actSub, e = BoltEnsureSubscriber(db, sub.Id())
		assert.Nil(t, e)
		assert.True(t, o)
		assert.Equal(t, sub, actSub.Subscriber)

		actAuth, o, e := BoltLoadSubscriberAuth(db, sub.Id(), DefaultAuthMethod)
		assert.Nil(t, e)
		assert.True(t, o)
		assert.Equal(t, key, actAuth.SignedOracleKey)
	})
}
