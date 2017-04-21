package warden

import (
	"testing"

	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/stash"
)

func TestStorage(t *testing.T) {
	db, err := stash.OpenTransient(common.NewEmptyContext())
	if err != nil {
		t.FailNow()
		return
	}

	_, err = newBoltStorage(db)
	if err != nil {
		t.FailNow()
		return
	}

	if err := initBoltBuckets(db); err != nil {
		t.FailNow()
		return
	}

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
