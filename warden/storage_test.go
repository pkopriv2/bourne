package warden

import (
	"crypto/rand"
	"testing"

	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/stash"
	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
)

func TestOracleStorage(t *testing.T) {
	db, err := stash.OpenTransient(common.NewEmptyContext())
	if err != nil {
		t.FailNow()
		return
	}

	if err := initBoltBuckets(db); err != nil {
		t.FailNow()
		return
	}

	store := boltOracleStorage{db}
	t.Run("LoadOracle_NoExist", func(t *testing.T) {
		_, f, e := store.LoadOracle(uuid.NewV1())
		assert.False(t, f)
		assert.Nil(t, e)
	})

	t.Run("LoadOracle_Exist", func(t *testing.T) {
		id := uuid.NewV1()

		o, _, e := genOracle(rand.Reader)
		assert.Nil(t, e)
		assert.Nil(t, store.RegisterOracle(id, o))

		act, f, e := store.LoadOracle(id)
		assert.Nil(t, e)
		assert.True(t, f)
		assert.Equal(t, o, act)
	})

	t.Run("LoadOracleKey_NoExist", func(t *testing.T) {
		_, f, e := store.LoadOracleKey(uuid.NewV1())
		assert.False(t, f)
		assert.Nil(t, e)
	})

	t.Run("LoadOracleKey_Exist", func(t *testing.T) {
		id := uuid.NewV1()

		o, l, e := genOracle(rand.Reader)
		assert.Nil(t, e)

		k, e := genOracleKey(rand.Reader, l, []byte("pass"), o.Opts)
		assert.Nil(t, e)
		assert.Nil(t, store.RegisterOracleKey(id, k))

		act, ok, e := store.LoadOracleKey(id)
		assert.Nil(t, e)
		assert.True(t, ok)
		assert.Equal(t, k, act)
	})
}
