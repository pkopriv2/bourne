package warden

import (
	"crypto/rand"
	"testing"

	"github.com/boltdb/bolt"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/stash"
	uuid "github.com/satori/go.uuid"
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

	t.Run("LoadInvitation_NoExist", func(t *testing.T) {
		db.View(func(tx *bolt.Tx) error {
			_, f, e := loadBoltInvitation(tx, uuid.NewV1())
			assert.False(t, f)
			assert.Nil(t, e)
			return nil
		})
	})

	t.Run("LoadInvitation", func(t *testing.T) {
		db.Update(func(tx *bolt.Tx) error {
			_, l, e := genOracle(rand.Reader)
			assert.Nil(t, e)

			domain, e := GenRsaKey(rand.Reader, 1024)
			assert.Nil(t, e)

			issuer, e := GenRsaKey(rand.Reader, 1024)
			assert.Nil(t, e)

			trustee, e := GenRsaKey(rand.Reader, 1024)
			assert.Nil(t, e)

			inv, e := generateInvitation(rand.Reader, l, domain, issuer, trustee.Public())
			assert.Nil(t, e)
			assert.Nil(t, registerBoltInvitation(tx, inv))

			act, f, e := loadBoltInvitation(tx, inv.Id)
			assert.Nil(t, e)
			assert.True(t, f)
			assert.Equal(t, inv, act)
			return nil
		})
	})
}
