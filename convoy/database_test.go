package convoy

import (
	"testing"

	"github.com/pkopriv2/bourne/common"
	"github.com/stretchr/testify/assert"
)

func TestDatabase_Close(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	log := OpenTestChangeLog(ctx)
	db := OpenTestDatabase(ctx, log)
	assert.Nil(t, db.Close())
}

func TestDatabase_OpenWithChanges(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	log := OpenTestChangeLog(ctx)
	log.Append("key1", "val1", false)
	log.Append("key2", "val2", false)
	log.Append("key2", "", true)

	db := OpenTestDatabase(ctx, log)
	val1, ok1, _ := db.Get("key1")
	assert.Equal(t, "val1", val1)
	assert.True(t, ok1)

	_, ok2, _ := db.Get("key2")
	assert.False(t, ok2)
}

func TestDatabase_Put(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	log := OpenTestChangeLog(ctx)

	// should this be buffered???
	ch := changeLogListen(log)

	db := OpenTestDatabase(ctx, log)
	db.Put("key", "val")

	chg := <-ch
	exp := Change{1, "key", "val", 1, false}
	assert.Equal(t, exp, chg)
}

func TestDatabase_Del(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	log := OpenTestChangeLog(ctx)

	// should this be buffered???
	ch := changeLogListen(log)

	db := OpenTestDatabase(ctx, log)
	db.Del("key")

	chg := <-ch
	exp := Change{1, "key", "", 1, true}
	assert.Equal(t, exp, chg)
}

func OpenTestDatabase(ctx common.Context, log ChangeLog) Database {
	db, err := initDatabase(ctx, log)
	if err != nil {
		panic(err)
	}
	return db
}
