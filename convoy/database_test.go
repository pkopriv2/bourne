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
	log.Append("key1", "val1", false) // 1
	log.Append("key2", "val2", false) // 2
	log.Append("key2", "", true)      //3

	db := OpenTestDatabase(ctx, log)
	ok, item1, err := db.Get("key1")
	assert.True(t, ok)
	assert.Equal(t, "val1", item1.Val)
	assert.Equal(t, 1, item1.Ver)
	assert.Nil(t, err)

	ok, item2, err := db.Get("key2")
	assert.True(t, ok)
	assert.Equal(t, 3, item2.Ver)
	assert.Nil(t, err)
}

func TestDatabase_Put(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	log := OpenTestChangeLog(ctx)

	// should this be buffered???
	l, _ := log.Listen()

	db := OpenTestDatabase(ctx, log)
	db.Put("key", "val", 0)

	chg := <-l.Ch()
	exp := change{1, "key", "val", 1, false}
	assert.Equal(t, exp, chg)
}

func TestDatabase_Del(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	log := OpenTestChangeLog(ctx)

	// should this be buffered???
	l, _ := log.Listen()

	db := OpenTestDatabase(ctx, log)
	db.Del("key", 0)

	chg := <-l.Ch()
	exp := change{1, "key", "", 1, true}
	assert.Equal(t, exp, chg)
}

func OpenTestDatabase(ctx common.Context, log *changeLog) *database {
	db, err := openDatabase(ctx, log)
	if err != nil {
		panic(err)
	}
	return db
}
