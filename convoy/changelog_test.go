package convoy

import (
	"testing"

	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/stash"
	"github.com/stretchr/testify/assert"
)

func TestChangeLog_Close(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	cl := OpenTestChangeLog(ctx)
	assert.Nil(t, cl.Close())
}

func TestChangeLog_Id_Consistency(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	cl := OpenTestChangeLog(ctx)

	id1, err := cl.Id()
	assert.Nil(t, err)

	id2, err := cl.Id()
	assert.Nil(t, err)
	assert.Equal(t, id1, id2)
}

func TestChangeLog_Id_Durability(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	db, err := stash.OpenTransient(ctx)
	assert.Nil(t, err)

	cl := openChangeLog(db)

	id1, err := cl.Id()
	assert.Nil(t, err)
	assert.NotNil(t, id1)

	path := db.Path()
	db.Close()
	ctx.Env().Data().Remove(path)

	db, err = stash.Open(ctx, path)
	assert.Nil(t, err)

	cl = openChangeLog(db)
	id2, err := cl.Id()
	assert.Nil(t, err)
	assert.Equal(t, id1, id2)
}

func TestChangeLog_Seq_Empty(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	cl := OpenTestChangeLog(ctx)

	seq, err := cl.Seq()
	assert.Nil(t, err)
	assert.Equal(t, 0, seq)
}

func TestChangeLog_Seq_Single(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	cl := OpenTestChangeLog(ctx)
	cl.Append("key", "val", false)

	seq, err := cl.Seq()
	assert.Nil(t, err)
	assert.Equal(t, 1, seq)
}

func TestChangeLog_Seq_Multi(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	cl := OpenTestChangeLog(ctx)
	cl.Append("key", "val", false)
	cl.Append("key", "val", false)
	cl.Append("key", "val", false)

	seq, err := cl.Seq()
	assert.Nil(t, err)
	assert.Equal(t, 3, seq)
}

func TestChangeLog_Append(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	cl := OpenTestChangeLog(ctx)
	chg, err := cl.Append("key", "val", false)
	assert.Nil(t, err)

	exp := change{1, "key", "val", 1, false}
	assert.Equal(t, exp, chg)
}

func TestChangeLog_Listen(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	cl := OpenTestChangeLog(ctx)
	listener, _ := cl.Listen()

	chg1, _ := cl.Append("key", "val", false)
	chg2, _ := cl.Append("key", "val1", false)
	chg3, _ := cl.Append("key", "", true)

	ch := listener.Ch()
	assert.Equal(t, chg1, <-ch)
	assert.Equal(t, chg2, <-ch)
	assert.Equal(t, chg3, <-ch)
}

func TestChangeLog_All(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	cl := OpenTestChangeLog(ctx)

	chg1, _ := cl.Append("key", "val", false)
	chg2, _ := cl.Append("key", "val1", false)
	chg3, _ := cl.Append("key", "", true)

	all, err := cl.All()
	assert.Nil(t, err)

	assert.Equal(t, []change{chg1, chg2, chg3}, all)
}

func OpenTestStash(ctx common.Context) stash.Stash {
	db, err := stash.OpenTransient(ctx)
	if err != nil {
		panic(err)
	}
	return db
}

func OpenTestChangeLog(ctx common.Context) *changeLog {
	log := openChangeLog(OpenTestStash(ctx))
	ctx.Env().OnClose(func() {
		log.Close()
	})
	return log
}
