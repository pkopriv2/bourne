package convoy

import (
	"testing"
	"time"

	"github.com/pkopriv2/bourne/common"
	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
)

func TestReplica_Close(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	replica := StartTestReplica(ctx)
	defer replica.Close()

	assert.Nil(t, replica.Close())
}

func TestReplica_Init_EmptyDb(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	replica := StartTestReplica(ctx)
	defer replica.Close()

	// make sure self exists
	members := replica.Search(func(id uuid.UUID, key string, val string) bool {
		return true
	})

	assert.Equal(t, []Member{replica.Self}, members)
}

func TestReplica_Init_NonEmptyDb(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	db := OpenTestDatabase(ctx, OpenTestChangeLog(ctx))
	db.Log().Append("key", "val", false)

	replica := StartTestReplicaFromDb(ctx, db)
	defer replica.Close()

	// make sure self exists
	members := replica.Search(func(id uuid.UUID, key string, val string) bool {
		return key == "key"
	})

	assert.Equal(t, []Member{replica.Self}, members)
}

func TestReplica_Dir_Indexing(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	replica := StartTestReplica(ctx)
	defer replica.Close()

	replica.Db.Put("key2", "val")
	time.Sleep(time.Millisecond)

	// make sure self exists
	members := replica.Search(func(id uuid.UUID, key string, val string) bool {
		return key == "key2"
	})

	assert.Equal(t, []Member{replica.Self}, members)
}

func StartTestReplica(ctx common.Context) *replica {
	return StartTestReplicaFromDb(ctx, OpenTestDatabase(ctx, OpenTestChangeLog(ctx)))
}

func StartTestReplicaFromDb(ctx common.Context, db Database) *replica {
	replica, err := StartReplica(ctx, db, 0)
	if err != nil {
		panic(err)
	}

	ctx.Env().OnClose(func() {
		db.Log().(*changeLog).Stash.Close()
	})

	ctx.Env().OnClose(func() {
		db.Close()
	})

	ctx.Env().OnClose(func() {
		replica.Close()
	})

	return replica
}
