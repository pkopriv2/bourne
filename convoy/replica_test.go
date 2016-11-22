package convoy

import (
	"testing"

	"github.com/pkopriv2/bourne/common"
	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
)

func TestReplica_Close(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	replica, err := StartCluster(ctx, OpenTestDatabase(ctx, OpenTestChangeLog(ctx)), 3089)
	assert.Nil(t, err)
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
	log := db.Log()
	log.Append("key", "val", false)

	replica := StartTestReplicaFromDb(ctx, db)
	defer replica.Close()

	// make sure self exists
	members := replica.Search(func(id uuid.UUID, key string, val string) bool {
		return key == "key"
	})

	assert.Equal(t, []Member{replica.Self}, members)
}

func StartTestReplica(ctx common.Context) *replica {
	return StartTestReplicaFromDb(ctx, OpenTestDatabase(ctx, OpenTestChangeLog(ctx)))
}

func StartTestReplicaFromDb(ctx common.Context, db Database) *replica {
	replica, err := StartCluster(ctx, db, 0)
	if err != nil {
		panic(err)
	}

	return replica
}
