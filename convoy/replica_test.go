package convoy

import (
	"fmt"
	"testing"
	"time"

	"github.com/pkopriv2/bourne/common"
	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
)

func TestReplica_Close(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	replica := StartTestReplica(ctx, 0)
	defer replica.Close()

	assert.Nil(t, replica.Close())
}

func TestReplica_Init_EmptyDb(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	replica := StartTestReplica(ctx, 0)
	defer replica.Close()

	// make sure self exists
	members := replica.Collect(func(id uuid.UUID, key string, val string) bool {
		return true
	})

	assert.Equal(t, []Member{replica.Self}, members)
}

func TestReplica_Init_NonEmptyDb(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	db := OpenTestDatabase(ctx, OpenTestChangeLog(ctx))
	db.Log().Append("key", "val", false)

	replica := StartTestReplicaFromDb(ctx, db, 0)
	defer replica.Close()

	// make sure self exists
	members := replica.Collect(func(id uuid.UUID, key string, val string) bool {
		return key == "key"
	})

	assert.Equal(t, []Member{replica.Self}, members)
}

func TestReplica_Dir_Indexing(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	replica := StartTestReplica(ctx, 0)
	defer replica.Close()

	replica.Db.Put("key2", "val")
	time.Sleep(time.Millisecond)

	// make sure that are
	members := replica.Collect(func(id uuid.UUID, key string, val string) bool {
		return key == "key2"
	})

	assert.Equal(t, []Member{replica.Self}, members)
}

func TestReplica_Join_TwoPeers(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	replica1 := StartTestReplica(ctx, 0)
	replica2 := StartTestReplica(ctx, 0)
	defer replica1.Close()
	defer replica2.Close()

	client1 := ReplicaClient(replica1)
	client2 := ReplicaClient(replica2)
	defer client1.Close()
	defer client2.Close()

	err := JoinReplica(replica2, client1)
	if err != nil {
		panic(err)
	}

	members1 := replica1.Collect(func(id uuid.UUID, key string, val string) bool {
		return true
	})

	members2 := replica2.Collect(func(id uuid.UUID, key string, val string) bool {
		return true
	})

	assert.Equal(t, []Member{replica1.Self, replica2.Self}, members1)
	assert.Equal(t, []Member{replica1.Self, replica2.Self}, members2)
}

func TestReplica_Join_ThreePeers(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	replica1 := StartTestReplica(ctx, 0)
	replica2 := StartTestReplica(ctx, 0)
	replica3 := StartTestReplica(ctx, 0)
	defer replica1.Close()
	defer replica2.Close()
	defer replica3.Close()

	client1 := ReplicaClient(replica1)
	client2 := ReplicaClient(replica2)
	client3 := ReplicaClient(replica3)
	defer client1.Close()
	defer client2.Close()
	defer client3.Close()

	err := JoinReplica(replica2, client1)
	if err != nil {
		panic(err)
	}

	err = JoinReplica(replica3, client1)
	if err != nil {
		panic(err)
	}

	time.Sleep(15 * time.Second)

	members1 := replica1.Collect(func(id uuid.UUID, key string, val string) bool {
		return true
	})

	members2 := replica2.Collect(func(id uuid.UUID, key string, val string) bool {
		return true
	})

	members3 := replica3.Collect(func(id uuid.UUID, key string, val string) bool {
		return true
	})

	fmt.Println("MEMBER1: ", members1)
	fmt.Println("MEMBER2: ", members2)
	fmt.Println("MEMBER3: ", members3)

	// assert.Equal(t, []Member{replica1.Self, replica2.Self}, members)
}

func ReplicaClient(r *replica) *client {
	client, err := r.Client()
	if err != nil {
		panic(err)
	}

	return client
}

func StartTestReplica(ctx common.Context, port int) *replica {
	return StartTestReplicaFromDb(ctx, OpenTestDatabase(ctx, OpenTestChangeLog(ctx)), port)
}

func StartTestReplicaFromDb(ctx common.Context, db Database, port int) *replica {
	replica, err := StartReplica(ctx, db, port)
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
