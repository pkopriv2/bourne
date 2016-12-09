package convoy

import (
	"math/rand"
	"testing"
	"time"

	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/concurrent"
	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
)

// TODO: Figure out how to randomize ports!!!

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
	assert.Equal(t, []member{replica.Self}, replica.Dir.Active())
}

func TestReplica_Init_NonEmptyDb(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	db := OpenTestDatabase(ctx, OpenTestChangeLog(ctx))
	db.Put("key", "val")

	replica := StartTestReplicaFromDb(ctx, db, 0)
	defer replica.Close()

	// make sure self exists
	members := replica.Dir.Collect(func(id uuid.UUID, key string, val string) bool {
		return key == "key"
	})

	assert.Equal(t, []member{replica.Self}, members)
}

func TestReplica_Dir_Indexing(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	replica := StartTestReplica(ctx, 0)
	defer replica.Close()

	replica.Db.Put("key2", "val")
	done, timeout := concurrent.NewBreaker(5*time.Second, func() interface{} {
		for {
			members := replica.Dir.Collect(func(id uuid.UUID, key string, val string) bool {
				return key == "key2"
			})

			if len(members) == 0 {
				continue
			}

			if len(members) > 1 {
				t.FailNow()
				return nil
			}

			assert.Equal(t, []member{replica.Self}, members)
			return nil
		}
	})

	select {
	case <-done:
	case <-timeout:
		t.FailNow()
	}
}

func TestReplica_Join(t *testing.T) {
	conf := common.NewConfig(map[string]interface{}{
		"bourne.log.level": int(common.Info),
	})

	ctx := common.NewContext(conf)
	defer ctx.Close()

	master := StartTestReplica(ctx, 8190)
	masterClient := ReplicaClient(master)

	r1 := StartTestReplica(ctx, 8191)
	r2 := StartTestReplica(ctx, 8192)

	replicaJoin(r1, masterClient)
	replicaJoin(r2, masterClient)
	WaitFor([]*replica{master, r1, r2}, func(r *replica) bool {
		return len(r.Dir.Active()) == 3
	})
}

func TestReplica_Leave(t *testing.T) {
	conf := common.NewConfig(map[string]interface{}{
		"bourne.log.level": int(common.Info),
	})

	ctx := common.NewContext(conf)
	defer ctx.Close()

	size := 16
	cluster := StartTestCluster(ctx, size)

	go cluster[rand.Intn(size)].Leave()
	WaitFor(cluster, func(r *replica) bool {
		return len(r.Dir.Active()) == size-1
	})
}

func TestReplica_Fail(t *testing.T) {
	conf := common.NewConfig(map[string]interface{}{
		"bourne.log.level": int(common.Info),
	})

	ctx := common.NewContext(conf)
	defer ctx.Close()

	size := 32
	cluster := StartTestCluster(ctx, size)

	r1 := cluster[rand.Intn(size)]
	r2 := cluster[rand.Intn(size)] // they don't have to be unique

	go r1.Dir.Fail(r2.Self)
	WaitFor(cluster, func(r *replica) bool {
		return len(r.Dir.Unhealthy()) == 1
	})

	WaitFor(cluster, func(r *replica) bool {
		return r.Dissem.Evts.Data.Size() == 0
	})
}

func StartTestCluster(ctx common.Context, num int) []*replica {
	master := StartTestReplica(ctx, 8190)
	masterClient := ReplicaClient(master)

	cluster := []*replica{master}
	for i := num - 1; i > 0; i-- {
		r := StartTestReplica(ctx, 8190+i)
		cluster = append(cluster, r)
	}

	go func(cluster []*replica) {
		for _, r := range cluster {
			replicaJoin(r, masterClient)
		}
	}(cluster)

	closed := make(chan struct{}, 1)
	ctx.Env().OnClose(func() {
		closed<-struct{}{}
	})
	go func(cluster []*replica) {
		for {
			select {
			case <-closed:
				return
			default:
			}
			for _, r := range cluster {
				r.Logger.Debug("Queue depth: %v", r.Dissem.Evts.Data.Size())
			}
		}
	}(cluster)

	WaitFor(cluster, func(r *replica) bool {
		return len(r.Dir.Active()) == len(cluster)
	})

	return cluster
}

func WaitFor(cluster []*replica, fn func(r *replica) bool) {
	done := make(map[member]struct{})
	for len(done) < len(cluster) {
		cluster[0].Logger.Info("Number of sync'ed: %v", len(done))
		for _, r := range cluster {
			if fn(r) {
				done[r.Self] = struct{}{}
			}
		}
		<-time.After(250 * time.Millisecond)
	}
	cluster[0].Logger.Info("Number of sync'ed: %v", len(done))
}

func ReplicaClient(r *replica) *client {
	client, err := r.Client()
	if err != nil {
		panic(err)
	}

	return client
}

func JoinTestReplica(r *replica, c *client) {
	err := replicaJoin(r, c)
	if err != nil {
		panic(err)
	}
}

func StartTestReplica(ctx common.Context, port int) *replica {
	return StartTestReplicaFromDb(ctx, OpenTestDatabase(ctx, OpenTestChangeLog(ctx)), port)
}

func StartTestReplicaFromDb(ctx common.Context, db Database, port int) *replica {
	replica, err := newReplica(ctx, db, port)
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
