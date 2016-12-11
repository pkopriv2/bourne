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

	assert.Nil(t, replica.Close())
	assert.NotNil(t, replica.Close())
}

func TestReplica_Init_EmptyDb(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	replica := StartTestReplica(ctx, 0)
	defer replica.Close()

	// make sure self exists
	assert.Equal(t, []member{replica.Self}, replica.Dir.AllActive())
}

func TestReplica_Init_NonEmptyDb(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	db := OpenTestDatabase(ctx, OpenTestChangeLog(ctx))
	db.Put("key", "val")

	replica := StartTestReplicaFromDb(ctx, db, 0)
	defer replica.Close()

	// make sure self exists
	members := replica.Dir.Search(func(id uuid.UUID, key string, val string) bool {
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
			members := replica.Dir.Search(func(id uuid.UUID, key string, val string) bool {
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

	done, timeout := concurrent.NewBreaker(10*time.Second, func() interface{} {
		Sync([]*replica{master, r1, r2}, func(r *replica) bool {
			return len(r.Dir.AllActive()) == 3
		})
		return nil
	})

	select {
	case <-done:
	case <-timeout:
		t.FailNow()
	}
}

func TestReplica_Leave(t *testing.T) {
	conf := common.NewConfig(map[string]interface{}{
		"bourne.log.level": int(common.Info),
	})

	ctx := common.NewContext(conf)
	defer ctx.Close()

	size := 128
	cluster := StartTestCluster(ctx, size)

	assert.True(t, true)

	//choose a random member
	idx := rand.Intn(size)
	rep := cluster[idx]
	rep.Leave()

	done, timeout := concurrent.NewBreaker(10*time.Second, func() interface{} {
		Sync(removeReplica(cluster, idx), func(r *replica) bool {
			return !r.Dir.IsHealthy(rep.Id()) || !r.Dir.IsActive(rep.Id())
		})

		return nil
	})

	select {
	case <-done:
	case <-timeout:
		t.FailNow()
	}

	assert.Equal(t, replicaClosedError, rep.ensureOpen())
}

func TestReplica_Evict(t *testing.T) {
	conf := common.NewConfig(map[string]interface{}{
		"bourne.log.level": int(common.Info),
	})

	ctx := common.NewContext(conf)
	defer ctx.Close()

	size := 128
	cluster := StartTestCluster(ctx, size)

	indices := rand.Perm(size)
	r1 := cluster[indices[0]]
	r2 := cluster[indices[1]]

	r1.Logger.Info("Evicting member [%v]", r2.Self)
	r1.Dir.Evict(r2.Self)

	done, timeout := concurrent.NewBreaker(10*time.Second, func() interface{} {
		Sync(removeReplica(cluster, indices[1]), func(r *replica) bool {
			return !r.Dir.IsActive(r2.Id())
		})

		Sync([]*replica{r2}, func(r *replica) bool {
			err := r.ensureOpen()
			return err != nil
		})
		return nil
	})

	select {
	case <-done:
	case <-timeout:
		t.Fail()
	}

	assert.Equal(t, replicaEvictedError, r2.ensureOpen())
}

func TestReplica_Fail_Manual(t *testing.T) {
	conf := common.NewConfig(map[string]interface{}{
		"bourne.log.level": int(common.Info),
	})

	ctx := common.NewContext(conf)
	defer ctx.Close()

	size := 128
	cluster := StartTestCluster(ctx, size)

	r1 := cluster[rand.Intn(size)]
	r2 := cluster[rand.Intn(size)] // they don't have to be unique
	r1.Dir.Fail(r2.Self)

	done, timeout := concurrent.NewBreaker(10*time.Second, func() interface{} {
		Sync(cluster, func(r *replica) bool {
			h, _ := r.Dir.Health(r2.Self.Id)
			return !h.Healthy
		})

		Sync([]*replica{r2}, func(r *replica) bool {
			err := r.ensureOpen()
			return err != nil
		})
		return nil
	})

	select {
	case <-done:
	case <-timeout:
		t.FailNow()
	}

	assert.Equal(t, replicaFailureError, r2.ensureOpen())
}

func TestReplica_Fail_Automatic(t *testing.T) {
	conf := common.NewConfig(map[string]interface{}{
		"bourne.log.level": int(common.Info),
	})

	ctx := common.NewContext(conf)
	defer ctx.Close()

	size := 128
	cluster := StartTestCluster(ctx, size)

	i := rand.Intn(size)
	m := cluster[i]

	m.Logger.Info("Triggering failure.")
	m.Server.Close()

	done, timeout := concurrent.NewBreaker(30*time.Second, func() interface{} {
		remaining := removeReplica(cluster, i)
		Sync(remaining, func(r *replica) bool {
			h, _ := r.Dir.Health(m.Self.Id)
			return !h.Healthy
		})

		Sync([]*replica{m}, func(r *replica) bool {
			err := r.ensureOpen()
			return err != nil
		})
		return nil
	})

	select {
	case <-done:
	case <-timeout:
		t.FailNow()
	}
}

func TestReplica_SingleDb_SingleUpdate(t *testing.T) {
	conf := common.NewConfig(map[string]interface{}{
		"bourne.log.level": int(common.Info),
	})

	ctx := common.NewContext(conf)
	defer ctx.Close()

	size := 128
	cluster := StartTestCluster(ctx, size)

	i := rand.Intn(size)
	m := cluster[i]
	m.Db.Put("key", "val")

	done, timeout := concurrent.NewBreaker(10*time.Second, func() interface{} {
		remaining := removeReplica(cluster, i)
		Sync(remaining, func(r *replica) bool {
			found := r.Dir.Search(func(id uuid.UUID, key string, val string) bool {
				return id == m.Self.Id && key == "key" && val == "val"
			})

			return len(found) == 1
		})
		return nil
	})

	select {
	case <-done:
	case <-timeout:
		t.FailNow()
	}
}

func removeReplica(cluster []*replica, i int) []*replica {
	return append(cluster[:i], cluster[i+1:]...)
}

func StartTestCluster(ctx common.Context, num int) []*replica {
	master := StartTestReplica(ctx, 8190)
	masterClient := ReplicaClient(master)
	defer masterClient.Close()

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
		closed <- struct{}{}
	})

	Sync(cluster, func(r *replica) bool {
		return len(r.Dir.AllActive()) == len(cluster)
	})

	return cluster
}

func Sync(cluster []*replica, fn func(r *replica) bool) {
	done := make(map[member]struct{})
	start := time.Now()

	for len(done) < len(cluster) {
		cluster[0].Logger.Info("Number of sync'ed: %v", len(done))
		for _, r := range cluster {
			if _, ok := done[r.Self]; ok {
				continue
			}

			if fn(r) {
				done[r.Self] = struct{}{}
				continue
			}

			if time.Now().Sub(start) > 10*time.Second {
				r.Logger.Info("Still not sync'ed")
				r.Logger.Info("Queue depth: %v", r.Dissem.events.data.Size())
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
	replica, err := initReplica(ctx, db, "localhost", port)
	if err != nil {
		panic(err)
	}

	ctx.Env().OnClose(func() {
		db.Log().(*changeLog).stash.Close()
	})

	ctx.Env().OnClose(func() {
		db.Close()
	})

	ctx.Env().OnClose(func() {
		replica.Close()
	})

	return replica
}
