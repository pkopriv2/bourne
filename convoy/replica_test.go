package convoy

import (
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/pkopriv2/bourne/common"
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
	members := replica.Collect(func(key string, val string) bool {
		return true
	})

	assert.Equal(t, []*member{replica.Self}, members)
}

func TestReplica_Init_NonEmptyDb(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	db := OpenTestDatabase(ctx, OpenTestChangeLog(ctx))
	db.Log().Append("key", "val", false)

	replica := StartTestReplicaFromDb(ctx, db, 0)
	defer replica.Close()

	// make sure self exists
	members := replica.Collect(func(key string, val string) bool {
		return key == "key"
	})

	assert.Equal(t, []*member{replica.Self}, members)
}

func TestReplica_Dir_Indexing(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	replica := StartTestReplica(ctx, 0)
	defer replica.Close()

	replica.Db.Put("key2", "val")
	time.Sleep(time.Millisecond)

	// make sure that are
	members := replica.Collect(func(key string, val string) bool {
		return key == "key2"
	})

	assert.Equal(t, []*member{replica.Self}, members)
}

//
// func TestReplica_Join_TwoPeers(t *testing.T) {
// ctx := common.NewContext(common.NewEmptyConfig())
// defer ctx.Close()
//
// replica1 := StartTestReplica(ctx, 0)
// replica2 := StartTestReplica(ctx, 0)
// defer replica1.Close()
// defer replica2.Close()
//
// client1 := ReplicaClient(replica1)
// client2 := ReplicaClient(replica2)
// defer client1.Close()
// defer client2.Close()
//
// err := JoinReplica(replica2, client1)
// if err != nil {
// panic(err)
// }
//
// members1 := replica1.Collect(func(id uuid.UUID, key string, val string) bool {
// return true
// })
//
// members2 := replica2.Collect(func(id uuid.UUID, key string, val string) bool {
// return true
// })
//
// assert.Equal(t, []*member{replica1.Self, replica2.Self}, members1)
// assert.Equal(t, []*member{replica1.Self, replica2.Self}, members2)
// }

// func TestReplica_Join_ThreePeers(t *testing.T) {
// ctx := common.NewContext(common.NewEmptyConfig())
// defer ctx.Close()
//
// replica1 := StartTestReplica(ctx, 8190)
// replica2 := StartTestReplica(ctx, 8191)
// replica3 := StartTestReplica(ctx, 8192)
// defer replica1.Close()
// defer replica2.Close()
// defer replica3.Close()
//
// client1 := ReplicaClient(replica1)
// client2 := ReplicaClient(replica2)
// client3 := ReplicaClient(replica3)
// defer client1.Close()
// defer client2.Close()
// defer client3.Close()
//
// err := replicaJoin(replica2, client1)
// if err != nil {
// panic(err)
// }
//
// err = replicaJoin(replica3, client1)
// if err != nil {
// panic(err)
// }
//
// time.Sleep(7 * time.Second)
//
// members1 := replica1.Collect(func(id uuid.UUID, key string, val string) bool {
// return true
// })
//
// members2 := replica2.Collect(func(id uuid.UUID, key string, val string) bool {
// return true
// })
//
// members3 := replica3.Collect(func(id uuid.UUID, key string, val string) bool {
// return true
// })
//
// fmt.Println("MEMBER1: ", members1)
// fmt.Println("MEMBER2: ", members2)
// fmt.Println("MEMBER3: ", members3)
//
// // assert.Equal(t, []*member{replica1.Self, replica2.Self}, members)
/* } */

// func TestReplica_Managers(t *testing.T) {
// ctx := common.NewContext(common.NewEmptyConfig())
// defer ctx.Close()
//
// replica1 := StartTestReplica(ctx, 8190)
// replica1.Manager(true)
//
// replica2 := StartTestReplica(ctx, 8191)
// replica2.Join(ReplicaClient(replica1))
//
// <-time.After(5 * time.Second)
// assert.Equal(t, []*member{replica1.Self}, replica2.Managers())
// }

func TestReplica_Join(t *testing.T) {
	conf := common.NewConfig(map[string]interface{}{
		"bourne.log.level": int(common.Error),
	})

	ctx := common.NewContext(conf)
	// defer ctx.Close()

	clusterSize := 128
	cluster := make([]*replica, 0, clusterSize)

	master := StartTestReplica(ctx, 8190)
	masterClient := ReplicaClient(master)

	cluster = append(cluster, master)

	joined := make(map[*member]struct{})
	joined[master.Self] = struct{}{}

	for i := 0; i < clusterSize-1; i++ {
		ri := StartTestReplica(ctx, 8191+i)

		JoinTestReplica(ri, masterClient)
		cluster = append(cluster, ri)
	}

	var wait sync.WaitGroup
	wait.Add(1)
	go func() {
		defer wait.Done()

		for len(joined) < len(cluster) {
			<-time.After(1 * time.Second)

			fmt.Println("COMPLETED: ", len(joined))

			for _, r := range cluster {
				members := r.Collect(func(key string, val string) bool {
					return true
				})

				if len(members) >= len(cluster) {
					joined[r.Self] = struct{}{}
				}
			}
		}
	}()

	fmt.Println("COMPLETED: ", joined)

	received := make(map[*member]struct{})
	received[master.Self] = struct{}{}

	numMessages := 1000

	wait.Wait()
	wait.Add(1)
	go func() {
		defer wait.Done()

		randomSize := int(math.Log(float64(numMessages)))
		random := make([]int, randomSize)
		for i := 0; i<randomSize; i++ {
			random[i] = rand.Intn(numMessages)
		}

		for len(received) < len(cluster) {
			<-time.After(1 * time.Second)

			for _, r := range cluster {
				if _, ok := received[r.Self]; ok {
					continue
				}

				found := make([]int, 0, len(random))
				r.Dir.View(func(d *dirView) {
					for i := range random {
						if _, _, ok := d.GetMemberAttr(master.Id(), strconv.Itoa(i)); ok {
							found = append(found, i)
						}
					}
				})

				if len(found) == len(random) {
					r.Logger.Error("RECEIVED!")
					received[r.Self] = struct{}{}
				}

			}
		}
	}()


	for j := 0; j < numMessages; j++ {
		master.Db.Put(strconv.Itoa(j), "sweeet")
	}



	wait.Wait()
	// fmt.Println("COMPLETED: ", received)
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
