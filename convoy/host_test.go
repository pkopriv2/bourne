package convoy

import (
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/net"
	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
)

// TODO: Figure out how to randomize ports!!!

func TestHost_Close(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	host := StartTestSeedHost(ctx, 0)

	assert.Nil(t, host.Close())
	assert.NotNil(t, host.Close())
}

func TestHost_Leave(t *testing.T) {
	conf := common.NewConfig(map[string]interface{}{
		"bourne.log.level": int(common.Info),
	})

	ctx := common.NewContext(conf)
	defer ctx.Close()

	hosts := StartTestHostCluster(ctx, 2)
	assert.Equal(t, 2, len(hosts))

	idx := rand.Intn(len(hosts))
	host := hosts[idx]
	assert.Nil(t, host.Leave())
	assert.Equal(t, ClosedError, host.Leave())
	assert.Equal(t, ClosedError, host.Leave())
}

func TestHost_Failed(t *testing.T) {
	conf := common.NewConfig(map[string]interface{}{
		"bourne.log.level": int(common.Info),
	})

	ctx := common.NewContext(conf)
	defer ctx.Close()

	hosts := StartTestHostCluster(ctx, 100)
	assert.Equal(t, 100, len(hosts))

	idx := rand.Intn(len(hosts))
	failed := hosts[idx]

	for i:=0; i<10; i++ {
		r := <-failed.inst
		r.Server.Close()

		r.Logger.Info("Sleeping")
		time.Sleep(3*time.Second)
		r.Logger.Info("Done Sleeping")

		SyncHostCluster(hosts, func(h *host) bool {
			dir, err := h.Directory()
			if err != nil {
				panic(err)
			}

			all, err := dir.All()
			if err != nil {
				panic(err)
			}

			return len(all) == len(hosts)
		})

	}
}

func removeHost(cluster []*replica, i int) []*replica {
	return append(cluster[:i], cluster[i+1:]...)
}

func StartTestHostCluster(ctx common.Context, num int) []*host {
	seeder := StartTestSeedHost(ctx, 9190)

	cluster := []*host{seeder}
	for i := num - 1; i > 0; i-- {
		h := StartTestMemberHost(ctx, 9190+i, 9190)
		cluster = append(cluster, h)
	}

	SyncHostCluster(cluster, func(h *host) bool {
		dir, err := h.Directory()
		if err != nil {
			panic(err)
		}

		all, err := dir.All()
		if err != nil {
			panic(err)
		}

		return len(all) == len(cluster)
	})

	seeder.logger.Info("Successfully started cluster of [%v] hosts", len(cluster))

	return cluster
}

func SyncHostCluster(cluster []*host, fn func(r *host) bool) {
	done := make(map[uuid.UUID]struct{})
	start := time.Now()

	cluster[0].logger.Info("Syncing cluster [%v]", len(cluster))
	for len(done) < len(cluster) {
		cluster[0].logger.Info("Number of sync'ed: %v", len(done))
		for _, r := range cluster {
			id := r.Id()
			if _, ok := done[id]; ok {
				continue
			}

			if fn(r) {
				done[id] = struct{}{}
				continue
			}

			if time.Now().Sub(start) > 10*time.Second {
				r.logger.Info("Still not sync'ed")
			}
		}
		<-time.After(250 * time.Millisecond)
	}
	cluster[0].logger.Info("Number of sync'ed: %v", len(done))
}

func StartTestSeedHost(ctx common.Context, port int) *host {
	return StartTestHostFromDb(ctx, OpenTestDatabase(ctx, OpenTestChangeLog(ctx)), port, "")
}

func StartTestMemberHost(ctx common.Context, selfPort int, peerPort int) *host {
	return StartTestHostFromDb(ctx, OpenTestDatabase(ctx, OpenTestChangeLog(ctx)), selfPort, net.NewAddr("localhost", strconv.Itoa(peerPort)))
}

func StartTestHostFromDb(ctx common.Context, db *database, port int, peer string) *host {
	host, err := newHost(ctx, db, "localhost", port, peer)
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
		host.Shutdown()
	})

	return host
}
