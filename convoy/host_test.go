package convoy

import (
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/concurrent"
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

	hosts := StartTestHostCluster(ctx, 16)

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

	hosts := StartTestHostCluster(ctx, 16)

	idx := rand.Intn(len(hosts))
	failed := hosts[idx]

	for i := 0; i < 3; i++ {
		r := <-failed.inst
		r.Server.Close()

		r.Logger.Info("Sleeping")
		time.Sleep(3 * time.Second)
		r.Logger.Info("Done Sleeping")

		done, timeout := concurrent.NewBreaker(10*time.Second, func() {
			SyncHostCluster(hosts, func(h *host) bool {
				all, err := h.Directory().All()
				if err != nil {
					panic(err)
				}

				return len(all) == len(hosts)
			})
		})

		select {
		case <-done:
		case <-timeout:
			assert.Fail(t, "Timed out waiting for member to rejoined")
		}
	}
}

func TestHost_Update_All(t *testing.T) {
	conf := common.NewConfig(map[string]interface{}{
		"bourne.log.level": int(common.Info),
	})

	ctx := common.NewContext(conf)
	defer ctx.Close()

	hosts := StartTestHostCluster(ctx, 16)

	for _, h := range hosts {
		h.logger.Info("Writing key,val")
		h.Store().Put("key", "val", 0)
	}

	done, timeout := concurrent.NewBreaker(10*time.Second, func() {
		SyncHostCluster(hosts, func(h *host) bool {
			found, _ := h.Directory().Search(func(id uuid.UUID, key string, val string) bool {
				if key == "key" {
					return true
				}
				return false
			})

			return len(found) == len(hosts)
		})
	})

	select {
	case <-done:
	case <-timeout:
		assert.Fail(t, "Timed out waiting for member to rejoined")
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
		all, err := h.Directory().All()
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
		panic(fmt.Sprintf("%+v", err))
	}

	ctx.Control().Defer(func(error) {
		host.Shutdown()
		db.Close()
		db.Log().stash.Close()
	})

	return host
}
