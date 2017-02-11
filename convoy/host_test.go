package convoy

import (
	"testing"
	"time"

	"github.com/pkopriv2/bourne/common"
	"github.com/stretchr/testify/assert"
)

func TestHost_Close(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()
	host, err := StartTestHost(ctx, ":0")
	assert.Nil(t, err)
	assert.Nil(t, host.Close())
	assert.NotNil(t, host.Close())
}

func TestHost_Join_Two_Peers(t *testing.T) {
	conf := common.NewConfig(map[string]interface{}{
		"bourne.log.level": int(common.Debug),
	})

	ctx := common.NewContext(conf)
	defer ctx.Close()

	hosts, err := StartTestCluster(ctx, 2)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(hosts))
}

func TestHost_Join_Three_Peers(t *testing.T) {
	conf := common.NewConfig(map[string]interface{}{
		"bourne.log.level": int(common.Debug),
	})

	ctx := common.NewContext(conf)
	defer ctx.Close()

	hosts, err := StartTestCluster(ctx, 3)
	assert.Nil(t, err)
	assert.Equal(t, 3, len(hosts))
}

func TestHost_Join_Many_Peers(t *testing.T) {
	conf := common.NewConfig(map[string]interface{}{
		"bourne.log.level": int(common.Info),
	})

	ctx := common.NewContext(conf)
	defer ctx.Close()

	hosts, err := StartTestCluster(ctx, 32)
	assert.Nil(t, err)
	assert.Equal(t, 32, len(hosts))
}

func TestHost_Join_Listener(t *testing.T) {
	conf := common.NewConfig(map[string]interface{}{
		"bourne.log.level": int(common.Debug),
	})

	ctx := common.NewContext(conf)
	defer ctx.Close()

	host1, err := StartTestHost(ctx, ":0")
	assert.Nil(t, err)

	self1, err := host1.Self()
	assert.Nil(t, err)

	dir, err := host1.Directory()
	assert.Nil(t, err)
	joins, err := dir.Joins()

	host2, err := JoinTestHost(ctx, ":0", []string{self1.Addr()})
	assert.Nil(t, err)

	timer := ctx.Timer(5 * time.Second)
	defer timer.Close()
	select {
	case <-timer.Closed():
		assert.Fail(t, "Failed to receive join notification")
	case id := <-joins.Data():
		assert.Equal(t, host2.Id(), id)
	}
}

func TestHost_Evict_Listener(t *testing.T) {
	conf := common.NewConfig(map[string]interface{}{
		"bourne.log.level": int(common.Debug),
	})

	ctx := common.NewContext(conf)
	defer ctx.Close()

	hosts, err := StartTestCluster(ctx, 32)
	assert.Nil(t, err)

	lists := make([]Listener, 0, len(hosts))
	for _, h := range hosts {
		dir, err := h.Directory()
		assert.Nil(t, err)

		evicts, err := dir.Evictions()
		assert.Nil(t, err)

		lists = append(lists, evicts)
	}

	self0, err := hosts[0].Self()
	assert.Nil(t, err)

	dir1, err := hosts[1].Directory()
	assert.Nil(t, err)

	timer := ctx.Timer(10 * time.Second)
	defer timer.Close()

	assert.Nil(t, dir1.Evict(timer.Closed(), self0))

	for _, l := range lists[1:] {
		select {
		case <-timer.Closed():
			assert.Fail(t, "Failed to receive evict notification")
		case id := <-l.Data():
			assert.Equal(t, self0.Id(), id)
		}
	}

	select {
	case <-timer.Closed():
		assert.Fail(t, "Failed to receive evict notification")
	case <-lists[0].Ctrl().Closed():
	}
}

func TestHost_Fail_Listener(t *testing.T) {
	conf := common.NewConfig(map[string]interface{}{
		"bourne.log.level": int(common.Debug),
	})

	ctx := common.NewContext(conf)
	defer ctx.Close()

	hosts, err := StartTestCluster(ctx, 3)
	assert.Nil(t, err)

	lists := make([]Listener, 0, len(hosts))
	for _, h := range hosts {
		dir, err := h.Directory()
		assert.Nil(t, err)

		l, err := dir.Failures()
		assert.Nil(t, err)

		lists = append(lists, l)
	}

	self0, err := hosts[0].Self()
	assert.Nil(t, err)

	dir1, err := hosts[1].Directory()
	assert.Nil(t, err)

	timer := ctx.Timer(10 * time.Second)
	defer timer.Close()

	assert.Nil(t, dir1.Fail(timer.Closed(), self0))
	for _, l := range lists[1:] {
		select {
		case <-timer.Closed():
			assert.Fail(t, "Failed to receive evict notification")
		case id := <-l.Data():
			assert.Equal(t, self0.Id(), id)
		}
	}

	assert.False(t, lists[0].Ctrl().IsClosed())
	assert.False(t, hosts[0].(*host).ctrl.IsClosed())
}

func TestHost_Fail_Rejoin(t *testing.T) {
	conf := common.NewConfig(map[string]interface{}{
		"bourne.log.level": int(common.Debug),
	})

	ctx := common.NewContext(conf)
	defer ctx.Close()

	hosts, err := StartTestCluster(ctx, 3)
	assert.Nil(t, err)

	fails := make([]Listener, 0, len(hosts))
	for _, h := range hosts {
		dir, err := h.Directory()
		assert.Nil(t, err)

		l, err := dir.Failures()
		assert.Nil(t, err)

		fails = append(fails, l)
	}

	joins := make([]Listener, 0, len(hosts))
	for _, h := range hosts {
		dir, err := h.Directory()
		assert.Nil(t, err)

		l, err := dir.Joins()
		assert.Nil(t, err)

		joins = append(joins, l)
	}

	self0, err := hosts[0].Self()
	assert.Nil(t, err)

	dir1, err := hosts[1].Directory()
	assert.Nil(t, err)

	timer := ctx.Timer(5 * time.Second)
	defer timer.Close()
	assert.Nil(t, dir1.Fail(timer.Closed(), self0))

	for _, l := range fails[1:] {
		select {
		case <-timer.Closed():
			assert.Fail(t, "Failed to receive evict notification")
			return
		case id := <-l.Data():
			assert.Equal(t, self0.Id(), id)
		}
	}

	for _, l := range joins[1:] {
		for {
			select {
			case <-timer.Closed():
				assert.Fail(t, "Failed to receive join notification")
				return
			case id := <-l.Data():
				if self0.Id() == id {
					break
				}
			}
		}
	}

	assert.False(t, fails[0].Ctrl().IsClosed())
	assert.False(t, joins[0].Ctrl().IsClosed())
	assert.False(t, hosts[0].(*host).ctrl.IsClosed())
}
// func TestHost_Failed(t *testing.T) {
// conf := common.NewConfig(map[string]interface{}{
// "bourne.log.level": int(common.Info),
// })
//
// ctx := common.NewContext(conf)
// defer ctx.Close()
//
// hosts := StartTestHostCluster(ctx, 16)
//
// idx := rand.Intn(len(hosts))
// failed := hosts[idx]
//
// for i := 0; i < 3; i++ {
// r := <-failed.inst
// r.Server.Close()
//
// r.Logger.Info("Sleeping")
// time.Sleep(3 * time.Second)
// r.Logger.Info("Done Sleeping")
//
// done, timeout := concurrent.NewBreaker(10*time.Second, func() {
// SyncHostCluster(hosts, func(h *host) bool {
// all, err := h.Directory().All()
// if err != nil {
// panic(err)
// }
//
// return len(all) == len(hosts)
// })
// })
//
// select {
// case <-done:
// case <-timeout:
// assert.Fail(t, "Timed out waiting for member to rejoined")
// }
// }
// }
//
// func TestHost_Update_All(t *testing.T) {
// conf := common.NewConfig(map[string]interface{}{
// "bourne.log.level": int(common.Info),
// })
//
// ctx := common.NewContext(conf)
// defer ctx.Close()
//
// hosts := StartTestHostCluster(ctx, 16)
//
// for _, h := range hosts {
// h.logger.Info("Writing key,val")
// h.Store().Put("key", "val", 0)
// }
//
// done, timeout := concurrent.NewBreaker(10*time.Second, func() {
// SyncHostCluster(hosts, func(h *host) bool {
// found, _ := h.Directory().Search(func(id uuid.UUID, key string, val string) bool {
// if key == "key" {
// return true
// }
// return false
// })
//
// return len(found) == len(hosts)
// })
// })
//
// select {
// case <-done:
// case <-timeout:
// assert.Fail(t, "Timed out waiting for member to rejoined")
// }
// }
//
