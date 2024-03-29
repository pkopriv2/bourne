package convoy

import (
	"strconv"
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
	assert.Nil(t, host.Close())
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

	hosts, err := StartTestCluster(ctx, 16)
	assert.Nil(t, err)
	assert.Equal(t, 16, len(hosts))
}


func TestHost_Fail_Manual(t *testing.T) {
	conf := common.NewConfig(map[string]interface{}{
		"bourne.log.level": int(common.Debug),
	})

	ctx := common.NewContext(conf)
	defer ctx.Close()

	hosts, err := StartTestCluster(ctx, 8)
	assert.Nil(t, err)

	self0, err := hosts[0].Self()
	assert.Nil(t, err)

	dir1, err := hosts[1].Directory()
	assert.Nil(t, err)

	timer := ctx.Timer(5 * time.Second)
	defer timer.Close()
	assert.Nil(t, dir1.FailMember(timer.Closed(), self0))

	SyncCluster(timer.Closed(), hosts, func(h Host) bool {
		dir, err := h.Directory()
		if err != nil {
			return false
		}

		m, err := dir.GetMember(timer.Closed(), self0.Id())
		if err != nil {
			return false
		}

		if m.Id() == self0.Id() && m.Version() > self0.Version() {
			return true
		}
		return false
	})

	assert.False(t, timer.IsClosed())
}

func TestHost_Fail_Rejoin_Automatic(t *testing.T) {
	conf := common.NewConfig(map[string]interface{}{
		"bourne.log.level": int(common.Debug),
	})

	ctx := common.NewContext(conf)
	defer ctx.Close()

	hosts, err := StartTestCluster(ctx, 32)
	assert.Nil(t, err)

	self0, err := hosts[0].Self()
	assert.Nil(t, err)

	// shutdown the replica (requests should timeout)
	hosts[0].(*host).iface.Shutdown()

	timer := ctx.Timer(10 * time.Second)
	defer timer.Close()

	SyncCluster(timer.Closed(), hosts, func(h Host) bool {
		dir, err := h.Directory()
		if err != nil {
			return false
		}

		m, err := dir.GetMember(timer.Closed(), self0.Id())
		if err != nil {
			return false
		}

		if m.Id() == self0.Id() && m.Version() > self0.Version() {
			return true
		}
		return false
	})

	assert.False(t, timer.IsClosed())
}

func TestHost_Store_Put_Single(t *testing.T) {
	conf := common.NewConfig(map[string]interface{}{
		"bourne.log.level": int(common.Debug),
	})

	ctx := common.NewContext(conf)
	defer ctx.Close()

	hosts, err := StartTestCluster(ctx, 8)
	assert.Nil(t, err)

	self0, err := hosts[0].Self()
	assert.Nil(t, err)

	store0, err := hosts[0].Store()
	assert.Nil(t, err)

	timer := ctx.Timer(10 * time.Second)
	defer timer.Close()

	ok, item, err := store0.Put(timer.Closed(), "key", "val", 0)
	assert.True(t, ok)
	assert.Equal(t, Item{"val", 1, false}, item)
	assert.Nil(t, err)

	SyncCluster(timer.Closed(), hosts, func(h Host) bool {
		dir, err := h.Directory()
		if err != nil {
			return false
		}

		val, ok, err := dir.GetMemberValue(timer.Closed(), self0.Id(), "key")
		if !ok || err != nil {
			return false
		}

		return val == "val"
	})

	assert.False(t, timer.IsClosed())
}

func TestHost_Store_Put_Multi(t *testing.T) {
	conf := common.NewConfig(map[string]interface{}{
		"bourne.log.level": int(common.Debug),
	})

	ctx := common.NewContext(conf)
	defer ctx.Close()

	hosts, err := StartTestCluster(ctx, 32)
	assert.Nil(t, err)

	num := 100

	timer := ctx.Timer(30 * time.Second)
	defer timer.Closed()

	for _, h := range hosts {
		go func(h Host) {
			store, err := h.Store()
			if err != nil {
				t.FailNow()
			}

			for i := 0; i < num; i++ {
				h.(*host).logger.Info("Putting [%v, %v]", h.Id().String()[:8], i)
				store.Put(timer.Closed(), strconv.Itoa(i), "val", 0)
			}
		}(h)
	}

	SyncCluster(timer.Closed(), hosts, func(h Host) bool {
		dir, err := h.Directory()
		if err != nil {
			return false
		}

		for _, h := range hosts {
			for i := 0; i < num; i++ {
				_, ok, err := dir.GetMemberValue(timer.Closed(), h.Id(), strconv.Itoa(i))
				if !ok || err != nil {
					return false
				}
			}
		}

		return true
	})

	assert.False(t, timer.IsClosed())
}

func TestHost_ListenRoster_Join(t *testing.T) {
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

	roster, err := dir.ListenRoster()
	assert.Nil(t, err)

	host2, err := JoinTestHost(ctx, ":0", []string{self1.Addr()})
	assert.Nil(t, err)

	timer := ctx.Timer(5 * time.Second)
	defer timer.Close()
	select {
	case <-timer.Closed():
		assert.Fail(t, "Failed to receive join notification")
	case m := <-roster.Data():
		assert.Equal(t, host2.Id(), m.Id)
	}
}

func TestHost_ListenRoster_Evict(t *testing.T) {
	conf := common.NewConfig(map[string]interface{}{
		"bourne.log.level": int(common.Debug),
	})

	ctx := common.NewContext(conf)
	defer ctx.Close()

	hosts, err := StartTestCluster(ctx, 3)
	assert.Nil(t, err)

	self0, err := hosts[0].Self()
	assert.Nil(t, err)

	dir1, err := hosts[1].Directory()
	assert.Nil(t, err)

	dir2, err := hosts[2].Directory()
	assert.Nil(t, err)

	roster2, err := dir2.ListenRoster()
	assert.Nil(t, err)

	timer := ctx.Timer(10 * time.Second)
	defer timer.Close()

	assert.Nil(t, dir1.EvictMember(timer.Closed(), self0))
	select {
	case <-timer.Closed():
		assert.Fail(t, "Failed to receive join notification")
	case m := <-roster2.Data():
		assert.Equal(t, self0.Id(), m.Id)
	}
}

func TestHost_ListenHealth_Fail(t *testing.T) {
	conf := common.NewConfig(map[string]interface{}{
		"bourne.log.level": int(common.Debug),
	})

	ctx := common.NewContext(conf)
	defer ctx.Close()

	hosts, err := StartTestCluster(ctx, 3)
	assert.Nil(t, err)

	self0, err := hosts[0].Self()
	assert.Nil(t, err)

	dir1, err := hosts[1].Directory()
	assert.Nil(t, err)

	dir2, err := hosts[2].Directory()
	assert.Nil(t, err)

	health2, err := dir2.ListenHealth()
	assert.Nil(t, err)

	timer := ctx.Timer(10 * time.Second)
	defer timer.Close()

	assert.Nil(t, dir1.FailMember(timer.Closed(), self0))
	select {
	case <-timer.Closed():
		assert.Fail(t, "Failed to receive join notification")
	case m := <-health2.Data():
		assert.Equal(t, self0.Id(), m.Id)
	}
}
