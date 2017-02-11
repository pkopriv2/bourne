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

	hosts, err := StartTestCluster(ctx, 8)
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

	hosts, err := StartTestCluster(ctx, 8)
	assert.Nil(t, err)

	fails := make([]Listener, 0, len(hosts))
	for _, h := range hosts {
		dir, err := h.Directory()
		assert.Nil(t, err)

		l, err := dir.Failures()
		assert.Nil(t, err)

		fails = append(fails, l)
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

	SyncCluster(timer.Closed(), hosts, func(h Host) bool {
		dir, err := h.Directory()
		if err != nil {
			return false
		}

		timer := ctx.Timer(30*time.Second)
		defer timer.Close()

		all, err := dir.All(timer.Closed())
		if err != nil {
			return false
		}

		for _, m := range all {
			if m.Id() == self0.Id() && m.Version() > self0.Version() {
				return true
			}
		}
		return false
	})

	assert.False(t, timer.IsClosed())
}

// func TestHost_Store_Put(t *testing.T) {
	// conf := common.NewConfig(map[string]interface{}{
		// "bourne.log.level": int(common.Debug),
	// })
//
	// ctx := common.NewContext(conf)
	// defer ctx.Close()
//
	// hosts, err := StartTestCluster(ctx, 8)
	// assert.Nil(t, err)
//
	// store0, err := hosts[0].Store()
	// assert.Nil(t, err)
//
	// ok, item, err := store0.Put("key", "val", 0)
	// assert.True(t, ok)
//
	// timer := ctx.Timer(10 * time.Second)
	// defer timer.Close()
//
	// SyncCluster(timer.Closed(), hosts, func(h Host) bool {
		// dir, err := h.Directory()
		// if err != nil {
			// return false
		// }
		// return false
	// })
//
	// assert.False(t, timer.IsClosed())
// }
