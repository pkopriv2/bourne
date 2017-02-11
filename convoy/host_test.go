package convoy

import (
	"testing"

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
