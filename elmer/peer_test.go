package elmer

import (
	"testing"
	"time"

	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/kayak"
	"github.com/stretchr/testify/assert"
)

func TestPeer_Start(t *testing.T) {
	conf := common.NewConfig(map[string]interface{}{
		"bourne.log.level": int(common.Debug),
	})

	ctx := common.NewContext(conf)
	defer ctx.Close()

	timer := ctx.Timer(10 * time.Second)
	defer timer.Close()

	raw, err := kayak.StartTestHost(ctx)
	assert.Nil(t, err)

	peer, err := Start(ctx, raw, ":0")
	assert.Nil(t, err)

	store, err := peer.Root()
	assert.Nil(t, err)

	item, ok, err := store.Put(timer.Closed(), []byte("key"), []byte("val"), -1)
	assert.Nil(t, err)
	assert.True(t, ok)
	assert.Equal(t, []byte("key"), item.Key)
}
