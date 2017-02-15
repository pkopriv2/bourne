package elmer

import (
	"testing"
	"time"

	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/kayak"
	"github.com/stretchr/testify/assert"
)

func TestIndexer_SmokeTest(t *testing.T) {
	conf := common.NewConfig(map[string]interface{}{
		"bourne.log.level": int(common.Debug),
	})

	ctx := common.NewContext(conf)
	defer ctx.Close()

	timer := ctx.Timer(10 * time.Second)
	defer timer.Close()

	raw, err := kayak.StartTestHost(ctx)
	assert.Nil(t, err)

	kayak.ElectLeader(timer.Closed(), []kayak.Host{raw})

	indexer, err := newIndexer(ctx, raw, 10)
	assert.Nil(t, err)

	// indexer.SwapItem(timer.Closed(), []byte("store"), []byte("key"), []byte)

	_, ok, err := indexer.StoreReadItem(timer.Closed(), []byte("store"), []byte("key"))
	indexer.logger.Info("Err: %+v", err)

	assert.Nil(t, err)
	assert.False(t, ok)
}
