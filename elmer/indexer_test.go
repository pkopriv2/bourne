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

	ok, err := indexer.StoreExists(timer.Closed(), []byte("store"))
	assert.Nil(t, err)
	assert.False(t, ok)
}


func TestIndexer_StoreEnsure(t *testing.T) {
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
	assert.Nil(t, indexer.StoreEnsure(timer.Closed(), []byte("store")))
}

func TestIndexer_StoreSwap(t *testing.T) {
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

	item1, ok1, err := indexer.StoreSwapItem(timer.Closed(), []byte("store"), []byte("key"), []byte("val"), 0)
	assert.Nil(t, err)
	assert.True(t, ok1)
	assert.Equal(t, item1.Ver, 1)

	item2, ok2, err := indexer.StoreSwapItem(timer.Closed(), []byte("store"), []byte("key"), []byte("val2"), 1)
	assert.Nil(t, err)
	assert.True(t, ok2)
	assert.Equal(t, item2.Ver, 2)
}

func TestIndexer_StoreGet(t *testing.T) {
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

	item1, ok1, err := indexer.StoreSwapItem(timer.Closed(), []byte("store"), []byte("key"), []byte("val"), 0)
	assert.Nil(t, err)
	assert.True(t, ok1)
	assert.Equal(t, item1.Ver, 1)

	item2, ok2, err := indexer.StoreReadItem(timer.Closed(), []byte("store"), []byte("key"))
	assert.Nil(t, err)
	assert.True(t, ok2)
	assert.Equal(t, item1, item2)
}
