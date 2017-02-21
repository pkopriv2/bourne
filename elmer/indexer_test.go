package elmer

import (
	"testing"
	"time"

	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/kayak"
	"github.com/stretchr/testify/assert"
)

// func TestIndexer_SmokeTest(t *testing.T) {
// conf := common.NewConfig(map[string]interface{}{
// "bourne.log.level": int(common.Debug),
// })
//
// ctx := common.NewContext(conf)
// defer ctx.Close()
//
// timer := ctx.Timer(10 * time.Second)
// defer timer.Close()
//
// raw, err := kayak.StartTestHost(ctx)
// assert.Nil(t, err)
//
// kayak.ElectLeader(timer.Closed(), []kayak.Host{raw})
//
// indexer, err := newIndexer(ctx, raw, 10)
// assert.Nil(t, err)
//
// ok, err := indexer.StoreExists(timer.Closed(), emptyPath.Sub([]byte{1}, 0))
// assert.Nil(t, err)
// assert.False(t, ok)
// }
//
//
func TestIndexer_StoreCreate(t *testing.T) {
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

	ok, err := indexer.StoreEnableOrCreate(timer.Closed(), emptyPath.Child([]byte{0}, 0))
	assert.Nil(t, err)
	assert.True(t, ok)

	// ok, err := indexer.StoreExists(timer.Closed(), store)
	// assert.Nil(t, err)
	// assert.True(t, ok)
}

//
// func TestIndexer_StoreSwapItem(t *testing.T) {
// conf := common.NewConfig(map[string]interface{}{
// "bourne.log.level": int(common.Debug),
// })
//
// ctx := common.NewContext(conf)
// defer ctx.Close()
//
// timer := ctx.Timer(10 * time.Second)
// defer timer.Close()
//
// raw, err := kayak.StartTestHost(ctx)
// assert.Nil(t, err)
//
// kayak.ElectLeader(timer.Closed(), []kayak.Host{raw})
//
// indexer, err := newIndexer(ctx, raw, 10)
// assert.Nil(t, err)
//
// err := indexer.StoreCreate(timer.Closed(), emptyPath.Sub([]byte{0}, 0))
// assert.Nil(t, store)
//
// item1, ok1, err := indexer.StoreItemSwap(timer.Closed(), store, Item{[]byte("key"), []byte("val"), 0, false})
// assert.Nil(t, err)
// assert.True(t, ok1)
// assert.Equal(t, item1.Ver, 1)
//
// item2, ok2, err := indexer.StoreItemSwap(timer.Closed(), store, Item{[]byte("key"), []byte("val2"), 1, false})
// assert.Nil(t, err)
// assert.True(t, ok2)
// assert.Equal(t, item2.Ver, 2)
// }

//
// func TestIndexer_StoreGet(t *testing.T) {
// conf := common.NewConfig(map[string]interface{}{
// "bourne.log.level": int(common.Debug),
// })
//
// ctx := common.NewContext(conf)
// defer ctx.Close()
//
// timer := ctx.Timer(10 * time.Second)
// defer timer.Close()
//
// raw, err := kayak.StartTestHost(ctx)
// assert.Nil(t, err)
//
// kayak.ElectLeader(timer.Closed(), []kayak.Host{raw})
//
// indexer, err := newIndexer(ctx, raw, 10)
// assert.Nil(t, err)
//
// item1, ok1, err := indexer.StoreSwapItem(timer.Closed(), []byte("store"), []byte("key"), []byte("val"), 0)
// assert.Nil(t, err)
// assert.True(t, ok1)
// assert.Equal(t, item1.Ver, 1)
//
// item2, ok2, err := indexer.StoreReadItem(timer.Closed(), []byte("store"), []byte("key"))
// assert.Nil(t, err)
// assert.True(t, ok2)
// assert.Equal(t, item1, item2)
// }
//
// func TestIndexer_Stuff(t *testing.T) {
// conf := common.NewConfig(map[string]interface{}{
// "bourne.log.level": int(common.Debug),
// })
//
// ctx := common.NewContext(conf)
// defer ctx.Close()
//
// timer := ctx.Timer(30 * time.Second)
// defer timer.Close()
//
// raw, err := kayak.StartTestHost(ctx)
// assert.Nil(t, err)
//
// kayak.ElectLeader(timer.Closed(), []kayak.Host{raw})
//
// indexer, err := newIndexer(ctx, raw, 10)
// assert.Nil(t, err)
//
// for i := 0; i < 10240; i++ {
// indexer.StoreSwapItem(timer.Closed(), []byte("store"), stash.IntBytes(i), stash.IntBytes(i), 0)
// }
// }
