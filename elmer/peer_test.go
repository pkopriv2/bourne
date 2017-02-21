package elmer

// import (
	// "testing"
	// "time"
//
	// "github.com/pkopriv2/bourne/common"
	// "github.com/pkopriv2/bourne/kayak"
	// "github.com/pkopriv2/bourne/net"
	// "github.com/stretchr/testify/assert"
// )
//
// func TestPeer_Start(t *testing.T) {
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
	// _, err = newPeer(ctx, raw, net.NewTcpNetwork(), ":0")
	// assert.Nil(t, err)
// }
//
// func TestPeer_Catalog_Get_NoExist(t *testing.T) {
	// conf := common.NewConfig(map[string]interface{}{
		// "bourne.log.level": int(common.Debug),
	// })
//
	// ctx := common.NewContext(conf)
	// defer ctx.Close()
//
	// timer := ctx.Timer(10*time.Second)
	// defer timer.Close()
//
	// peer := NewTestPeer(ctx)
//
	// catalog, err := peer.Catalog()
	// assert.Nil(t, err)
//
	// store, err := catalog.Get(timer.Closed(), []byte("store"))
	// assert.Nil(t, err)
	// assert.Nil(t, store)
// }
//
// func TestPeer_Catalog_Ensure(t *testing.T) {
	// conf := common.NewConfig(map[string]interface{}{
		// "bourne.log.level": int(common.Debug),
	// })
//
	// ctx := common.NewContext(conf)
	// defer ctx.Close()
//
	// timer := ctx.Timer(10*time.Second)
	// defer timer.Close()
//
	// peer := NewTestPeer(ctx)
//
	// catalog, err := peer.Catalog()
	// assert.Nil(t, err)
//
	// store, err := catalog.Ensure(timer.Closed(), []byte("store"))
	// assert.Nil(t, err)
	// assert.NotNil(t, store)
// }
//
// func TestPeer_Catalog_Store_Get_NoExist(t *testing.T) {
	// conf := common.NewConfig(map[string]interface{}{
		// "bourne.log.level": int(common.Debug),
	// })
//
	// ctx := common.NewContext(conf)
	// defer ctx.Close()
//
	// timer := ctx.Timer(10*time.Second)
	// defer timer.Close()
//
	// peer := NewTestPeer(ctx)
//
	// catalog, err := peer.Catalog()
	// assert.Nil(t, err)
//
	// store, err := catalog.Ensure(timer.Closed(), []byte("store"))
	// assert.Nil(t, err)
//
	// _, ok, err := store.Get(timer.Closed(), []byte("key"))
	// assert.Nil(t, err)
	// assert.False(t, ok)
// }
//
// func TestPeer_Catalog_StorePut_NoExist(t *testing.T) {
	// conf := common.NewConfig(map[string]interface{}{
		// "bourne.log.level": int(common.Debug),
	// })
//
	// ctx := common.NewContext(conf)
	// defer ctx.Close()
//
	// timer := ctx.Timer(10*time.Second)
	// defer timer.Close()
//
	// peer := NewTestPeer(ctx)
//
	// catalog, err := peer.Catalog()
	// assert.Nil(t, err)
//
	// store, err := catalog.Ensure(timer.Closed(), []byte("store"))
	// assert.Nil(t, err)
//
	// item, ok, err := store.Put(timer.Closed(), []byte("key"), []byte("val"), 0)
	// assert.Nil(t, err)
	// assert.True(t, ok)
	// assert.Equal(t, []byte("val"), item.Val)
// }
//
// func NewTestPeer(ctx common.Context) *peer {
	// timer := ctx.Timer(30 * time.Second)
	// defer timer.Close()
//
	// raw, err := kayak.StartTestHost(ctx)
	// if err != nil {
		// panic(err)
	// }
//
	// kayak.ElectLeader(timer.Closed(), []kayak.Host{raw})
//
	// peer, err := newPeer(ctx, raw, net.NewTcpNetwork(), ":0")
	// if err != nil {
		// panic(err)
	// }
	// return peer
// }
