package convoy

// func TestIndexer_Close(t *testing.T) {
// ctx := common.NewContext(common.NewEmptyConfig())
// idx := NewTestIndexer(ctx)
// assert.Nil(t, idx.Close())
// }
//
// func TestIndex_Event(t *testing.T) {
// ctx := common.NewContext(common.NewEmptyConfig())
// idx := NewTestIndexer(ctx)
// defer idx.Close()
//
// id, _ := idx.log.Id()
// idx.log.Append("key", "val", 0, false)
//
// time.Sleep(10 * time.Millisecond)
//
// idx.dir.View(func(v *view) {
// val, ver, ok := v.GetMemberAttr(id, "key")
// assert.Equal(t, "val", val)
// assert.Equal(t, 0, ver)
// assert.True(t, ok)
// })
// }
//
// func NewTestIndexer(ctx common.Context) *indexer {
// dir := newDirectory(ctx)
// ctx.Env().OnClose(func() {
// dir.Close()
// })
//
// log := OpenTestChangeLog(ctx)
// idx, err := newIndexer(ctx, log, dir)
// if err != nil {
// panic(err)
// }
//
// return idx
// }
