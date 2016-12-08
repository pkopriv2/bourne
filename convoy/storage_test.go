package convoy

import (
	"testing"

	"github.com/pkopriv2/bourne/common"
	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
)

func TestStorage_Close(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	core := newStorage(ctx, ctx.Logger())
	assert.Nil(t, core.Close())
}

func TestStorage_Join_NotJoined(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	core := newStorage(ctx, ctx.Logger())
	defer core.Close()

	id := uuid.NewV1()
	core.Update(func(u *update) {
		assert.True(t, u.Join(id, 0))
	})

	core.View(func(v *view) {
		s, found := v.Status(id)
		assert.True(t, found)
		assert.True(t, s.Enabled)
		assert.Equal(t, 0, s.Version)


		i, ok := v.Get(id, memberEnabledAttr)
		assert.Equal(t, true, ok)
		assert.Equal(t, "true", i.Val)
	})
}

//
// func TestStorage_UpdateMember(t *testing.T) {
// ctx := common.NewContext(common.NewEmptyConfig())
// core := newStorage(ctx, ctx.Logger())
// defer core.Close()
//
// member := newMember(uuid.NewV1(), "host", "0", 1, Alive)
// core.update(func(u *coreUpdate) {
// u.AddMember(member)
// u.UpdateMember(member.Id, Failed, 2)
// })
//
// val := *member
// val.Status = Failed
// val.Version = 2
// core.View(func(v *coreView) {
// assert.Equal(t, &val, v.GetMember(member.Id))
// })
// }
//
// func TestStorage_GetAttr_NoExist(t *testing.T) {
// ctx := common.NewContext(common.NewEmptyConfig())
// core := newStorage(ctx, ctx.Logger())
// defer core.Close()
//
// member := newMember(uuid.NewV1(), "host", "0", 1, Alive)
// core.update(func(u *coreUpdate) {
// u.AddMember(member)
// })
//
// core.View(func(v *coreView) {
// _, _, found := v.Get(member.Id, "attr")
// assert.False(t, found)
// })
// }
//
// func TestStorage_DelAttr_NoExist(t *testing.T) {
// ctx := common.NewContext(common.NewEmptyConfig())
// core := newStorage(ctx, ctx.Logger())
// defer core.Close()
//
// member := newMember(uuid.NewV1(), "host", "0", 1, Alive)
// core.update(func(u *coreUpdate) {
// u.AddMember(member)
// u.Del(member.Id, "attr", 1)
// })
//
// core.View(func(v *coreView) {
// _, _, found := v.Get(member.Id, "attr")
// assert.False(t, found)
// })
// }
//
// func TestStorage_Scan(t *testing.T) {
// ctx := common.NewContext(common.NewEmptyConfig())
// core := newStorage(ctx, ctx.Logger())
// defer core.Close()
//
// core.update(func(u *coreUpdate) {
// for i := 0; i < 1024; i++ {
// member := newMember(uuid.NewV1(), "host", "0", 1, Alive)
// u.AddMember(member)
// }
// })
//
// count := 0
// core.View(func(v *coreView) {
// v.Scan(func(s *amoeba.Scan, id uuid.UUID, attr string, val string, ver int) {
// count++
// })
// assert.Equal(t, 1024*3, count)
// })
// }
//
// func TestStorage_ListMembers(t *testing.T) {
// ctx := common.NewContext(common.NewEmptyConfig())
// core := newStorage(ctx, ctx.Logger())
// defer core.Close()
//
// members := make(map[uuid.UUID]*member)
//
// core.update(func(u *coreUpdate) {
// for i := 0; i < 1024; i++ {
// member := newMember(uuid.NewV1(), "host", "0", 1, Alive)
// members[member.Id] = member
// u.AddMember(member)
// }
// })
//
// wait := new(sync.WaitGroup)
// wait.Add(1)
// go func() {
// defer wait.Done()
// for i := 0; i < 10240; i++ {
// core.update(func(u *coreUpdate) {
// u.Put(uuid.NewV1(), "key", strconv.Itoa(i), 0)
// })
// }
// }()
//
// for i := 0; i < 64; i++ {
// assert.Equal(t, members, indexById(core.All()))
// }
//
// wait.Wait()
// }
//
// func indexById(members []*member) map[uuid.UUID]*member {
// ret := make(map[uuid.UUID]*member)
// for _, m := range members {
// ret[m.Id] = m
// }
// return ret
// }
//
// func TestStorage_ApplyDataEvent(t *testing.T) {
// ctx := common.NewContext(common.NewEmptyConfig())
// core := newStorage(ctx, ctx.Logger())
//
// type item struct {
// Id   uuid.UUID
// Attr string
// Val  string
// Ver  int
// Del  bool
// }
//
// evt := &dataEvent{uuid.NewV1(), "key", "val", 0, false}
// assert.True(t, core.Apply(evt))
// assert.False(t, core.Apply(evt))
// }
//
// func TestStorage_ApplyEvents(t *testing.T) {
// ctx := common.NewContext(common.NewEmptyConfig())
// core := newStorage(ctx, ctx.Logger())
//
// type item struct {
// Id   uuid.UUID
// Attr string
// Val  string
// Ver  int
// Del  bool
// }
//
// core.update(func(u *coreUpdate) {
// for i := 0; i < 1; i++ {
// member := newMember(uuid.NewV1(), "host", "0", 1, Alive)
// u.AddMember(member)
// }
// })
//
// copy := newStorage(ctx, ctx.Logger())
// copy.ApplyAll(core.Events())
//
// expected := make([]item, 0, 128)
// core.View(func(v *coreView) {
// v.Scan(func(s *amoeba.Scan, id uuid.UUID, attr string, val string, ver int) {
// expected = append(expected, item{id, attr, val, ver, false})
// })
// })
//
// actual := make([]item, 0, 128)
// copy.View(func(v *coreView) {
// v.Scan(func(s *amoeba.Scan, id uuid.UUID, attr string, val string, ver int) {
// actual = append(actual, item{id, attr, val, ver, false})
// })
// })
//
// assert.Equal(t, expected, actual)
// }
