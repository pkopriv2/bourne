package convoy

import (
	"testing"

	"github.com/pkopriv2/bourne/common"
	"github.com/stretchr/testify/assert"
)

func TestDirectory_Close(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	dir := newDirectory(ctx, ctx.Logger())
	assert.Nil(t, dir.Close())
}

// func TestDirectory_GetMember_NoExist(t *testing.T) {
	// ctx := common.NewContext(common.NewEmptyConfig())
	// dir := newDirectory(ctx, ctx.Logger())
	// defer dir.Close()
//
	// var id uuid.UUID
//
	// dir.View(func(v *dirView) {
		// assert.Nil(t, v.GetMember(id))
	// })
// }
//
// func TestDirectory_GetMember_Exist(t *testing.T) {
	// ctx := common.NewContext(common.NewEmptyConfig())
	// dir := newDirectory(ctx, ctx.Logger())
	// defer dir.Close()
//
	// member := newMember(uuid.NewV1(), "host", "0", 1, Alive)
	// dir.update(func(u *dirUpdate) {
		// u.AddMember(member)
	// })
//
	// dir.View(func(v *dirView) {
		// assert.Equal(t, member, v.GetMember(member.Id))
	// })
// }
//
// func TestDirectory_UpdateMember(t *testing.T) {
	// ctx := common.NewContext(common.NewEmptyConfig())
	// dir := newDirectory(ctx, ctx.Logger())
	// defer dir.Close()
//
	// member := newMember(uuid.NewV1(), "host", "0", 1, Alive)
	// dir.update(func(u *dirUpdate) {
		// u.AddMember(member)
		// u.UpdateMember(member.Id, Failed, 2)
	// })
//
	// val := *member
	// val.Status = Failed
	// val.Version = 2
	// dir.View(func(v *dirView) {
		// assert.Equal(t, &val, v.GetMember(member.Id))
	// })
// }
//
// func TestDirectory_GetAttr_NoExist(t *testing.T) {
	// ctx := common.NewContext(common.NewEmptyConfig())
	// dir := newDirectory(ctx, ctx.Logger())
	// defer dir.Close()
//
	// member := newMember(uuid.NewV1(), "host", "0", 1, Alive)
	// dir.update(func(u *dirUpdate) {
		// u.AddMember(member)
	// })
//
	// dir.View(func(v *dirView) {
		// _, _, found := v.Get(member.Id, "attr")
		// assert.False(t, found)
	// })
// }
//
// func TestDirectory_DelAttr_NoExist(t *testing.T) {
	// ctx := common.NewContext(common.NewEmptyConfig())
	// dir := newDirectory(ctx, ctx.Logger())
	// defer dir.Close()
//
	// member := newMember(uuid.NewV1(), "host", "0", 1, Alive)
	// dir.update(func(u *dirUpdate) {
		// u.AddMember(member)
		// u.Del(member.Id, "attr", 1)
	// })
//
	// dir.View(func(v *dirView) {
		// _, _, found := v.Get(member.Id, "attr")
		// assert.False(t, found)
	// })
// }
//
// func TestDirectory_Scan(t *testing.T) {
	// ctx := common.NewContext(common.NewEmptyConfig())
	// dir := newDirectory(ctx, ctx.Logger())
	// defer dir.Close()
//
	// dir.update(func(u *dirUpdate) {
		// for i := 0; i < 1024; i++ {
			// member := newMember(uuid.NewV1(), "host", "0", 1, Alive)
			// u.AddMember(member)
		// }
	// })
//
	// count := 0
	// dir.View(func(v *dirView) {
		// v.Scan(func(s *amoeba.Scan, id uuid.UUID, attr string, val string, ver int) {
			// count++
		// })
		// assert.Equal(t, 1024*3, count)
	// })
// }
//
// func TestDirectory_ListMembers(t *testing.T) {
	// ctx := common.NewContext(common.NewEmptyConfig())
	// dir := newDirectory(ctx, ctx.Logger())
	// defer dir.Close()
//
	// members := make(map[uuid.UUID]*member)
//
	// dir.update(func(u *dirUpdate) {
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
			// dir.update(func(u *dirUpdate) {
				// u.Put(uuid.NewV1(), "key", strconv.Itoa(i), 0)
			// })
		// }
	// }()
//
	// for i := 0; i < 64; i++ {
		// assert.Equal(t, members, indexById(dir.All()))
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
// func TestDirectory_ApplyDataEvent(t *testing.T) {
	// ctx := common.NewContext(common.NewEmptyConfig())
	// dir := newDirectory(ctx, ctx.Logger())
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
	// assert.True(t, dir.Apply(evt))
	// assert.False(t, dir.Apply(evt))
// }
//
// func TestDirectory_ApplyEvents(t *testing.T) {
	// ctx := common.NewContext(common.NewEmptyConfig())
	// dir := newDirectory(ctx, ctx.Logger())
//
	// type item struct {
		// Id   uuid.UUID
		// Attr string
		// Val  string
		// Ver  int
		// Del  bool
	// }
//
	// dir.update(func(u *dirUpdate) {
		// for i := 0; i < 1; i++ {
			// member := newMember(uuid.NewV1(), "host", "0", 1, Alive)
			// u.AddMember(member)
		// }
	// })
//
	// copy := newDirectory(ctx, ctx.Logger())
	// copy.ApplyAll(dir.Events())
//
	// expected := make([]item, 0, 128)
	// dir.View(func(v *dirView) {
		// v.Scan(func(s *amoeba.Scan, id uuid.UUID, attr string, val string, ver int) {
			// expected = append(expected, item{id, attr, val, ver, false})
		// })
	// })
//
	// actual := make([]item, 0, 128)
	// copy.View(func(v *dirView) {
		// v.Scan(func(s *amoeba.Scan, id uuid.UUID, attr string, val string, ver int) {
			// actual = append(actual, item{id, attr, val, ver, false})
		// })
	// })
//
	// assert.Equal(t, expected, actual)
// }
