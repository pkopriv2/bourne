package convoy

import (
	"testing"

	"github.com/pkopriv2/bourne/amoeba"
	"github.com/pkopriv2/bourne/common"
	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
)

func TestDirectory_Close(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	dir := newDirectory(ctx)
	assert.Nil(t, dir.Close())
}

func TestDirectory_GetMember_NoExist(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	dir := newDirectory(ctx)

	var id uuid.UUID

	dir.View(func(v *view) {
		assert.Nil(t, v.GetMember(id))
	})
}

func TestDirectory_GetMember_Exist(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	dir := newDirectory(ctx)

	member := newMember(uuid.NewV4(), "host", "0", 1)
	dir.Update(func(u *update) {
		u.AddMember(member)
	})

	dir.View(func(v *view) {
		assert.Equal(t, member, v.GetMember(member.Id))
	})
}

func TestDirectory_GetAttr_NoExist(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	dir := newDirectory(ctx)

	member := newMember(uuid.NewV4(), "host", "0", 1)
	dir.Update(func(u *update) {
		u.AddMember(member)
	})

	dir.View(func(v *view) {
		_, _, found := v.GetMemberAttr(member.Id, "attr")
		assert.False(t, found)
	})
}

func TestDirectory_GetAttr_Exist(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	dir := newDirectory(ctx)

	member := newMember(uuid.NewV4(), "host", "0", 1)
	dir.Update(func(u *update) {
		u.AddMember(member)
	})

	dir.View(func(v *view) {
		val, ver, found := v.GetMemberAttr(member.Id, memberHostAttr)
		assert.True(t, found)
		assert.Equal(t, "host", val)
		assert.Equal(t, 1, ver)
	})
}

func TestDirectory_DelAttr_NoExist(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	dir := newDirectory(ctx)

	member := newMember(uuid.NewV4(), "host", "0", 1)
	dir.Update(func(u *update) {
		u.AddMember(member)
		u.DelMemberAttr(member.Id, "attr", 1)
	})

	dir.View(func(v *view) {
		_, _, found := v.GetMemberAttr(member.Id, "attr")
		assert.False(t, found)
	})
}

func TestDirectory_Scan(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	dir := newDirectory(ctx)

	dir.Update(func(u *update) {
		for i := 0; i < 1024; i++ {
			member := newMember(uuid.NewV4(), "host", "0", 1)
			u.AddMember(member)
		}
	})

	count := 0
	dir.View(func(v *view) {
		v.Scan(func(s *amoeba.Scan, id uuid.UUID, attr string, val string, ver int) {
			count++
		})
		assert.Equal(t, 1024*2, count)
	})
}

func TestDirectory_ApplyEvents(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	dir := newDirectory(ctx)

	type item struct {
		Id   uuid.UUID
		Attr string
		Val  string
		Ver  int
		Del  bool
	}

	dir.Update(func(u *update) {
		for i := 0; i < 1; i++ {
			member := newMember(uuid.NewV4(), "host", "0", 1)
			u.AddMember(member)
		}
	})

	copy := newDirectory(ctx)
	copy.ApplyAll(dir.Events())

	expected := make([]item, 0, 128)
	dir.View(func(v *view) {
		v.Scan(func(s *amoeba.Scan, id uuid.UUID, attr string, val string, ver int) {
			expected = append(expected, item{id, attr, val, ver, false})
		})
	})

	actual := make([]item, 0, 128)
	copy.View(func(v *view) {
		v.Scan(func(s *amoeba.Scan, id uuid.UUID, attr string, val string, ver int) {
			actual = append(actual, item{id, attr, val, ver, false})
		})
	})

	assert.Equal(t, expected, actual)
}
