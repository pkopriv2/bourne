package convoy

import (
	"testing"

	"github.com/pkopriv2/bourne/amoeba"
	"github.com/pkopriv2/bourne/common"
	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
)

func TestStorage_Close(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	core := newStorage(ctx, ctx.Logger())
	assert.Nil(t, core.Close())
}

func TestStorage_Status_NotEnabled_NotDisabled(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	core := newStorage(ctx, ctx.Logger())
	defer core.Close()

	core.Update(func(u *update) error {
		_, ok := u.Roster[uuid.NewV1()]
		assert.False(t, ok)
		return nil
	})
}

func TestStorage_Status_Enabled(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	core := newStorage(ctx, ctx.Logger())
	defer core.Close()

	id := uuid.NewV1()
	core.Update(func(u *update) error {
		assert.True(t, u.Join(id, 0))
		return nil
	})

	core.View(func(v *view) {
		s, found := v.Roster[id]
		assert.True(t, found)
		assert.True(t, s.Active)
		assert.Equal(t, 0, s.Version)

		i, ok := v.GetActive(id, memberMembershipAttr)
		assert.Equal(t, true, ok)
		assert.Equal(t, "true", i.Val)
	})
}

func TestStorage_Get_NotEnabled(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	core := newStorage(ctx, ctx.Logger())
	defer core.Close()

	id := uuid.NewV1()
	core.Update(func(u *update) error {
		assert.True(t, u.Put(id, 0, "key", "val", 0))
		return nil
	})

	core.View(func(v *view) {
		_, ok := v.GetActive(id, "key")
		assert.False(t, ok)
	})
}

func TestStorage_Scan_NotEnabled(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	core := newStorage(ctx, ctx.Logger())
	defer core.Close()

	id := uuid.NewV1()
	core.Update(func(u *update) error {
		assert.True(t, u.Put(id, 0, "key", "val", 0))
		return nil
	})

	core.View(func(v *view) {
		i := 0
		v.ScanActive(func(amoeba.Scan, item) {
			i++
		})
		assert.Equal(t, 0, i)

		v.ScanAll(func(amoeba.Scan, item) {
			i++
		})
		assert.Equal(t, 1, i)
	})
}

func TestStorage_Get_Enabled(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	core := newStorage(ctx, ctx.Logger())
	defer core.Close()

	id := uuid.NewV1()
	core.Update(func(u *update) error {
		assert.True(t, u.Join(id, 0))
		assert.True(t, u.Put(id, 0, "key", "val", 0))
		return nil
	})

	core.View(func(v *view) {
		i, ok := v.GetActive(id, "key")
		assert.True(t, ok)
		assert.Equal(t, "val", i.Val)
		assert.Equal(t, 0, i.Ver)
	})
}

func TestStorage_Get_Disabled(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	core := newStorage(ctx, ctx.Logger())
	defer core.Close()

	id := uuid.NewV1()
	core.Update(func(u *update) error {
		assert.True(t, u.Evict(id, 0))
		assert.True(t, u.Put(id, 0, "key", "val", 0))
		return nil
	})

	core.View(func(v *view) {
		_, ok := v.GetActive(id, "key")
		assert.False(t, ok)
	})
}

func TestStorage_Get_Rejoin(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	core := newStorage(ctx, ctx.Logger())
	defer core.Close()

	id := uuid.NewV1()
	core.Update(func(u *update) error {
		assert.True(t, u.Evict(id, 0))
		assert.True(t, u.Put(id, 0, "key", "val", 0))
		assert.True(t, u.Join(id, 1))
		assert.True(t, u.Put(id, 1, "key2", "val", 0))
		return nil
	})

	var ok bool
	core.View(func(v *view) {
		_, ok = v.GetActive(id, "key")
		assert.False(t, ok)
		_, ok = v.GetActive(id, "key2")
		assert.True(t, ok)
	})
}

func TestStorage_Get_Del(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	core := newStorage(ctx, ctx.Logger())
	defer core.Close()

	id := uuid.NewV1()
	core.Update(func(u *update) error {
		assert.True(t, u.Join(id, 0))
		assert.True(t, u.Put(id, 0, "key", "val", 0))
		assert.True(t, u.Del(id, 0, "key", 0))
		return nil
	})

	core.View(func(v *view) {
		_, ok := v.GetActive(id, "key")
		assert.False(t, ok)
	})
}

func TestStorage_Get_OldData(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	core := newStorage(ctx, ctx.Logger())
	defer core.Close()

	id := uuid.NewV1()
	core.Update(func(u *update) error {
		assert.True(t, u.Join(id, 1))
		assert.True(t, u.Put(id, 0, "key", "val", 0))
		return nil
	})

	core.View(func(v *view) {
		_, ok := v.GetActive(id, "key")
		assert.False(t, ok)
	})
}

func TestStorage_Update_RosterListener(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	core := newStorage(ctx, ctx.Logger())
	defer core.Close()

	id := uuid.NewV1()

	list := core.Listen()
	core.Update(func(u *update) error {
		u.Join(id, 1)
		return nil
	})

	ch := streamMemberships(list)

	m := <-ch
	assert.Equal(t, m.Id, id)

	list.Close()
	core.Update(func(u *update) error {
		u.Join(id, 2)
		return nil
	})

	m, ok := <-ch
	assert.False(t, ok)
}

func TestStorage_Update_HealthListener(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	core := newStorage(ctx, ctx.Logger())
	defer core.Close()

	id := uuid.NewV1()

	list := core.Listen()
	core.Update(func(u *update) error {
		u.Fail(id, 1)
		return nil
	})

	ch := streamHealth(list)

	h := <-ch
	assert.Equal(t, h.Id, id)

	list.Close()
	core.Update(func(u *update) error {
		u.Fail(id, 2)
		return nil
	})

	h, ok := <-ch
	assert.False(t, ok)
}
