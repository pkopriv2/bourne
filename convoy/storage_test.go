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

	core.Update(func(u *update) {
		_, ok := u.Status(uuid.NewV1())
		assert.False(t, ok)
	})
}

func TestStorage_Status_Enabled(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	core := newStorage(ctx, ctx.Logger())
	defer core.Close()

	id := uuid.NewV1()
	core.Update(func(u *update) {
		assert.True(t, u.Enable(id, 0))
	})

	core.View(func(v *view) {
		s, found := v.Status(id)
		assert.True(t, found)
		assert.True(t, s.Enabled)
		assert.Equal(t, 0, s.Version)

		i, ok := v.GetLive(id, memberEnabledAttr)
		assert.Equal(t, true, ok)
		assert.Equal(t, "true", i.Val)
	})
}

func TestStorage_Get_NotEnabled(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	core := newStorage(ctx, ctx.Logger())
	defer core.Close()

	id := uuid.NewV1()
	core.Update(func(u *update) {
		assert.True(t, u.Put(id, 0, "key", "val", 0))
	})

	core.View(func(v *view) {
		_, ok := v.GetLive(id, "key")
		assert.False(t, ok)
	})
}

func TestStorage_Scan_NotEnabled(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	core := newStorage(ctx, ctx.Logger())
	defer core.Close()

	id := uuid.NewV1()
	core.Update(func(u *update) {
		assert.True(t, u.Put(id, 0, "key", "val", 0))
	})

	core.View(func(v *view) {
		i := 0
		v.ScanLive(func(amoeba.Scan, item) {
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
	core.Update(func(u *update) {
		assert.True(t, u.Enable(id, 0))
		assert.True(t, u.Put(id, 0, "key", "val", 0))
	})

	core.View(func(v *view) {
		i, ok := v.GetLive(id, "key")
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
	core.Update(func(u *update) {
		assert.True(t, u.Disable(id, 0))
		assert.True(t, u.Put(id, 0, "key", "val", 0))
	})

	core.View(func(v *view) {
		_, ok := v.GetLive(id, "key")
		assert.False(t, ok)
	})
}

func TestStorage_Get_Del(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	core := newStorage(ctx, ctx.Logger())
	defer core.Close()

	id := uuid.NewV1()
	core.Update(func(u *update) {
		assert.True(t, u.Enable(id, 0))
		assert.True(t, u.Put(id, 0, "key", "val", 0))
		assert.True(t, u.Del(id, 0, "key", 0))
	})

	core.View(func(v *view) {
		_, ok := v.GetLive(id, "key")
		assert.False(t, ok)
	})
}

func TestStorage_Get_OldData(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	core := newStorage(ctx, ctx.Logger())
	defer core.Close()

	id := uuid.NewV1()
	core.Update(func(u *update) {
		assert.True(t, u.Enable(id, 1))
		assert.True(t, u.Put(id, 0, "key", "val", 0))
	})

	core.View(func(v *view) {
		_, ok := v.GetLive(id, "key")
		assert.False(t, ok)
	})
}

func TestStorage_Update(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	core := newStorage(ctx, ctx.Logger())
	defer core.Close()

	id := uuid.NewV1()
	items := core.Update(func(u *update) {
		assert.True(t, u.Enable(id, 1))
		assert.True(t, u.Put(id, 0, "key", "val", 0))
		assert.False(t, u.Put(id, 0, "key", "val", 0))
	})

	assert.Equal(t, 2, len(items))
}
