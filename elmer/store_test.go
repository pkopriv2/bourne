package elmer

import (
	"testing"

	"github.com/pkopriv2/bourne/common"
	"github.com/stretchr/testify/assert"
)

func TestStore_ChildEnable_NoPrevious(t *testing.T) {
	root := newStore([]segment{})
	sub, ok := root.ChildEnable([]byte("store"), -1)
	assert.True(t, ok)
	assert.NotNil(t, sub)
}

func TestStore_ChildEnable_CompareFail(t *testing.T) {
	root := newStore([]segment{})
	_, ok := root.ChildEnable([]byte("store"), -1)
	assert.True(t, ok)

	_, ok = root.ChildEnable([]byte("store"), 1)
	assert.False(t, ok)
}

func TestStore_ChildEnable(t *testing.T) {
	root := newStore([]segment{})
	_, ok := root.ChildEnable([]byte("store"), -1)
	assert.True(t, ok)
}

func TestStore_ChildEnable_AlreadEnabled(t *testing.T) {
	root := newStore([]segment{})
	_, ok := root.ChildEnable([]byte("store"), -1)
	assert.True(t, ok)

	_, ok = root.ChildEnable([]byte("store"), 0)
	assert.False(t, ok)
}

func TestStore_ChildInfo_NoExist(t *testing.T) {
	root := newStore([]segment{})
	_, found := root.ChildInfo([]byte("store"))
	assert.False(t, found)
}

func TestStore_ChildInfo(t *testing.T) {
	root := newStore([]segment{})
	_, ok := root.ChildEnable([]byte("store"), -1)
	assert.True(t, ok)

	info, found := root.ChildInfo([]byte("store"))
	assert.True(t, found)
	assert.Equal(t, 0, info.Version())
}

func TestStore_ChildGet_Exist(t *testing.T) {
	root := newStore([]segment{})
	_, ok := root.ChildEnable([]byte("store"), -1)
	assert.True(t, ok)

	sub := root.ChildLoad([]byte("store"), 0)
	assert.NotNil(t, sub)
}

func TestStore_ChildDisable_NoExist(t *testing.T) {
	root := newStore([]segment{})
	_, ok := root.ChildDisable([]byte("store"), -1)
	assert.False(t, ok)
}

func TestStore_ChildDisable(t *testing.T) {
	root := newStore([]segment{})

	child, ok := root.ChildEnable([]byte("store"), -1)
	assert.True(t, ok)
	assert.Equal(t, emptyPath.Child([]byte("store"), 0), child.Path())

	info, ok := root.ChildDisable([]byte("store"), 0)
	assert.True(t, ok)
	assert.Equal(t, 1, info.Version())
}

func TestStore_ChildDisable_AlreadyDisabled(t *testing.T) {
	root := newStore([]segment{})

	_, ok := root.ChildEnable([]byte("store"), -1)
	assert.True(t, ok)

	disabled, ok := root.ChildDisable([]byte("store"), 0)
	assert.True(t, ok)

	_, ok = root.ChildDisable([]byte("store"), disabled.Version())
	assert.False(t, ok)
}

func TestStore_Load_MissingElem(t *testing.T) {
	root := newStore([]segment{})

	_, err := root.Load(emptyPath.Child([]byte{0}, 0))
	assert.Equal(t, PathError, common.Extract(err, PathError))
}

func TestStore_Load_Child(t *testing.T) {
	root := newStore([]segment{})

	child, ok := root.ChildEnable([]byte("child"), -1)
	assert.True(t, ok)

	loaded, err := root.Load(child.Path())
	assert.Nil(t, err)
	assert.Equal(t, child, loaded)
}

func TestStore_Load_GrandChild(t *testing.T) {
	root := newStore([]segment{})

	child, ok := root.ChildEnable([]byte("child"), -1)
	assert.True(t, ok)

	grandchild, ok := child.ChildEnable([]byte("grandchild"), -1)
	assert.True(t, ok)

	loaded, err := root.Load(grandchild.Path())
	assert.Nil(t, err)
	assert.Equal(t, grandchild, loaded)
}

func TestStore_RecurseEnable_Child(t *testing.T) {
	root := newStore([]segment{})

	child, ok, err := root.RecurseEnable(emptyPath.Child([]byte("child"), -1))
	assert.Nil(t, err)
	assert.True(t, ok)

	loaded, err := root.Load(child.Path())
	assert.Nil(t, err)
	assert.Equal(t, child, loaded)
}

func TestStore_RecurseEnable_GrandChild(t *testing.T) {
	root := newStore([]segment{})

	child, ok := root.ChildEnable([]byte("child"), -1)
	assert.True(t, ok)

	grandchild, ok, err := root.RecurseEnable(child.Path().Child([]byte("grandchild"), -1))
	assert.True(t, ok)

	loaded, err := root.Load(grandchild.Path())
	assert.Nil(t, err)
	assert.Equal(t, grandchild, loaded)
}

func TestStore_RecurseInfo_GrandChild(t *testing.T) {
	root := newStore([]segment{})

	child, ok := root.ChildEnable([]byte("child"), -1)
	assert.True(t, ok)

	grandchild, ok, err := root.RecurseEnable(child.Path().Child([]byte("grandchild"), -1))
	assert.True(t, ok)
	assert.Nil(t, err)

	info, ok, err := root.RecurseInfo(child.Path(), grandchild.Path().Last().Elem)
	assert.True(t, ok)
	assert.Nil(t, err)

	assert.Equal(t, grandchild.Info(), info)
}

func TestStore_Get_NoExist(t *testing.T) {
	root := newStore([]segment{})

	child, ok := root.ChildEnable([]byte("child"), -1)
	assert.True(t, ok)

	_, ok = child.Read([]byte("key"))
	assert.False(t, ok)
}

func TestStore_Swap_First_BadVersion(t *testing.T) {
	root := newStore([]segment{})

	_, ok := root.Swap([]byte("key"), []byte("val"), false, 0, 1)
	assert.False(t, ok)
}

func TestStore_Swap_First(t *testing.T) {
	root := newStore([]segment{})

	item, ok := root.Swap([]byte("key"), []byte("val"), false, 0, -1)
	assert.True(t, ok)
	assert.Equal(t, 0, item.Ver)
}

func TestStore_Swap_ExistMatch(t *testing.T) {
	root := newStore([]segment{})

	item1, ok := root.Swap([]byte("key"), []byte("val1"), false, 0, -1)
	assert.True(t, ok)
	assert.Equal(t, 0, item1.Ver)

	item2, ok := root.Swap([]byte("key"), []byte("val2"), false, 0, item1.Ver)
	assert.True(t, ok)
	assert.Equal(t, 1, item2.Ver)
}

func TestStore_Swap_ExistNoMatch(t *testing.T) {
	root := newStore([]segment{})

	_, ok := root.Swap([]byte("key"), []byte("val1"), false, 0, -1)
	assert.True(t, ok)

	_, ok = root.Swap([]byte("key"), []byte("val2"), false, 0, 1)
	assert.False(t, ok)
}

func TestStore_Get_Exist(t *testing.T) {
	root := newStore([]segment{})

	swapped, ok := root.Swap([]byte("key"), []byte("val1"), false, 0, -1)
	assert.True(t, ok)

	loaded, ok := root.Read([]byte("key"))
	assert.True(t, ok)
	assert.Equal(t, swapped, loaded)
}
