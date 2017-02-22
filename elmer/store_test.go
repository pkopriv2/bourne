package elmer

import (
	"testing"

	"github.com/pkopriv2/bourne/common"
	"github.com/stretchr/testify/assert"
)

func TestPath_Empty_String(t *testing.T) {
	assert.Equal(t, "", emptyPath.String())
}

// func TestPath_Single_String(t *testing.T) {
// path := emptyPath.Sub([]byte{0}, 0)
// assert.Equal(t, "", path.String())
// }
//
// func TestPath_Multi_String(t *testing.T) {
// path := emptyPath.Sub([]byte{0}, 0).Sub([]byte{1}, 1)
// assert.Equal(t, "", path.String())
// }

func TestStore_ChildEnableOrCreate_NoPrevious(t *testing.T) {
	store := newStore([]segment{})
	sub, ok := store.ChildEnable([]byte("store"), -1)
	assert.True(t, ok)
	assert.NotNil(t, sub)
}

func TestStore_ChildEnableOrCreate_CompareFail(t *testing.T) {
	root := newStore([]segment{})
	_, ok := root.ChildEnable([]byte("store"), -1)
	assert.True(t, ok)

	_, ok = root.ChildEnable([]byte("store"), 1)
	assert.False(t, ok)
}

func TestStore_ChildEnableOrCreate(t *testing.T) {
	root := newStore([]segment{})
	_, ok := root.ChildEnable([]byte("store"), -1)
	assert.True(t, ok)

	_, ok = root.ChildEnable([]byte("store"), 0)
	assert.True(t, ok)
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
	store := newStore([]segment{})
	_, ok := store.ChildEnable([]byte("store"), -1)
	assert.True(t, ok)

	sub := store.ChildGet([]byte("store"), 0)
	assert.NotNil(t, sub)
}

func TestStore_ChildDisable_NoExist(t *testing.T) {
	store := newStore([]segment{})
	_, ok := store.ChildDisable([]byte("store"), -1)
	assert.False(t, ok)
}

func TestStore_ChildDisable(t *testing.T) {
	store := newStore([]segment{})

	child, ok := store.ChildEnable([]byte("store"), -1)
	assert.True(t, ok)
	assert.Equal(t, emptyPath.Sub([]byte("store"), 0), child.Path())

	info, ok := store.ChildDisable([]byte("store"), 0)
	assert.True(t, ok)
	assert.Equal(t, 1, info.Version())
}

func TestStore_Load_MissingElem(t *testing.T) {
	store := newStore([]segment{})

	_, err := store.Load(emptyPath.Sub([]byte{0}, 0))
	assert.Equal(t, PathError, common.Extract(err, PathError))
}

func TestStore_Load_Child(t *testing.T) {
	store := newStore([]segment{})

	child, ok := store.ChildEnable([]byte("child"), -1)
	assert.True(t, ok)

	loaded, err := store.Load(child.Path())
	assert.Nil(t, err)
	assert.Equal(t, child, loaded)
}

func TestStore_Load_GrandChild(t *testing.T) {
	store := newStore([]segment{})

	child, ok := store.ChildEnable([]byte("child"), -1)
	assert.True(t, ok)

	grandchild, ok := child.ChildEnable([]byte("grandchild"), -1)
	assert.True(t, ok)

	loaded, err := store.Load(grandchild.Path())
	assert.Nil(t, err)
	assert.Equal(t, grandchild, loaded)
}

// func TestStore_RecurseEnable_Child(t *testing.T) {
// store := newStore([]segment{})
//
// child, ok, err := store.RecurseEnable(emptyPath.Child([]byte("child"), 0))
// assert.Nil(t, err)
// assert.True(t, ok)
//
// loaded, err := store.Load(child.Path())
// assert.Nil(t, err)
// assert.Equal(t, child, loaded)
// }
//
// func TestStore_RecurseEnable_GrandChild(t *testing.T) {
// store := newStore([]segment{})
//
// child, ok := store.ChildEnable([]byte("child"), -1)
// assert.True(t, ok)
//
// grandchild, ok, err := store.RecurseEnable(child.Path().Child([]byte("grandchild"), 0))
// assert.True(t, ok)
//
// loaded, err := store.Load(grandchild.Path())
// assert.Nil(t, err)
// assert.Equal(t, grandchild, loaded)
// }

// func TestStore_RecurseInfo_GrandChild(t *testing.T) {
// store := newStore([]segment{})
//
// child, ok := store.ChildEnable([]byte("child"), -1)
// assert.True(t, ok)
//
// grandchild, ok, err := store.RecurseEnable(child.Path().Child([]byte("grandchild"), 0))
// assert.True(t, ok)
// assert.Nil(t, err)
//
// info, ok, err := store.RecurseInfo(child.Path(), grandchild.Path().Leaf().Elem)
// assert.True(t, ok)
// assert.Nil(t, err)
//
// assert.Equal(t, grandchild.Info(), info)
// }
//
// func TestStore_Get_NoExist(t *testing.T) {
// catalog := newCatalog()
//
// store1 := catalog.Ensure([]byte("store"))
// assert.NotNil(t, store1)
//
// _, ok := store1.Get([]byte("key"))
// assert.False(t, ok)
// }
//
// func TestStore_Swap_NoExist(t *testing.T) {
// catalog := newCatalog()
//
// store1 := catalog.Ensure([]byte("store"))
// assert.NotNil(t, store1)
//
// _, ok := store1.Swap([]byte("key"), []byte("val"), 1)
// assert.False(t, ok)
// }
//
// func TestStore_Swap_First(t *testing.T) {
// catalog := newCatalog()
//
// store1 := catalog.Ensure([]byte("store"))
// assert.NotNil(t, store1)
//
// item, ok := store1.Swap([]byte("key"), []byte("val"), 0)
// assert.True(t, ok)
// assert.Equal(t, 1, item.Ver)
// }
//
// func TestStore_Swap_ExistMatch(t *testing.T) {
// catalog := newCatalog()
//
// store1 := catalog.Ensure([]byte("store"))
// assert.NotNil(t, store1)
//
// item, ok := store1.Swap([]byte("key"), []byte("val"), 0)
// assert.True(t, ok)
//
// item2, ok := store1.Swap([]byte("key"), []byte("val2"), item.Ver)
// assert.True(t, ok)
// assert.Equal(t, 2, item2.Ver)
// }
//
// func TestStore_Swap_ExistNoMatch(t *testing.T) {
// catalog := newCatalog()
//
// store1 := catalog.Ensure([]byte("store"))
// assert.NotNil(t, store1)
//
// _, ok := store1.Swap([]byte("key"), []byte("val"), 0)
// assert.True(t, ok)
//
// _, ok = store1.Swap([]byte("key"), []byte("val2"), 2)
// assert.False(t, ok)
// }
//
// func TestStore_Get_Exist(t *testing.T) {
// catalog := newCatalog()
//
// store1 := catalog.Ensure([]byte("store"))
// assert.NotNil(t, store1)
//
// _, ok := store1.Swap([]byte("key"), []byte("val"), 0)
// assert.True(t, ok)
//
// _, ok = store1.Get([]byte("key"))
// assert.True(t, ok)
// }
